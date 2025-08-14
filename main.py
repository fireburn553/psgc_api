from fastapi import FastAPI, Query
import pandas as pd
from functools import lru_cache

app = FastAPI(
    title="PSGC API",
    description="API for Philippine Standard Geographic Code (PSGC) data with hierarchy search.",
    version="1.0.0"
)

@lru_cache(maxsize=1)
def load_data():
    # Load once and cache in memory
    df = pd.read_excel("PSGC-2Q-2025-Publication-Datafile.xlsx", sheet_name="PSGC")
    df.columns = df.columns.str.strip()
    code_to_name = dict(zip(df["10-digit PSGC"].astype(str), df["Name"]))
    code_to_level = dict(zip(df["10-digit PSGC"].astype(str), df["Geographic Level"]))
    return df, code_to_name, code_to_level

df, code_to_name, code_to_level = load_data()

def build_full_path(psgc_code: str) -> str:
    psgc_code = str(psgc_code)
    parts = []
    region = next((name for code, name in code_to_name.items()
                   if code.startswith(psgc_code[:2]) and code_to_level[code] == "Reg"), None)
    
    if region:
        parts.append(region)
    province = next((name for code, name in code_to_name.items()
                     if code.startswith(psgc_code[:5]) and code_to_level[code] == "Prov"), None)
    
    if province:
        parts.append(province)
    city_mun = next((name for code, name in code_to_name.items()
                 if code.startswith(psgc_code[:7]) and (code_to_level[code] == "City" or code_to_level[code] == "Mun")), None)
    
    if city_mun:
        parts.append(city_mun)
    if code_to_level.get(psgc_code) == "Bgy":
        parts.append(code_to_name.get(psgc_code))
    return " > ".join(parts)

@app.get("/api/regions")
def get_regions():
    regions = df[df["Geographic Level"] == "Reg"][["10-digit PSGC", "Name"]].copy()
    regions["full_path"] = regions["Name"]
    return regions.to_dict(orient="records")

@app.get("/api/provinces")
def get_provinces(region_code: str = Query(None, description="Optional region PSGC code to filter")):
    if region_code:
        prov_df = df[(df["Geographic Level"] == "Prov") &
                     (df["10-digit PSGC"].astype(str).str.startswith(region_code[:2]))][["10-digit PSGC", "Name"]]
    else:
        prov_df = df[df["Geographic Level"] == "Prov"][["10-digit PSGC", "Name"]]
    prov_df["full_path"] = prov_df["10-digit PSGC"].astype(str).apply(build_full_path)
    return prov_df.to_dict(orient="records")

@app.get("/api/citi_muni")
def get_cities_municipalities(province_code: str = Query(None, description="Optional province PSGC code to filter")):
    if province_code:
        citi_muni_df = df[
            df["Geographic Level"].isin(["City", "Mun"]) &
            (df["10-digit PSGC"].astype(str).str.startswith(province_code[:5]))
        ][["10-digit PSGC", "Name"]]
    else:
        citi_muni_df = df[
            df["Geographic Level"].isin(["City", "Mun"])
        ][["10-digit PSGC", "Name"]]

    citi_muni_df["full_path"] = citi_muni_df["10-digit PSGC"].astype(str).apply(build_full_path)
    return citi_muni_df.to_dict(orient="records")

@app.get("/api/barangays")
def get_barangays(municipality_code: str = Query(None, description="Optional municipality PSGC code to filter")):
    if municipality_code:
        bgy_df = df[(df["Geographic Level"] == "Bgy") &
                    (df["10-digit PSGC"].astype(str).str.startswith(municipality_code[:7]))][["10-digit PSGC", "Name"]]
    else:
        bgy_df = df[df["Geographic Level"] == "Bgy"][["10-digit PSGC", "Name"]]
    bgy_df["full_path"] = bgy_df["10-digit PSGC"].astype(str).apply(build_full_path)
    return bgy_df.to_dict(orient="records")

@app.get("/api/search")
def search_locations(level: str = Query(..., description="Reg, Prov, City, or Bgy"),
                     q: str = Query(..., description="Partial name to search for")):
    q_lower = q.lower()
    results = df[(df["Geographic Level"] == level) &
                 (df["Name"].str.lower().str.contains(q_lower, na=False))][["10-digit PSGC", "Name"]]
    results["full_path"] = results["10-digit PSGC"].astype(str).apply(build_full_path)
    return results.to_dict(orient="records")
