from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI(
    title="PSGC API",
    description="API for Philippine Standard Geographic Code (PSGC) data with hierarchy search.",
    version="1.0.0"
)

# Load PSGC Excel data
df = pd.read_excel("PSGC-2Q-2025-Publication-Datafile.xlsx", sheet_name="PSGC")
df.columns = df.columns.str.strip()

# Lookup dictionaries
code_to_name = dict(zip(df["10-digit PSGC"].astype(str), df["Name"]))
code_to_level = dict(zip(df["10-digit PSGC"].astype(str), df["Geographic Level"]))

def build_full_path(psgc_code: str) -> str:
    psgc_code = str(psgc_code)
    parts = []
    if code_to_level.get(psgc_code) == "Reg":
        parts.append(code_to_name.get(psgc_code))
    else:
        region_name = next((name for code, name in code_to_name.items()
                            if code.startswith(psgc_code[:2]) and code_to_level[code] == "Reg"), None)
        if region_name:
            parts.append(region_name)

        province_name = next((name for code, name in code_to_name.items()
                              if code.startswith(psgc_code[:4]) and code_to_level[code] == "Prov"), None)
        if province_name:
            parts.append(province_name)

        municipality_name = next((name for code, name in code_to_name.items()
                                  if code.startswith(psgc_code[:6]) and code_to_level[code] == "City"), None)
        if municipality_name:
            parts.append(municipality_name)

        if code_to_level.get(psgc_code) == "Bgy":
            parts.append(code_to_name.get(psgc_code))

    return " > ".join(filter(None, parts))

def search_by_level(level: str, query: str):
    q_lower = query.lower()
    results = df[(df["Geographic Level"] == level) &
                 (df["Name"].str.lower().str.contains(q_lower, na=False))][
        ["10-digit PSGC", "Name"]
    ].copy()
    results["full_path"] = results["10-digit PSGC"].astype(str).apply(build_full_path)
    return results.to_dict(orient="records")

@app.get("/regions")
def get_regions():
    regions = df[df["Geographic Level"] == "Reg"][["10-digit PSGC", "Name"]].copy()
    regions["full_path"] = regions["Name"]
    return regions.to_dict(orient="records")

@app.get("/provinces/{region_code}")
def get_provinces(region_code: str):
    provinces = df[(df["Geographic Level"] == "Prov") &
                   (df["10-digit PSGC"].astype(str).str.startswith(region_code[:2]))][
        ["10-digit PSGC", "Name"]
    ].copy()
    provinces["full_path"] = provinces["10-digit PSGC"].astype(str).apply(build_full_path)
    return provinces.to_dict(orient="records")

@app.get("/municipalities/{province_code}")
def get_municipalities(province_code: str):
    municipalities = df[(df["Geographic Level"] == "City") &
                        (df["10-digit PSGC"].astype(str).str.startswith(province_code[:4]))][
        ["10-digit PSGC", "Name"]
    ].copy()
    municipalities["full_path"] = municipalities["10-digit PSGC"].astype(str).apply(build_full_path)
    return municipalities.to_dict(orient="records")

@app.get("/barangays/{municipality_code}")
def get_barangays(municipality_code: str):
    barangays = df[(df["Geographic Level"] == "Bgy") &
                   (df["10-digit PSGC"].astype(str).str.startswith(municipality_code[:6]))][
        ["10-digit PSGC", "Name"]
    ].copy()
    barangays["full_path"] = barangays["10-digit PSGC"].astype(str).apply(build_full_path)
    return barangays.to_dict(orient="records")

@app.get("/search/regions")
def search_regions(q: str = Query(..., description="Partial region name")):
    return search_by_level("Reg", q)

@app.get("/search/provinces")
def search_provinces(q: str = Query(..., description="Partial province name")):
    return search_by_level("Prov", q)

@app.get("/search/municipalities")
def search_municipalities(q: str = Query(..., description="Partial city/municipality name")):
    return search_by_level("City", q)

@app.get("/search/barangays")
def search_barangays(q: str = Query(..., description="Partial barangay name")):
    return search_by_level("Bgy", q)
