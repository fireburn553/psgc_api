from fastapi import FastAPI, Query
import pandas as pd
from functools import lru_cache

# PSGC API Metadata for Swagger UI
app = FastAPI(
    title="Philippine Standard Geographic Code (PSGC) API",
    description=(
        "Philippine Standard Geographic Code (PSGC) API 1.0\n"
        "[ Base URL: /api ]\n"
        "This API is based on the 2025 Philippine Standard Geographic Code (PSGC), "
        "which is a systematic classification and coding of geographic areas in the Philippines. "
        "Its units of classification are based on the four well-established levels of "
        "geographical-political subdivisions of the country: region, province, municipality/city, and barangay.\n\n"
        "Data used in this API is sourced from the PSGC main page: "
        "[https://psa.gov.ph/classification/psgc](https://psa.gov.ph/classification/psgc)"
    ),
    version="1.0.0"
)

@lru_cache(maxsize=1)
def load_data():
    """Load PSGC data from Excel and cache in memory."""
    df = pd.read_excel("PSGC-2Q-2025-Publication-Datafile.xlsx", sheet_name="PSGC")
    df.columns = df.columns.str.strip()
    code_to_name = dict(zip(df["10-digit PSGC"].astype(str), df["Name"]))
    code_to_level = dict(zip(df["10-digit PSGC"].astype(str), df["Geographic Level"]))
    return df, code_to_name, code_to_level

df, code_to_name, code_to_level = load_data()

def build_full_path(psgc_code: str) -> str:
    """Build the full hierarchical path for a given PSGC code."""
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
                     if code.startswith(psgc_code[:7]) and code_to_level[code] in ["City", "Mun"]), None)
    if city_mun:
        parts.append(city_mun)

    if code_to_level.get(psgc_code) == "Bgy":
        parts.append(code_to_name.get(psgc_code))

    return " > ".join(parts)


@app.get("/api/regions", summary="List all regions")
def get_regions():
    """Retrieve all regions in the PSGC dataset."""
    regions = df[df["Geographic Level"] == "Reg"][["10-digit PSGC", "Name"]].copy()
    regions["full_path"] = regions["Name"]
    return regions.to_dict(orient="records")


@app.get("/api/provinces", summary="List provinces (optional filter by region code)")
def get_provinces(region_code: str = Query(None, description="Optional region PSGC code to filter")):
    """Retrieve provinces. Optionally filter by a region PSGC code."""
    if region_code:
        prov_df = df[
            (df["Geographic Level"] == "Prov") &
            (df["10-digit PSGC"].astype(str).str.startswith(region_code[:2]))
        ][["10-digit PSGC", "Name"]]
    else:
        prov_df = df[df["Geographic Level"] == "Prov"][["10-digit PSGC", "Name"]]

    prov_df["full_path"] = prov_df["10-digit PSGC"].astype(str).apply(build_full_path)
    return prov_df.to_dict(orient="records")


@app.get("/api/citi_muni", summary="List cities and municipalities (optional filter by province code)")
def get_cities_municipalities(province_code: str = Query(None, description="Optional province PSGC code to filter")):
    """Retrieve cities and municipalities. Optionally filter by a province PSGC code."""
    if province_code:
        citi_muni_df = df[
            df["Geographic Level"].isin(["City", "Mun"]) &
            (df["10-digit PSGC"].astype(str).str.startswith(province_code[:5]))
        ][["10-digit PSGC", "Name"]]
    else:
        citi_muni_df = df[df["Geographic Level"].isin(["City", "Mun"])][["10-digit PSGC", "Name"]]

    citi_muni_df["full_path"] = citi_muni_df["10-digit PSGC"].astype(str).apply(build_full_path)
    return citi_muni_df.to_dict(orient="records")


@app.get("/api/barangays", summary="List barangays (optional filter by municipality code)")
def get_barangays(municipality_code: str = Query(None, description="Optional municipality PSGC code to filter")):
    """Retrieve barangays. Optionally filter by a municipality PSGC code."""
    if municipality_code:
        bgy_df = df[
            (df["Geographic Level"] == "Bgy") &
            (df["10-digit PSGC"].astype(str).str.startswith(municipality_code[:7]))
        ][["10-digit PSGC", "Name"]]
    else:
        bgy_df = df[df["Geographic Level"] == "Bgy"][["10-digit PSGC", "Name"]]

    bgy_df["full_path"] = bgy_df["10-digit PSGC"].astype(str).apply(build_full_path)
    return bgy_df.to_dict(orient="records")


@app.get("/api/search", summary="Search for locations by name and level")
def search_locations(
    level: str = Query(..., description="Geographic Level: Reg, Prov, City, Mun, or Bgy"),
    q: str = Query(..., description="Partial name to search for (case-insensitive)")
):
    """Search for a location in the PSGC dataset by name and geographic level."""
    q_lower = q.lower()
    results = df[
        (df["Geographic Level"] == level) &
        (df["Name"].str.lower().str.contains(q_lower, na=False))
    ][["10-digit PSGC", "Name"]]

    results["full_path"] = results["10-digit PSGC"].astype(str).apply(build_full_path)
    return results.to_dict(orient="records")
