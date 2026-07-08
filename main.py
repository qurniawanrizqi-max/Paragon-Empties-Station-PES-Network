"""
main.py
========
Entrypoint FastAPI untuk "Paragon Empties Station Network & Monitoring"
Dashboard -- 2-tab architecture:
  - Tab 1 MONITORING (deskriptif/historis, dengan filter Brand/Category/Format/Channel)
  - Tab 2 SIMULATION (preskriptif/optimasi, objective function murni minimasi
    emisi dengan constraint EPR>=20%)

Jalankan:
    uvicorn main:app --reload --port 8000

Environment:
    Lihat .env.example untuk daftar lengkap environment variables
    (koneksi Snowflake/CSV & parameter bisnis). Set DATA_MODE=snowflake|csv|mock.
"""

from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from services import data_service as ds

app = FastAPI(
    title="Paragon Empties Station Network & Monitoring",
    description="Dashboard analytics & optimasi jaringan Paragon Empties Station (PES)",
    version="2.0.0",
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def _build_filters(
    brand: Optional[str], category: Optional[str], product_format: Optional[str], channel: Optional[str]
) -> Optional[dict]:
    f = {}
    if brand:
        f["brand"] = brand
    if category:
        f["category"] = category
    if product_format:
        f["product_format"] = product_format
    if channel:
        f["channel"] = channel
    return f or None


# =========================================================================
# PAGE ROUTE
# =========================================================================

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html", {})


# =========================================================================
# TAB 1: MONITORING (deskriptif/historis) — semua endpoint terima filter
# opsional: ?brand=&category=&product_format=&channel=
# =========================================================================

@app.get("/api/filters")
def api_filters():
    """Opsi dropdown filter: Product Brand, Category, Format, Customer Channel."""
    try:
        return ds.get_filter_options()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil opsi filter: {e}")


@app.get("/api/kpi")
def api_kpi(
    brand: Optional[str] = None,
    category: Optional[str] = None,
    product_format: Optional[str] = None,
    channel: Optional[str] = None,
):
    try:
        filters = _build_filters(brand, category, product_format, channel)
        return ds.get_kpi_summary(filters=filters)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil KPI: {e}")


@app.get("/api/emission-trend")
def api_emission_trend(
    brand: Optional[str] = None,
    category: Optional[str] = None,
    product_format: Optional[str] = None,
    channel: Optional[str] = None,
):
    try:
        filters = _build_filters(brand, category, product_format, channel)
        return ds.get_emission_trend(filters=filters)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil tren emisi: {e}")


@app.get("/api/emission-by-region")
def api_emission_by_region(
    brand: Optional[str] = None,
    category: Optional[str] = None,
    product_format: Optional[str] = None,
    channel: Optional[str] = None,
):
    try:
        filters = _build_filters(brand, category, product_format, channel)
        return ds.get_emission_by_region(filters=filters)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil emisi per wilayah: {e}")


@app.get("/api/emission-treemap")
def api_emission_treemap(
    brand: Optional[str] = None,
    category: Optional[str] = None,
    product_format: Optional[str] = None,
    channel: Optional[str] = None,
):
    """Hierarchy Brand -> Category -> Format untuk Treemap Chart."""
    try:
        filters = _build_filters(brand, category, product_format, channel)
        return ds.get_emission_treemap(filters=filters)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil data treemap: {e}")


@app.get("/api/epr-target")
def api_epr_target(
    brand: Optional[str] = None,
    category: Optional[str] = None,
    product_format: Optional[str] = None,
    channel: Optional[str] = None,
):
    """Target Line Indicator: emisi/berat aktual bulanan vs target EPR wajib 20%."""
    try:
        filters = _build_filters(brand, category, product_format, channel)
        return ds.get_epr_gap(filters=filters)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil target EPR: {e}")


# =========================================================================
# TAB 2: SIMULATION (preskriptif/optimasi) — objective function murni
# minimasi emisi dengan constraint EPR>=20% (lihat data_service.py §10)
# =========================================================================

@app.get("/api/simulation/scenario-comparison")
def api_scenario_comparison():
    """Side-by-side: Skenario 1 Baseline Waste Fate vs Skenario 2 Jaringan PES Optimal."""
    try:
        return ds.get_scenario_comparison()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal membandingkan skenario: {e}")


@app.get("/api/simulation/network-map")
def api_network_map():
    """Data peta Leaflet: demand nodes, titik PES terpilih, DC, rute reverse logistics."""
    try:
        return ds.get_simulation_map_data()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil data peta: {e}")


@app.get("/api/simulation/tradeoff")
def api_tradeoff():
    """Financial & Emission Trade-off Summary."""
    try:
        return ds.get_trade_off_summary()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal menghitung trade-off summary: {e}")


# =========================================================================
# LEGACY / EXPERIMENT — dual objective function (cost vs emission) dari
# eksperimen sebelumnya, tetap disediakan untuk eksplorasi lanjutan.
# =========================================================================

@app.get("/api/cost-emission-frontier")
def api_cost_emission_frontier(objective: str = "cost"):
    """
    objective="cost"     -> Z(x) = Total Cost minimum
    objective="emission" -> Z(x) = Total Net Emission minimum (tanpa constraint EPR keras)
    """
    try:
        return ds.run_location_allocation_optimization(objective_mode=objective)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal menjalankan optimasi: {e}")


@app.get("/api/pes-locations")
def api_pes_locations(objective: str = "cost"):
    try:
        return ds.get_pes_locations(objective_mode=objective)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil lokasi PES: {e}")


@app.get("/api/simulation/candidate-table")
def api_pes_candidate_table():
    """Tabel kandidat lokasi PES: kabupaten/kota, kecamatan, latlong, estimasi emisi terserap."""
    try:
        return ds.get_pes_candidate_table()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil tabel kandidat PES: {e}")


@app.get("/api/simulation/scaling-chart")
def api_pes_scaling_chart():
    """Trajectory n stasiun vs EPR compliance & penurunan emisi (dari solver Tab 2)."""
    try:
        sol = ds.solve_optimal_network_pure_emission()
        return {"trajectory": sol["trajectory"], "n_stations_selected": sol["n_stations"]}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal mengambil data scaling chart: {e}")


@app.get("/api/location-crosswalk-report")
def api_location_crosswalk_report():
    """
    Audit hasil rekonsiliasi nama wilayah (customer_city/province di sales
    vs city/province di data kepadatan penduduk). Dipakai data team untuk
    mengecek baris yang butuh review manual (needs_review=True).
    """
    try:
        return ds.get_location_crosswalk_report()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Gagal membangun location crosswalk: {e}")


@app.get("/api/health")
def health():
    return {"status": "ok", "data_mode": ds.get_settings().data_mode}
