"""
services/data_service.py
=========================
Layer data & business-logic untuk Skincare Waste Emission Monitoring &
Paragon Empties Station (PES) Network.

Tanggung jawab modul ini:
1. Koneksi ke Snowflake (via environment variables, TIDAK ada credential hardcode).
2. Query & join 5 tabel sumber (sales, kepadatan penduduk, DC facilities,
   emission standard, masa habis pakai) — mengikuti schema riil (lihat SQL asli
   di bagian query_* di bawah).
3. Rekonsiliasi nama wilayah (customer_city/district/province) vs data
   kepadatan penduduk (CITY/DISTRICT/PROVINCE) yang formatnya BERBEDA
   (lihat bagian 4. LOCATION CROSSWALK).
4. Perhitungan formula sesuai CLAUDE.md:
   - Waste weight per kabupaten/kota per bulan (dengan time-lag shift)
   - Emisi Baseline Waste Fate (tanpa PES)
   - Emisi Skenario PES Aktif (fate shift + emisi truk reverse logistics)
   - Biaya total (Capex+Opex mesin, Denda EPR, Denda/Kredit Karbon)
   - Optimasi location-allocation (enumerasi n=1..M kandidat lokasi PES,
     mempertimbangkan kepadatan penduduk usia produktif sebagai bobot lokasi)

Mode data dikontrol oleh env var DATA_MODE:
   - "mock"      -> data sintetis, untuk development/demo tanpa akses Snowflake
   - "snowflake" -> query live ke Snowflake

Catatan asumsi terbuka (lihat CLAUDE.md §12 Data Gap):
   - Join `product_format_name` (sales) <-> `product_form` (masa habis pakai)
     TIDAK sama nama kolomnya -> di-handle eksplisit di query/merge.
   - `faktor_emisi_daur_ulang_pes` sudah tersedia langsung dari tabel
     emission_standard (tidak perlu asumsi EF_recycle lagi seperti versi awal).
   - Cap_Emisi_Perusahaan belum ditentukan bisnis -> default 0 (semua net emisi
     dihitung sebagai "shadow price" ke Denda_Karbon). Override via env var
     COMPANY_EMISSION_CAP_KG jika sudah ada keputusan bisnis.
   - Location crosswalk (customer_city/district/province -> kepadatan
     penduduk CITY/DISTRICT/PROVINCE) dibangun otomatis dengan fuzzy matching
     dan di-cache ke file CSV agar bisa diaudit/dikoreksi manual oleh data
     team sekali saja (lihat bagian 4).
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from rapidfuzz import fuzz, process

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CROSSWALK_CACHE_PATH = BASE_DIR / "cache" / "location_crosswalk.csv"
MASTER_TABLE_CACHE_PATH = BASE_DIR / "cache" / "waste_master_table.parquet"
# Cache waste_master_table.parquet dikompres per kombinasi dimensi (lihat
# _compress_master_table_for_cache) sehingga kolom month_date (tanggal sales
# asli, sebelum di-shift jadi waste_month) tidak ikut tersimpan. Nilai
# max(month_date) yang dibutuhkan get_kpi_summary() untuk menentukan
# "bulan berjalan" disimpan terpisah di sidecar kecil ini.
REFERENCE_MONTH_CACHE_PATH = BASE_DIR / "cache" / "waste_reference_month.txt"
# Volume sampah per (kota, kecamatan) -- terpisah dari cache utama karena
# granularitas kecamatan (6000+ unik) akan mengalikan cache utama jadi
# jutaan baris kalau digabung (lihat _compress_district_volume_for_cache).
DISTRICT_VOLUME_CACHE_PATH = BASE_DIR / "cache" / "district_volume_table.parquet"


# =========================================================================
# 1. KONFIGURASI (semua via environment variables)
# =========================================================================

@dataclass(frozen=True)
class Settings:
    data_mode: str = os.getenv("DATA_MODE", "mock").lower()

    # Snowflake connection
    sf_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    sf_user: str = os.getenv("SNOWFLAKE_USER", "")
    sf_password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    sf_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    sf_database: str = os.getenv("SNOWFLAKE_DATABASE", "PLAYGROUND")
    sf_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "PLAYGROUND_RIZQI")
    sf_role: str = os.getenv("SNOWFLAKE_ROLE", "")

    # Path file CSV lokal (dipakai saat DATA_MODE=csv) -- lihat §6a README
    csv_sales_path: str = os.getenv("CSV_SALES_PATH", "data/data_sales_order_pes.csv")
    csv_facilities_path: str = os.getenv("CSV_FACILITIES_PATH", "data/dno_facilties.csv")
    csv_kepadatan_path: str = os.getenv("CSV_KEPADATAN_PATH", "data/data_kepadatan_penduduk.csv")
    csv_emission_standard_path: str = os.getenv(
        "CSV_EMISSION_STANDARD_PATH", "data/emission_standard_packaging_format.csv"
    )
    csv_masa_habis_pakai_path: str = os.getenv(
        "CSV_MASA_HABIS_PAKAI_PATH", "data/masa_habis_pakai_product_format.csv"
    )

    # Parameter bisnis (§7 CLAUDE.md)
    carbon_price_per_ton: float = float(os.getenv("CARBON_PRICE_PER_TON", 30000))
    epr_fine_per_kg: float = float(os.getenv("EPR_FINE_PER_KG", 5000))
    target_epr_pct: float = float(os.getenv("TARGET_EPR_PCT", 0.20))
    pes_capex_per_month: float = float(os.getenv("PES_CAPEX_PER_MONTH", 2_000_000))
    pes_opex_per_month: float = float(os.getenv("PES_OPEX_PER_MONTH", 1_500_000))
    fuel_ef_kg_per_liter: float = float(os.getenv("FUEL_EMISSION_FACTOR_KG_PER_LITER", 2.68))
    fuel_km_per_liter: float = float(os.getenv("FUEL_CONSUMPTION_KM_PER_LITER", 8))
    truck_capacity_kg_per_trip: float = float(os.getenv("TRUCK_CAPACITY_KG_PER_TRIP", 500))
    company_emission_cap_kg: float = float(os.getenv("COMPANY_EMISSION_CAP_KG", 0))
    default_lag_bulan: float = float(os.getenv("DEFAULT_LAG_BULAN", 2))

    # Waste-fate split (§10 CLAUDE.md)
    baseline_tpa_pct: float = 0.60
    baseline_burn_pct: float = 0.30
    baseline_wild_pct: float = 0.10  # dihitung sebagai emisi TPA

    pes_tpa_pct: float = 0.50
    pes_burn_pct: float = 0.20
    pes_collected_pct: float = 0.20
    pes_wild_pct: float = 0.10  # dihitung sebagai emisi TPA

    # Bobot kepadatan penduduk usia produktif dalam skor prioritas lokasi PES
    # (0 = diabaikan, 1 = signifikan). Lihat _marginal_benefit_score().
    density_weight_factor: float = float(os.getenv("DENSITY_WEIGHT_FACTOR", 0.15))

    # Fuzzy-matching threshold (0-100) untuk location crosswalk
    location_match_threshold: float = float(os.getenv("LOCATION_MATCH_THRESHOLD", 80))


@lru_cache
def get_settings() -> Settings:
    return Settings()


# =========================================================================
# 2. KONEKSI SNOWFLAKE
# =========================================================================

def get_snowflake_connection():
    """
    Membuka koneksi ke Snowflake menggunakan kredensial dari environment
    variables. Dipanggil hanya saat DATA_MODE == "snowflake".
    """
    import snowflake.connector

    s = get_settings()
    missing = [
        name
        for name, val in [
            ("SNOWFLAKE_ACCOUNT", s.sf_account),
            ("SNOWFLAKE_USER", s.sf_user),
            ("SNOWFLAKE_PASSWORD", s.sf_password),
            ("SNOWFLAKE_WAREHOUSE", s.sf_warehouse),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(
            f"Environment variable belum diset untuk koneksi Snowflake: {missing}"
        )

    return snowflake.connector.connect(
        account=s.sf_account,
        user=s.sf_user,
        password=s.sf_password,
        warehouse=s.sf_warehouse,
        database=s.sf_database,
        schema=s.sf_schema,
        role=s.sf_role or None,
    )


def run_query(sql: str) -> pd.DataFrame:
    """
    Menjalankan query ke Snowflake dan mengembalikan pandas DataFrame.
    Nama kolom selalu di-lowercase supaya konsisten -- Snowflake bisa
    mengembalikan kolom dalam UPPERCASE (identifier tanpa quote) atau case
    aslinya (identifier di-quote), jadi kita normalisasi di satu tempat ini.
    """
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


# =========================================================================
# 3. QUERY PER TABEL (mode "snowflake") — sesuai schema riil
# =========================================================================

def query_sales_order_pes() -> pd.DataFrame:
    sql = """
        select
            month_date
            , dc_code
            , dc_name
            , dc_lat, dc_long
            , customer_id, customer_name, channel_report_name, sub_channel_name, customer_group
            , customer_lat, customer_long
            , customer_district, customer_subdistrict, customer_city, customer_province
            , postal_code, sales_area, region_sales
            , product_code, product_name
            , material_weight_name, gross_weight_value, net_weight_value
            , packing_size_value_gram
            , product_brand_name
            , product_category_name
            , product_sub_category_name
            , product_format_name
            , product_format_packaging
            , co_volume
            , do_volume
        from data_sales_order_pes
    """
    return run_query(sql)


def query_dno_facilities() -> pd.DataFrame:
    sql = """
        select dc_code, dc_name, facility_type, lat, long
        from dno_facilties
    """
    return run_query(sql)


def query_kepadatan_penduduk() -> pd.DataFrame:
    sql = """
        select "objectid" as objectid, province, city, district, sub_district,
               "jumlah_penduduk" as jumlah_penduduk, usia_produktif,
               "luas_wilayah" as luas_wilayah,
               (usia_produktif / "luas_wilayah") as tingkat_kepadatan_penduduk
        from data_kepadatan_penduduk
    """
    return run_query(sql)


def query_emission_standard() -> pd.DataFrame:
    sql = """
        select product_format_packaging, faktor_emisi_dibakar, faktor_emisi_tpa,
               faktor_emisi_daur_ulang_pes
        from emission_standard_packaging_format
    """
    return run_query(sql)


def query_masa_habis_pakai() -> pd.DataFrame:
    sql = """
        select product_form, masa_habis_pakai_median_bulan
        from masa_habis_pakai_product_format
    """
    return run_query(sql)


# =========================================================================
# 3b. QUERY PER TABEL (mode "csv") — baca file CSV lokal di drive Anda
# =========================================================================
#
# Dipakai saat DATA_MODE=csv. Path tiap file diatur lewat environment
# variable (lihat Settings di atas / .env.example): CSV_SALES_PATH,
# CSV_FACILITIES_PATH, CSV_KEPADATAN_PATH, CSV_EMISSION_STANDARD_PATH,
# CSV_MASA_HABIS_PAKAI_PATH. Bisa berupa path relatif (terhadap folder
# tempat `uvicorn` dijalankan) atau path absolut (mis. "D:/data/sales.csv").
#
# Nama kolom di file CSV WAJIB sama dengan nama kolom di query Snowflake
# (lihat bagian 3 di atas) -- kalau beda, sesuaikan nama header CSV-nya,
# atau tambahkan rename di load_csv_table() masing-masing.
# =========================================================================

def load_csv_table(path: str, required_cols: Optional[list[str]] = None) -> pd.DataFrame:
    """Membaca 1 file CSV lokal, lowercase semua nama kolom, validasi kolom wajib ada."""
    p = Path(path)
    if not p.is_absolute():
        p = BASE_DIR / path
    if not p.exists():
        raise FileNotFoundError(
            f"File CSV tidak ditemukan: {p}. Cek env var path CSV-nya, atau pastikan "
            f"file sudah diletakkan di lokasi tersebut."
        )
    df = pd.read_csv(p)
    df.columns = [c.strip().lower() for c in df.columns]
    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Kolom berikut tidak ditemukan di {p.name}: {missing}. "
                f"Kolom yang tersedia: {list(df.columns)}"
            )
    return df


def load_csv_sales() -> pd.DataFrame:
    s = get_settings()
    df = load_csv_table(
        s.csv_sales_path,
        required_cols=[
            "month_date", "dc_code", "dc_name", "dc_lat", "dc_long",
            "customer_id", "customer_lat", "customer_long",
            "customer_city", "customer_province",
            "product_code", "packing_size_value_gram",
            "product_brand_name", "product_format_name", "product_format_packaging",
            "do_volume",
        ],
    )
    # Bug data sumber: ~1.800 baris (0.02%) di provinsi yang PASTI ada di
    # belahan bumi selatan (Jawa/Bali/Banten/Jakarta) tercatat dengan
    # customer_lat POSITIF (mis. Kec. Lomanis, Kab. Cilacap = +7.69, padahal
    # seharusnya -7.69) -- salah tanda minus saat input data, menggeser
    # centroid kota/kecamatan ke belahan bumi utara di peta & jarak ke DC.
    _SOUTHERN_HEMISPHERE_PROVINCES = {
        "BANTEN", "DKI JAKARTA", "JAKARTA RAYA", "JAWA BARAT", "JAWA TENGAH",
        "JAWA TIMUR", "DI YOGYAKARTA", "YOGYAKARTA", "BALI",
        "NUSA TENGGARA BARAT", "NUSA TENGGARA TIMUR",
    }
    bad = df["customer_lat"].gt(0) & df["customer_province"].str.upper().isin(_SOUTHERN_HEMISPHERE_PROVINCES)
    df.loc[bad, "customer_lat"] = -df.loc[bad, "customer_lat"]
    return df


def load_csv_facilities() -> pd.DataFrame:
    s = get_settings()
    df = load_csv_table(s.csv_facilities_path, required_cols=["dc_code", "dc_name", "lat", "long"])
    # Banyak baris di file sumber pakai format desimal Indonesia (koma, mis.
    # "110,83" / "-3,63") bercampur dengan format titik pada kolom lat & long
    # yang sama -- normalisasi keduanya jadi float TANPA SYARAT (bukan cuma
    # kalau dtype terdeteksi object), karena parser CSV pandas bisa infer
    # dtype kolom ini berbeda antar versi/lingkungan (pernah lolos jadi
    # string di deployment meski aman di lokal, bikin _is_in_java()/
    # _haversine_km() error "'<=' not supported between float and str").
    for col in ("lat", "long"):
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", ".", regex=False), errors="coerce"
        )
    return df


def load_csv_kepadatan() -> pd.DataFrame:
    s = get_settings()
    df = load_csv_table(
        s.csv_kepadatan_path,
        required_cols=["province", "city", "usia_produktif", "luas_wilayah"],
    )
    if "tingkat_kepadatan_penduduk" not in df.columns:
        df["tingkat_kepadatan_penduduk"] = df["usia_produktif"] / df["luas_wilayah"]
    return df


def load_csv_emission_standard() -> pd.DataFrame:
    s = get_settings()
    return load_csv_table(
        s.csv_emission_standard_path,
        required_cols=[
            "product_format_packaging", "faktor_emisi_dibakar",
            "faktor_emisi_tpa", "faktor_emisi_daur_ulang_pes",
        ],
    )


def load_csv_masa_habis_pakai() -> pd.DataFrame:
    s = get_settings()
    return load_csv_table(
        s.csv_masa_habis_pakai_path,
        required_cols=["product_form", "masa_habis_pakai_median_bulan"],
    )


# =========================================================================
# 4. LOCATION CROSSWALK — rekonsiliasi nama wilayah
#    customer_district/city/province (data sales) vs
#    district/city/province (data kepadatan penduduk)
# =========================================================================
#
# Masalah: kedua sumber data punya format penulisan nama wilayah yang
# berbeda (mis. "Jakarta Selatan" vs "KOTA ADM. JAKARTA SELATAN", atau
# "Kab. Bogor" vs "BOGOR"). Exact-join by string akan banyak miss.
#
# Solusi "cara cepat" yang dipakai di sini:
#   1. Normalisasi nama (uppercase, buang prefix administratif umum
#      seperti "KAB.", "KOTA ADM.", "KABUPATEN", tanda baca, spasi ganda).
#   2. Exact-match dulu pada nama yang sudah dinormalisasi.
#   3. Sisanya di-fuzzy-match (rapidfuzz token_sort_ratio) dibatasi pada
#      kandidat dalam PROVINCE yang sama dulu (biar cepat & akurat),
#      threshold bisa diatur via env LOCATION_MATCH_THRESHOLD (default 80).
#   4. Hasil mapping di-cache ke file CSV (`cache/location_crosswalk.csv`)
#      supaya:
#        a. tidak perlu fuzzy-match ulang tiap request (cepat),
#        b. bisa DIAUDIT & DIKOREKSI MANUAL oleh data team sekali saja --
#           baris dengan match_score rendah ditandai `needs_review=True`.
#      Untuk re-generate crosswalk (misal ada wilayah baru), hapus file
#      cache-nya atau panggil build_location_crosswalk(force_rebuild=True).
# =========================================================================

_ADMIN_PREFIXES = [
    "KABUPATEN ADMINISTRASI ",
    "KOTA ADMINISTRASI ",
    "KOTA ADM. ",
    "KOTA ADM ",
    "KABUPATEN ",
    "KAB. ",
    "KAB ",
    "KOTA ",
]


def normalize_admin_name(name: Optional[str]) -> str:
    """Normalisasi nama wilayah administratif untuk keperluan matching."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = str(name).upper().strip()
    s = re.sub(r"[.,'\u2019]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for prefix in _ADMIN_PREFIXES:
        prefix_norm = prefix.replace(".", "").strip()
        if s.startswith(prefix_norm + " "):
            s = s[len(prefix_norm):].strip()
            break
    return s


def build_location_crosswalk(
    sales_df: pd.DataFrame, kepadatan_df: pd.DataFrame, force_rebuild: bool = False
) -> pd.DataFrame:
    """
    Menghasilkan tabel mapping:
        customer_city, customer_province -> matched_city, matched_province,
        match_score, needs_review
    di-cache ke CSV. Dipakai untuk join kepadatan penduduk (kandidat lokasi
    PES) ke data sales yang formatnya berbeda.
    """
    s = get_settings()

    if CROSSWALK_CACHE_PATH.exists() and not force_rebuild:
        return pd.read_csv(CROSSWALK_CACHE_PATH)

    sales_locs = (
        sales_df[["customer_city", "customer_province"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    kep = kepadatan_df[["city", "province"]].drop_duplicates().copy()
    kep["city_norm"] = kep["city"].apply(normalize_admin_name)
    kep["province_norm"] = kep["province"].apply(normalize_admin_name)

    rows = []
    for _, r in sales_locs.iterrows():
        city_norm = normalize_admin_name(r["customer_city"])
        prov_norm = normalize_admin_name(r["customer_province"])

        exact = kep[(kep["city_norm"] == city_norm) & (kep["province_norm"] == prov_norm)]
        if not exact.empty:
            rows.append(
                {
                    "customer_city": r["customer_city"],
                    "customer_province": r["customer_province"],
                    "matched_city": exact.iloc[0]["city"],
                    "matched_province": exact.iloc[0]["province"],
                    "match_score": 100.0,
                    "match_type": "exact",
                    "needs_review": False,
                }
            )
            continue

        candidates = kep[kep["province_norm"] == prov_norm]
        search_scope = "same_province"
        if candidates.empty:
            candidates = kep
            search_scope = "all_province"

        match = process.extractOne(
            city_norm, candidates["city_norm"].tolist(), scorer=fuzz.token_sort_ratio
        )
        if match is not None:
            matched_row = candidates.iloc[match[2]]
            score = match[1]
            rows.append(
                {
                    "customer_city": r["customer_city"],
                    "customer_province": r["customer_province"],
                    "matched_city": matched_row["city"] if score >= s.location_match_threshold else None,
                    "matched_province": matched_row["province"] if score >= s.location_match_threshold else None,
                    "match_score": score,
                    "match_type": f"fuzzy_{search_scope}",
                    "needs_review": score < 90,
                }
            )
        else:
            rows.append(
                {
                    "customer_city": r["customer_city"],
                    "customer_province": r["customer_province"],
                    "matched_city": None,
                    "matched_province": None,
                    "match_score": 0.0,
                    "match_type": "no_match",
                    "needs_review": True,
                }
            )

    crosswalk = pd.DataFrame(rows)
    try:
        CROSSWALK_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        crosswalk.to_csv(CROSSWALK_CACHE_PATH, index=False)
    except OSError:
        # Filesystem read-only (mis. Vercel serverless) -- caching cuma
        # optimisasi best-effort, app tetap jalan tanpa cache ini (cuma
        # dihitung ulang tiap proses baru alih-alih tersimpan permanen).
        pass
    return crosswalk


def get_location_crosswalk_report() -> dict:
    """Ringkasan kualitas matching -- dipakai untuk audit/QA data team."""
    tbl = _load_raw_tables()
    crosswalk = build_location_crosswalk(tbl["sales"], tbl["kepadatan"])
    n_total = len(crosswalk)
    n_matched = crosswalk["matched_city"].notna().sum()
    n_needs_review = crosswalk["needs_review"].sum()
    return {
        "total_customer_city": int(n_total),
        "matched": int(n_matched),
        "unmatched": int(n_total - n_matched),
        "needs_review": int(n_needs_review),
        "cache_path": str(CROSSWALK_CACHE_PATH),
        "rows_needing_review": crosswalk[crosswalk["needs_review"]].to_dict(orient="records"),
    }


# =========================================================================
# 5. DATA SINTETIS (mode "mock") — meniru schema riil di atas
#    Sengaja dibuat MISMATCH format nama wilayah antara sales & kepadatan
#    (mis. "Jakarta Selatan" vs "KOTA ADM. JAKARTA SELATAN") untuk
#    mendemonstrasikan location crosswalk di atas benar-benar bekerja.
# =========================================================================

_JAWA_BALI_CITIES = [
    # (customer_city di sales, customer_province, lat, long,
    #  nama city versi data kepadatan (sengaja beda format), province versi kepadatan)
    ("Jakarta Selatan", "DKI Jakarta", -6.2615, 106.8106, "KOTA ADM. JAKARTA SELATAN", "DKI JAKARTA"),
    ("Jakarta Barat", "DKI Jakarta", -6.1683, 106.7590, "KOTA ADM. JAKARTA BARAT", "DKI JAKARTA"),
    ("Bandung", "Jawa Barat", -6.9175, 107.6191, "KOTA BANDUNG", "JAWA BARAT"),
    ("Bekasi", "Jawa Barat", -6.2383, 106.9756, "KOTA BEKASI", "JAWA BARAT"),
    ("Bogor", "Jawa Barat", -6.5950, 106.8166, "KAB. BOGOR", "JAWA BARAT"),
    ("Depok", "Jawa Barat", -6.4025, 106.7942, "KOTA DEPOK", "JAWA BARAT"),
    ("Tangerang", "Banten", -6.1783, 106.6319, "KOTA TANGERANG", "BANTEN"),
    ("Semarang", "Jawa Tengah", -6.9932, 110.4203, "KOTA SEMARANG", "JAWA TENGAH"),
    ("Surakarta", "Jawa Tengah", -7.5755, 110.8243, "KOTA SURAKARTA", "JAWA TENGAH"),
    ("Yogyakarta", "DI Yogyakarta", -7.7956, 110.3695, "KOTA YOGYAKARTA", "DI YOGYAKARTA"),
    ("Surabaya", "Jawa Timur", -7.2575, 112.7521, "KOTA SURABAYA", "JAWA TIMUR"),
    ("Malang", "Jawa Timur", -7.9666, 112.6326, "KOTA MALANG", "JAWA TIMUR"),
    ("Sidoarjo", "Jawa Timur", -7.4478, 112.7183, "KAB. SIDOARJO", "JAWA TIMUR"),
    ("Denpasar", "Bali", -8.6705, 115.2126, "KOTA DENPASAR", "BALI"),
    ("Badung", "Bali", -8.5900, 115.1670, "KAB. BADUNG", "BALI"),
]

_DC_LIST = [
    ("DC001", "DC Jakarta", -6.2000, 106.8160),
    ("DC002", "DC Bandung", -6.9147, 107.6098),
    ("DC003", "DC Semarang", -6.9667, 110.4167),
    ("DC004", "DC Surabaya", -7.2758, 112.6425),
    ("DC005", "DC Denpasar", -8.6500, 115.2167),
]

_BRANDS = ["Wardah", "Emina", "Make Over", "Kahf", "Instaperfect"]
_PRODUCT_FORMATS = ["Cream Jar", "Serum Bottle", "Sachet", "Tube", "Spray Bottle"]
_PACKAGING_FORMATS = ["Rigid Plastic", "Flexible Sachet", "Glass", "PET Bottle"]

_MONTHS = pd.date_range("2025-07-01", "2026-06-01", freq="MS")


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _mock_sales_df(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for month in _MONTHS:
        for city, prov, lat, lon, _kcity, _kprov in _JAWA_BALI_CITIES:
            n_orders = rng.integers(15, 40)
            for _ in range(n_orders):
                dc_code, dc_name, dc_lat, dc_lon = _DC_LIST[rng.integers(0, len(_DC_LIST))]
                brand = _BRANDS[rng.integers(0, len(_BRANDS))]
                fmt = _PRODUCT_FORMATS[rng.integers(0, len(_PRODUCT_FORMATS))]
                pack_fmt = _PACKAGING_FORMATS[rng.integers(0, len(_PACKAGING_FORMATS))]
                packing_size_gram = float(rng.uniform(15, 220))
                do_volume = int(rng.integers(50, 500))
                co_volume = int(do_volume * rng.uniform(0.95, 1.05))
                rows.append(
                    {
                        "month_date": month,
                        "dc_code": dc_code,
                        "dc_name": dc_name,
                        "dc_lat": dc_lat,
                        "dc_long": dc_lon,
                        "customer_id": f"CUST-{city[:3].upper()}-{rng.integers(1000,9999)}",
                        "customer_name": f"Toko {city} {rng.integers(1,99)}",
                        "channel_report_name": "Modern Trade",
                        "sub_channel_name": "Minimarket",
                        "customer_group": "B2B",
                        "customer_lat": lat + rng.normal(0, 0.02),
                        "customer_long": lon + rng.normal(0, 0.02),
                        "customer_district": f"Kec. {city}",
                        "customer_subdistrict": f"Kel. {city}",
                        "customer_city": city,
                        "customer_province": prov,
                        "postal_code": "00000",
                        "sales_area": "Jawa-Bali",
                        "region_sales": prov,
                        "product_code": f"SKU-{brand[:3].upper()}-{rng.integers(100,999)}",
                        "product_name": f"{brand} {fmt}",
                        "material_weight_name": pack_fmt,
                        "gross_weight_value": packing_size_gram * 1.1,
                        "net_weight_value": packing_size_gram,
                        "packing_size_value_gram": packing_size_gram,
                        "product_brand_name": brand,
                        "product_category_name": "Skincare",
                        "product_sub_category_name": fmt.split(" ")[0],
                        "product_format_name": fmt,
                        "product_format_packaging": pack_fmt,
                        "co_volume": co_volume,
                        "do_volume": do_volume,
                    }
                )
    return pd.DataFrame(rows)


def _mock_masa_habis_pakai() -> pd.DataFrame:
    lag_map = {
        "Cream Jar": 3,
        "Serum Bottle": 2,
        "Sachet": 1,
        "Tube": 2,
        "Spray Bottle": 3,
    }
    return pd.DataFrame(
        [{"product_form": k, "masa_habis_pakai_median_bulan": v} for k, v in lag_map.items()]
    )


def _mock_emission_standard() -> pd.DataFrame:
    base_ef = {
        "Rigid Plastic": {"tpa": 1.9, "burn": 3.1, "recycle": 0.12},
        "Flexible Sachet": {"tpa": 2.3, "burn": 3.6, "recycle": 0.15},
        "Glass": {"tpa": 0.6, "burn": 0.05, "recycle": 0.03},
        "PET Bottle": {"tpa": 1.7, "burn": 2.9, "recycle": 0.10},
    }
    rows = []
    for pack_fmt, ef in base_ef.items():
        rows.append(
            {
                "product_format_packaging": pack_fmt,
                "faktor_emisi_tpa": ef["tpa"],
                "faktor_emisi_dibakar": ef["burn"],
                "faktor_emisi_daur_ulang_pes": ef["recycle"],
            }
        )
    return pd.DataFrame(rows)


def _mock_kepadatan_penduduk() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    rows = []
    for _city, _prov, _lat, _lon, kcity, kprov in _JAWA_BALI_CITIES:
        usia_produktif = int(rng.integers(150_000, 900_000))
        luas_wilayah = float(rng.uniform(30, 220))  # km2
        rows.append(
            {
                "objectid": len(rows) + 1,
                "province": kprov,
                "city": kcity,
                "district": kcity,
                "sub_district": kcity,
                "jumlah_penduduk": int(usia_produktif * 1.4),
                "usia_produktif": usia_produktif,
                "luas_wilayah": luas_wilayah,
                "tingkat_kepadatan_penduduk": usia_produktif / luas_wilayah,
            }
        )
    return pd.DataFrame(rows)


# =========================================================================
# 6. BUILD "MASTER TABLE" — waste weight per kabupaten/kota per bulan
#    (join sales + emission standard + masa habis pakai + time-lag shift
#     + density via location crosswalk)
# =========================================================================

def _load_raw_tables() -> dict[str, pd.DataFrame]:
    s = get_settings()
    if s.data_mode == "snowflake":
        return {
            "sales": query_sales_order_pes(),
            "kepadatan": query_kepadatan_penduduk(),
            "facilities": query_dno_facilities(),
            "emission_standard": query_emission_standard(),
            "masa_habis_pakai": query_masa_habis_pakai(),
        }
    if s.data_mode == "csv":
        return {
            "sales": load_csv_sales(),
            "kepadatan": load_csv_kepadatan(),
            "facilities": load_csv_facilities(),
            "emission_standard": load_csv_emission_standard(),
            "masa_habis_pakai": load_csv_masa_habis_pakai(),
        }
    # default: mock
    return {
        "sales": _mock_sales_df(),
        "kepadatan": _mock_kepadatan_penduduk(),
        "facilities": pd.DataFrame(
            [
                {"dc_code": c, "dc_name": n, "facility_type": "DC", "lat": la, "long": lo}
                for c, n, la, lo in _DC_LIST
            ]
        ),
        "emission_standard": _mock_emission_standard(),
        "masa_habis_pakai": _mock_masa_habis_pakai(),
    }


@lru_cache
def get_facilities_table() -> pd.DataFrame:
    """
    Tabel DC facilities saja, di-cache terpisah (in-memory per proses).
    JANGAN pakai _load_raw_tables()["facilities"] untuk ini -- _load_raw_tables()
    juga memuat ulang CSV sales 3.8GB tiap dipanggil (tidak di-cache), padahal
    facilities-nya sendiri kecil (~100 baris).
    """
    s = get_settings()
    if s.data_mode == "snowflake":
        return query_dno_facilities()
    if s.data_mode == "csv":
        return load_csv_facilities()
    return pd.DataFrame(
        [{"dc_code": c, "dc_name": n, "facility_type": "DC", "lat": la, "long": lo} for c, n, la, lo in _DC_LIST]
    )


def _csv_source_paths(s: Settings) -> list[Path]:
    raw = [
        s.csv_sales_path,
        s.csv_facilities_path,
        s.csv_kepadatan_path,
        s.csv_emission_standard_path,
        s.csv_masa_habis_pakai_path,
    ]
    out = []
    for p in raw:
        pp = Path(p)
        out.append(pp if pp.is_absolute() else BASE_DIR / p)
    return out


def _save_reference_month(month_date_max) -> None:
    try:
        REFERENCE_MONTH_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        REFERENCE_MONTH_CACHE_PATH.write_text(pd.Timestamp(month_date_max).isoformat())
    except OSError:
        pass  # filesystem read-only (mis. Vercel serverless) -- best-effort cache


def _load_reference_month() -> Optional[pd.Timestamp]:
    if not REFERENCE_MONTH_CACHE_PATH.exists():
        return None
    try:
        return pd.Timestamp(REFERENCE_MONTH_CACHE_PATH.read_text().strip())
    except ValueError:
        return None


def get_current_reference_month(master: pd.DataFrame) -> pd.Timestamp:
    """
    "Bulan berjalan" sebenarnya (bulan sales asli terakhir), dipakai
    get_kpi_summary() supaya tidak kejatuhan ke waste_month yang mundur
    bertahun-tahun akibat produk dengan masa_habis_pakai panjang. Kalau
    `master` masih granular (mode mock/snowflake, belum dikompres) kolom
    month_date masih ada; kalau sudah dikompres (mode csv) baca dari sidecar.
    """
    if "month_date" in master.columns:
        return master["month_date"].max()
    cached = _load_reference_month()
    if cached is not None:
        return cached
    # fallback -- seharusnya tidak pernah kejadian selama cache dibangun
    # lewat build_waste_master_table(), tapi lebih baik dari error total.
    return master["waste_month"].max()


def _master_table_cache_is_fresh(s: Settings) -> bool:
    """Cache Parquet valid selama tidak lebih tua dari file CSV sumbernya."""
    if not MASTER_TABLE_CACHE_PATH.exists():
        return False
    cache_mtime = MASTER_TABLE_CACHE_PATH.stat().st_mtime
    return all(
        (not p.exists()) or p.stat().st_mtime <= cache_mtime for p in _csv_source_paths(s)
    )


# Kombinasi dimensi ini menentukan granularitas cache -- semua kolom yang
# dipakai untuk filter/groupby di endpoint manapun (lihat _apply_filters,
# get_filter_options, get_emission_treemap, _aggregate_w_by_city_month)
# HARUS ada di sini, kalau tidak nilainya akan ikut tercampur saat agregasi.
_MASTER_CACHE_GROUP_COLS = [
    "waste_month",
    "customer_city",
    "customer_province",
    "product_brand_name",
    "product_category_name",
    "product_format_name",
    "product_format_packaging",
    "channel_report_name",
]


def _compress_master_table_for_cache(sales: pd.DataFrame) -> pd.DataFrame:
    """
    Kompres tabel granular (1 baris = 1 order) jadi 1 baris per kombinasi
    dimensi yang benar-benar dipakai downstream, sum w_kg -- dari ~10 juta
    baris order individual jadi paling banyak puluhan ribu baris. Ini yang
    disimpan ke Parquet supaya restart berikutnya tidak perlu membaca ulang
    & memproses CSV 3.8GB dari nol (~80 detik -> ~1-3 detik).

    customer_subdistrict SENGAJA TIDAK dimasukkan ke granularitas ini --
    6200+ kecamatan unik akan mengalikan jumlah baris jadi ~3 juta (nyaris
    sebesar data mentah lagi). Detail kecamatan hanya dibutuhkan tabel
    kandidat PES, jadi disimpan terpisah & jauh lebih kecil lewat
    _compress_district_volume_for_cache().
    """
    return sales.groupby(_MASTER_CACHE_GROUP_COLS, as_index=False, dropna=False).agg(
        w_kg=("w_kg", "sum"),
        faktor_emisi_tpa=("faktor_emisi_tpa", "first"),
        faktor_emisi_dibakar=("faktor_emisi_dibakar", "first"),
        faktor_emisi_daur_ulang_pes=("faktor_emisi_daur_ulang_pes", "first"),
        dist_to_nearest_dc_km=("dist_to_nearest_dc_km", "first"),
        tingkat_kepadatan_penduduk=("tingkat_kepadatan_penduduk", "first"),
        city_lat=("city_lat", "first"),
        city_lon=("city_lon", "first"),
    )


def _compress_district_volume_for_cache(sales: pd.DataFrame) -> pd.DataFrame:
    """
    Tabel kecil terpisah: volume sampah per (kota, kecamatan) -- dipakai
    HANYA oleh get_pes_candidate_table() untuk memilih kecamatan dengan
    volume tertinggi per kota sebagai representasi titik PES. Jauh lebih
    kecil dari cache utama karena tidak dipecah lagi per brand/category/
    format/channel/bulan.
    """
    return sales.groupby(["customer_city", "customer_subdistrict"], as_index=False, dropna=False).agg(
        w_kg=("w_kg", "sum"),
        district_lat=("district_lat", "first"),
        district_lon=("district_lon", "first"),
    )


@lru_cache
def build_waste_master_table() -> pd.DataFrame:
    """
    Menghasilkan tabel teragregasi: satu baris = kombinasi
    (bulan_waste, kabupaten/kota, brand, product_format, packaging_format,
    channel) dengan kolom w_kg (berat kemasan jadi sampah, sudah dijumlah
    per kombinasi tsb) sudah di-shift sesuai masa habis pakai riil, plus
    faktor emisi, jarak ke DC terdekat, dan kepadatan penduduk usia
    produktif (via location crosswalk).

    Mode "csv" di-cache ke Parquet (lihat MASTER_TABLE_CACHE_PATH) supaya
    proses berat (baca CSV 3.8GB, join, crosswalk fuzzy-match, hitung jarak)
    tidak perlu diulang tiap kali proses/server restart -- cache otomatis
    dianggap basi & dibangun ulang kalau file CSV sumber lebih baru.
    """
    s = get_settings()
    if s.data_mode == "csv" and _master_table_cache_is_fresh(s):
        return pd.read_parquet(MASTER_TABLE_CACHE_PATH)
    tbl = _load_raw_tables()
    sales = tbl["sales"].copy()

    # Buang kategori non-skincare (APPAREL, MERCHANDISE, WELLNESS & RELAXATION,
    # dll -- cuma segelintir baris dari total jutaan). Beberapa produk di
    # kategori ini (mis. celana, tas, mukena) punya masa_habis_pakai puluhan
    # bulan di master data, yang menggeser waste_month proyeksinya sampai
    # 3-4 tahun ke depan dan bikin angka "bulan berjalan" di KPI jadi 0
    # (kejatuhan di bulan yang nyaris tidak ada datanya).
    # Cuma berlaku utk data csv/snowflake asli -- mock pakai placeholder
    # "Skincare" generik (bukan taksonomi kategori asli), jadi filter ini
    # akan membuang SEMUA baris mock kalau tetap diterapkan (bug yang
    # baru ketemu: sales jadi kosong -> crosswalk kosong -> KeyError).
    SKINCARE_CATEGORIES = {"MAKE UP", "FACE CARE", "BODY CARE", "HAIR CARE"}
    if s.data_mode != "mock" and "product_category_name" in sales.columns:
        sales = sales[sales["product_category_name"].isin(SKINCARE_CATEGORIES)].copy()

    masa = tbl["masa_habis_pakai"]
    ef = tbl["emission_standard"]
    facilities = tbl["facilities"]
    kepadatan = tbl["kepadatan"]

    # --- 1. Berat kemasan (kg) per baris sales, pakai do_volume (actual demand) ---
    sales["w_kg"] = (sales["packing_size_value_gram"] * sales["do_volume"]) / 1000.0

    # --- 2. Time-lag shift: bulan sales -> bulan menjadi waste ---
    #     join key: sales.product_format_name <-> masa_habis_pakai.product_form
    sales = sales.merge(
        masa, left_on="product_format_name", right_on="product_form", how="left"
    )
    sales["masa_habis_pakai_median_bulan"] = sales["masa_habis_pakai_median_bulan"].fillna(
        s.default_lag_bulan
    )
    sales["month_date"] = pd.to_datetime(sales["month_date"])
    # Vektorisasi (bukan .apply(axis=1)) -- versi row-wise sebelumnya makan
    # waktu ~15 menit untuk 10 juta baris data riil.
    month_periods = sales["month_date"].values.astype("datetime64[M]")
    lag_months = sales["masa_habis_pakai_median_bulan"].astype(int).values.astype("timedelta64[M]")
    sales["waste_month"] = pd.to_datetime(month_periods + lag_months)

    # --- 3. Join faktor emisi per packaging format ---
    sales = sales.merge(ef, on="product_format_packaging", how="left")
    ef_cols = ["faktor_emisi_tpa", "faktor_emisi_dibakar", "faktor_emisi_daur_ulang_pes"]
    sales[ef_cols] = sales[ef_cols].fillna(sales[ef_cols].mean(numeric_only=True))

    # --- 4. Jarak PES(kandidat = centroid kabupaten customer) -> DC terdekat ---
    #     Pakai master data facilities (dno_facilties) sebagai source of truth
    #     koordinat DC, bukan dc_lat/dc_long yang didenormalisasi di sales.
    def nearest_dc_distance(lat, lon) -> float:
        dists = [
            _haversine_km(lat, lon, r["lat"], r["long"]) for _, r in facilities.iterrows()
        ]
        return min(dists) if dists else np.nan

    city_coords = sales.groupby("customer_city")[["customer_lat", "customer_long"]].mean()
    city_coords["dist_to_nearest_dc_km"] = city_coords.apply(
        lambda r: nearest_dc_distance(r["customer_lat"], r["customer_long"]), axis=1
    )
    # city_lat/city_lon = centroid koordinat customer riil per kota (dipakai
    # utk peta & tabel kandidat PES) -- BUKAN daftar koordinat mock/hardcode.
    city_coords = city_coords.rename(columns={"customer_lat": "city_lat", "customer_long": "city_lon"})
    sales = sales.merge(
        city_coords[["city_lat", "city_lon", "dist_to_nearest_dc_km"]],
        on="customer_city",
        how="left",
    )

    # Centroid per kecamatan -- dipakai tabel kandidat lokasi PES supaya
    # titiknya lebih presisi daripada cuma centroid kota (lihat
    # get_pes_candidate_table()). Pakai customer_subdistrict (bukan
    # customer_district) karena customer_district ~99.85% kosong di data
    # riil, sedangkan customer_subdistrict terisi penuh dan berisi nama
    # kecamatan asli (mis. "COBLONG", "TANAH ABANG", "RUNGKUT").
    district_coords = (
        sales.groupby(["customer_city", "customer_subdistrict"])[["customer_lat", "customer_long"]]
        .mean()
        .rename(columns={"customer_lat": "district_lat", "customer_long": "district_lon"})
        .reset_index()
    )
    sales = sales.merge(district_coords, on=["customer_city", "customer_subdistrict"], how="left")

    # --- 5. Location crosswalk -> kepadatan penduduk usia produktif per kabupaten customer ---
    crosswalk = build_location_crosswalk(sales, kepadatan)
    sales = sales.merge(
        crosswalk[["customer_city", "customer_province", "matched_city", "matched_province"]],
        on=["customer_city", "customer_province"],
        how="left",
    )
    # kepadatan.csv ada di level kecamatan/sub-district (rata2 ~160 baris per
    # kota, ada yang sampai 852) -- kalau di-merge langsung tanpa diagregasi
    # dulu ke level kota, join meledak many-to-many (pernah coba alokasi
    # 16 GB / 2+ milyar baris). Agregasi dulu: kepadatan kota = total usia
    # produktif / total luas wilayah seluruh kecamatan di kota tsb.
    kep_city = (
        kepadatan.groupby(["city", "province"])
        .apply(lambda x: x["usia_produktif"].sum() / x["luas_wilayah"].sum())
        .rename("tingkat_kepadatan_penduduk")
        .reset_index()
    )
    kep_slim = kep_city.rename(columns={"city": "matched_city", "province": "matched_province"})
    sales = sales.merge(kep_slim, on=["matched_city", "matched_province"], how="left")
    sales["tingkat_kepadatan_penduduk"] = sales["tingkat_kepadatan_penduduk"].fillna(
        kepadatan["tingkat_kepadatan_penduduk"].mean()
    )

    if s.data_mode == "csv":
        _save_reference_month(sales["month_date"].max())
        district_volume = _compress_district_volume_for_cache(sales)
        compressed = _compress_master_table_for_cache(sales)
        try:
            MASTER_TABLE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            district_volume.to_parquet(DISTRICT_VOLUME_CACHE_PATH, index=False)
            compressed.to_parquet(MASTER_TABLE_CACHE_PATH, index=False)
        except OSError:
            pass  # filesystem read-only (mis. Vercel serverless) -- best-effort cache
        return compressed

    return sales


def get_district_volume_table() -> pd.DataFrame:
    """
    Volume sampah per (kota, kecamatan) -- dipakai get_pes_candidate_table().
    Mode csv: master table sudah dikompres tanpa customer_subdistrict (lihat
    _compress_master_table_for_cache), jadi baca dari cache Parquet terpisah
    yang ditulis build_waste_master_table() sebagai side-effect (dipaksa
    rebuild kalau cache itu belum ada/basi). Mode mock/snowflake: master
    masih granular dan punya kolom ini langsung, hitung di tempat.
    """
    s = get_settings()
    master = build_waste_master_table()
    if "customer_subdistrict" in master.columns:
        return _compress_district_volume_for_cache(master)
    if not DISTRICT_VOLUME_CACHE_PATH.exists() or not _master_table_cache_is_fresh(s):
        build_waste_master_table.cache_clear()
        build_waste_master_table()
    return pd.read_parquet(DISTRICT_VOLUME_CACHE_PATH)


# =========================================================================
# 7. FORMULA EMISI & BIAYA (sesuai CLAUDE.md §10 & §11)
# =========================================================================

def _aggregate_w_by_city_month(df: pd.DataFrame) -> pd.DataFrame:
    """W_j per kabupaten/kota per bulan waste (kg), plus EF tertimbang, jarak DC & kepadatan.

    Pakai groupby().agg() vektor (bukan groupby().apply() dengan lambda Python
    per grup) -- versi apply() sebelumnya sangat lambat di dataset jutaan baris.
    Weighted average dihitung lewat trik sum(bobot x nilai) / sum(bobot).
    """
    tmp = df.assign(
        _w_ef_tpa=df["w_kg"] * df["faktor_emisi_tpa"],
        _w_ef_burn=df["w_kg"] * df["faktor_emisi_dibakar"],
        _w_ef_recycle=df["w_kg"] * df["faktor_emisi_daur_ulang_pes"],
    )
    g = tmp.groupby(["waste_month", "customer_city"]).agg(
        w_kg=("w_kg", "sum"),
        _w_ef_tpa=("_w_ef_tpa", "sum"),
        _w_ef_burn=("_w_ef_burn", "sum"),
        _w_ef_recycle=("_w_ef_recycle", "sum"),
        dist_to_dc_km=("dist_to_nearest_dc_km", "first"),
        kepadatan_usia_produktif=("tingkat_kepadatan_penduduk", "first"),
        city_lat=("city_lat", "first"),
        city_lon=("city_lon", "first"),
    ).reset_index()
    g["ef_tpa"] = g["_w_ef_tpa"] / g["w_kg"]
    g["ef_burn"] = g["_w_ef_burn"] / g["w_kg"]
    g["ef_recycle"] = g["_w_ef_recycle"] / g["w_kg"]
    return g.drop(columns=["_w_ef_tpa", "_w_ef_burn", "_w_ef_recycle"])


def compute_baseline_emission_kg(w_kg: float, ef_tpa: float, ef_burn: float) -> float:
    """E_baseline = EF_TPA x (60%+10%) x W + EF_burn x 30% x W"""
    s = get_settings()
    return (
        ef_tpa * (s.baseline_tpa_pct + s.baseline_wild_pct) * w_kg
        + ef_burn * s.baseline_burn_pct * w_kg
    )


def compute_pes_fate_emission_kg(
    w_kg: float, ef_tpa: float, ef_burn: float, ef_recycle: float
) -> float:
    """E_fate(x_j=1) = EF_TPA x (50%+10%) x W + EF_burn x 20% x W + EF_recycle x 20% x W"""
    s = get_settings()
    return (
        ef_tpa * (s.pes_tpa_pct + s.pes_wild_pct) * w_kg
        + ef_burn * s.pes_burn_pct * w_kg
        + ef_recycle * s.pes_collected_pct * w_kg
    )


def compute_truck_emission_kg(w_kg: float, dist_to_dc_km: float) -> float:
    """
    E_truck = ((2 x d_j x f_j) / 8) x 2.68
    f_j = CEIL(Volume_PES / Kapasitas_Truk_per_Trip), Volume_PES = 20% x W_j
    """
    s = get_settings()
    if pd.isna(dist_to_dc_km):
        return 0.0
    volume_pes_kg = s.pes_collected_pct * w_kg
    f_j = math.ceil(volume_pes_kg / s.truck_capacity_kg_per_trip) if volume_pes_kg > 0 else 0
    round_trip_km = 2 * dist_to_dc_km * f_j
    liters = round_trip_km / s.fuel_km_per_liter
    return liters * s.fuel_ef_kg_per_liter


def _apply_filters(
    df: pd.DataFrame,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    product_format: Optional[str] = None,
    channel: Optional[str] = None,
) -> pd.DataFrame:
    """Filter tabel granular berdasarkan Product Brand, Category, Format, Customer Channel."""
    out = df
    if brand:
        out = out[out["product_brand_name"] == brand]
    if category:
        out = out[out["product_category_name"] == category]
    if product_format:
        out = out[out["product_format_name"] == product_format]
    if channel:
        out = out[out["channel_report_name"] == channel]
    return out


def get_filter_options() -> dict:
    """Daftar opsi filter untuk Tab 1 Monitoring (dropdown Brand/Category/Format/Channel)."""
    master = build_waste_master_table()
    return {
        "brands": sorted(master["product_brand_name"].dropna().unique().tolist()),
        "categories": sorted(master["product_category_name"].dropna().unique().tolist()),
        "formats": sorted(master["product_format_name"].dropna().unique().tolist()),
        "channels": sorted(master["channel_report_name"].dropna().unique().tolist()),
    }


def compute_scenario_table(
    open_cities: Optional[set[str]] = None, filters: Optional[dict] = None
) -> pd.DataFrame:
    """
    Tabel per (bulan, kabupaten): w_kg, e_baseline_kg, e_pes_scenario_kg
    (fate+truck jika kabupaten dibuka PES), w_collected_kg.
    `open_cities=None` -> semua kabupaten dianggap x_j=1 (skenario "PES penuh").
    `filters` -> dict opsional {brand, category, product_format, channel} untuk
    Tab 1 Monitoring (lihat _apply_filters()).
    """
    master = build_waste_master_table()
    if filters:
        master = _apply_filters(master, **filters)
    agg = _aggregate_w_by_city_month(master)

    def row_calc(r):
        is_open = True if open_cities is None else (r["customer_city"] in open_cities)
        e_baseline = compute_baseline_emission_kg(r["w_kg"], r["ef_tpa"], r["ef_burn"])
        if is_open:
            e_fate = compute_pes_fate_emission_kg(r["w_kg"], r["ef_tpa"], r["ef_burn"], r["ef_recycle"])
            e_truck = compute_truck_emission_kg(r["w_kg"], r["dist_to_dc_km"])
            e_pes_scenario = e_fate + e_truck
            w_collected = get_settings().pes_collected_pct * r["w_kg"]
        else:
            e_pes_scenario = e_baseline
            w_collected = 0.0
        return pd.Series(
            {
                "e_baseline_kg": e_baseline,
                "e_pes_scenario_kg": e_pes_scenario,
                "w_collected_kg": w_collected,
                "is_pes_open": is_open,
            }
        )

    calc = agg.apply(row_calc, axis=1)
    return pd.concat([agg, calc], axis=1)


# =========================================================================
# 8. OPTIMASI LOCATION-ALLOCATION (§11 CLAUDE.md)
#    Skor prioritas kabupaten kini juga mempertimbangkan kepadatan
#    penduduk usia produktif (density_weight_factor di Settings).
# =========================================================================

def _city_level_summary() -> pd.DataFrame:
    master = build_waste_master_table()
    agg = _aggregate_w_by_city_month(master)
    city_summary = (
        agg.groupby("customer_city")
        .apply(
            lambda x: pd.Series(
                {
                    "w_total_kg": x["w_kg"].sum(),
                    "ef_tpa": np.average(x["ef_tpa"], weights=x["w_kg"]),
                    "ef_burn": np.average(x["ef_burn"], weights=x["w_kg"]),
                    "ef_recycle": np.average(x["ef_recycle"], weights=x["w_kg"]),
                    "dist_to_dc_km": x["dist_to_dc_km"].iloc[0],
                    "kepadatan_usia_produktif": x["kepadatan_usia_produktif"].iloc[0],
                    "n_months": x["waste_month"].nunique(),
                    "city_lat": x["city_lat"].iloc[0],
                    "city_lon": x["city_lon"].iloc[0],
                }
            )
        )
        .reset_index()
    )
    return city_summary


def _marginal_benefit_score(row, s: Settings, density_norm_max: float) -> float:
    """
    Skor untuk greedy ranking kandidat lokasi PES: kombinasi
    (a) nilai ekonomi penurunan emisi + penurunan denda EPR dikurangi biaya
        mesin, dan
    (b) bonus proporsional kepadatan penduduk usia produktif (semakin padat
        -> semakin diprioritaskan, sesuai brief "mempertimbangkan kepadatan
        penduduk usia produktif BPS").
    """
    w = row["w_total_kg"] / max(row["n_months"], 1)
    e_baseline = compute_baseline_emission_kg(w, row["ef_tpa"], row["ef_burn"])
    e_fate = compute_pes_fate_emission_kg(w, row["ef_tpa"], row["ef_burn"], row["ef_recycle"])
    e_truck = compute_truck_emission_kg(w, row["dist_to_dc_km"])
    delta_emission_kg = e_baseline - (e_fate + e_truck)
    delta_emission_value = (delta_emission_kg / 1000.0) * s.carbon_price_per_ton
    w_collected = s.pes_collected_pct * w
    delta_epr_value = w_collected * s.epr_fine_per_kg
    station_cost = s.pes_capex_per_month + s.pes_opex_per_month

    base_score = delta_emission_value + delta_epr_value - station_cost

    density_norm = (
        row["kepadatan_usia_produktif"] / density_norm_max if density_norm_max > 0 else 0
    )
    density_bonus = s.density_weight_factor * density_norm * station_cost

    return base_score + density_bonus


def _emission_reduction_score(row) -> float:
    """
    Skor ranking untuk objective_mode='emission': murni delta emisi (kg) yang
    terselamatkan jika PES dibuka di kabupaten ini, TANPA mempertimbangkan
    biaya mesin maupun kepadatan penduduk. Dipakai saat tujuan optimasi
    adalah minimisasi emisi total, bukan biaya.

        delta_E_j = E_j(x_j=0) - E_j(x_j=1)

    delta_E_j > 0 berarti membuka PES di kabupaten ini MENURUNKAN net emisi
    (fate shift ke daur ulang formal mengalahkan tambahan emisi truk).
    delta_E_j <= 0 (jarang, biasanya kabupaten sangat jauh dari DC dengan
    volume kecil) berarti membuka PES justru MENAIKKAN net emisi karena
    emisi truk melebihi penghematan fate.
    """
    w = row["w_total_kg"] / max(row["n_months"], 1)
    e_baseline = compute_baseline_emission_kg(w, row["ef_tpa"], row["ef_burn"])
    e_fate = compute_pes_fate_emission_kg(w, row["ef_tpa"], row["ef_burn"], row["ef_recycle"])
    e_truck = compute_truck_emission_kg(w, row["dist_to_dc_km"])
    return e_baseline - (e_fate + e_truck)


# =========================================================================
# Aturan modelling lokasi PES (feedback bisnis):
#  1. Kota dalam radius 20km dari DC Facility TIDAK perlu PES terpisah --
#     DC yang ada sudah cukup dekat untuk jadi titik reverse-logistics.
#  2. Coverage antar-PES: 20km (luar Jabodetabek) / 10km (Jabodetabek).
#     Kalau 2 kandidat PES saling tumpang tindih coverage-nya, cuma yang
#     PALING DEKAT dengan centroid demand gabungan (weighted by volume)
#     yang dipertahankan -- supaya tidak ada PES mubazir yang menutupi
#     area yang sama.
# =========================================================================

DC_EXCLUSION_RADIUS_KM = 20.0
_JABODETABEK_KEYWORDS = ("JAKARTA", "BOGOR", "DEPOK", "TANGERANG", "BEKASI")


def _is_jabodetabek(city_name: str) -> bool:
    name = (city_name or "").upper()
    return any(kw in name for kw in _JABODETABEK_KEYWORDS)


def _coverage_radius_km(city_name: str) -> float:
    return 10.0 if _is_jabodetabek(city_name) else 20.0


def _select_eligible_pes_cities(city_summary: pd.DataFrame, ranked_cities: list[str]) -> dict:
    """
    Jalankan `ranked_cities` (urutan prioritas skor, dari objective mode
    manapun) lewat 2 aturan di atas. Mengembalikan dict:
      - "eligible": set kota yang boleh dipasang PES (sudah dedup coverage)
      - "excluded_near_dc": kota yang dibuang karena aturan 1
      - "excluded_by_coverage": kota yang dibuang karena aturan 2 (kalah
        jarak ke centroid demand gabungan vs kota lain yang overlap)
    """
    by_city = city_summary.set_index("customer_city")
    accepted: list[str] = []
    excluded_near_dc: list[str] = []
    excluded_by_coverage: list[str] = []

    for city in ranked_cities:
        if city not in by_city.index:
            continue
        row = by_city.loc[city]
        dist_dc = row["dist_to_dc_km"]
        if pd.notna(dist_dc) and dist_dc <= DC_EXCLUSION_RADIUS_KM:
            excluded_near_dc.append(city)
            continue

        lat, lon = row["city_lat"], row["city_lon"]
        if pd.isna(lat) or pd.isna(lon):
            accepted.append(city)
            continue

        conflict_idx = None
        for i, other in enumerate(accepted):
            orow = by_city.loc[other]
            olat, olon = orow["city_lat"], orow["city_lon"]
            if pd.isna(olat) or pd.isna(olon):
                continue
            dist = _haversine_km(lat, lon, olat, olon)
            radius = min(_coverage_radius_km(city), _coverage_radius_km(other))
            if dist <= radius:
                conflict_idx = i
                break

        if conflict_idx is None:
            accepted.append(city)
            continue

        # Aturan 2: pertahankan yang lebih dekat ke centroid demand gabungan
        # (weighted by volume rata-rata/bulan kedua kota).
        other = accepted[conflict_idx]
        orow = by_city.loc[other]
        w_city = row["w_total_kg"] / max(row["n_months"], 1)
        w_other = orow["w_total_kg"] / max(orow["n_months"], 1)
        total_w = w_city + w_other
        if total_w <= 0:
            excluded_by_coverage.append(city)
            continue
        centroid_lat = (lat * w_city + orow["city_lat"] * w_other) / total_w
        centroid_lon = (lon * w_city + orow["city_lon"] * w_other) / total_w
        d_city = _haversine_km(lat, lon, centroid_lat, centroid_lon)
        d_other = _haversine_km(orow["city_lat"], orow["city_lon"], centroid_lat, centroid_lon)
        if d_city < d_other:
            accepted[conflict_idx] = city
            excluded_by_coverage.append(other)
        else:
            excluded_by_coverage.append(city)

    return {
        "eligible": set(accepted),
        "excluded_near_dc": excluded_near_dc,
        "excluded_by_coverage": excluded_by_coverage,
    }


def run_location_allocation_optimization(objective_mode: str = "cost") -> dict:
    """
    Enumerasi n = 0..M kandidat kabupaten, dengan 2 mode objective:

    - objective_mode="cost" (default): ranking kabupaten pakai
      _marginal_benefit_score (kombinasi nilai ekonomi emisi + EPR - biaya
      mesin + bonus kepadatan penduduk). n* dipilih dari Total Cost minimum.
      Ini merepresentasikan Z(x) = C_total(x) di CLAUDE.md §11.4.

    - objective_mode="emission": ranking kabupaten pakai
      _emission_reduction_score (murni delta emisi, TIDAK memperhitungkan
      biaya sama sekali). n* dipilih dari Total Net Emission minimum.
      Catatan: karena E_total(x) separable per kabupaten, hasil ranking ini
      pada dasarnya berlaku sebagai aturan "buka semua kabupaten dengan
      delta_E_j > 0" -- enumerasi n=0..M tetap dijalankan supaya bisa
      dibandingkan pada chart yang sama dengan mode "cost", tapi n* di sini
      TIDAK mempertimbangkan biaya mesin sama sekali (murni Z(x)=E_total(x)).

    Kedua mode tetap melaporkan cost & emisi berdampingan di setiap titik n
    pada frontier -- yang beda hanya kriteria ranking & pemilihan n* optimal.
    """
    if objective_mode not in ("cost", "emission"):
        raise ValueError("objective_mode harus 'cost' atau 'emission'")

    s = get_settings()
    city_summary = _city_level_summary()

    if objective_mode == "cost":
        density_norm_max = city_summary["kepadatan_usia_produktif"].max()
        city_summary["score"] = city_summary.apply(
            lambda r: _marginal_benefit_score(r, s, density_norm_max), axis=1
        )
    else:  # objective_mode == "emission"
        city_summary["score"] = city_summary.apply(_emission_reduction_score, axis=1)

    ranked = city_summary.sort_values("score", ascending=False).reset_index(drop=True)

    # Terapkan aturan eksklusi dekat-DC & dedup coverage antar-PES -- kota
    # yang tidak eligible tidak akan pernah dibuka di enumerasi manapun.
    elig = _select_eligible_pes_cities(city_summary, ranked["customer_city"].tolist())
    ranked = ranked[ranked["customer_city"].isin(elig["eligible"])].reset_index(drop=True)

    total_w_all = city_summary["w_total_kg"].sum() / max(city_summary["n_months"].mean(), 1)
    target_epr_kg = s.target_epr_pct * total_w_all

    frontier = []
    opened: set[str] = set()
    for n in range(0, len(ranked) + 1):
        if n > 0:
            opened.add(ranked.loc[n - 1, "customer_city"])

        e_total = 0.0
        w_collected_total = 0.0
        for _, r in city_summary.iterrows():
            w = r["w_total_kg"] / max(r["n_months"], 1)
            if r["customer_city"] in opened:
                e_fate = compute_pes_fate_emission_kg(w, r["ef_tpa"], r["ef_burn"], r["ef_recycle"])
                e_truck = compute_truck_emission_kg(w, r["dist_to_dc_km"])
                e_total += e_fate + e_truck
                w_collected_total += s.pes_collected_pct * w
            else:
                e_total += compute_baseline_emission_kg(w, r["ef_tpa"], r["ef_burn"])

        c_machine = n * (s.pes_capex_per_month + s.pes_opex_per_month)
        shortfall = max(0.0, target_epr_kg - w_collected_total)
        denda_epr = s.epr_fine_per_kg * shortfall
        excess_ton = max(0.0, (e_total - s.company_emission_cap_kg) / 1000.0)
        denda_karbon = s.carbon_price_per_ton * excess_ton
        c_total = c_machine + denda_epr + denda_karbon

        frontier.append(
            {
                "n_stations": n,
                "opened_cities": sorted(opened),
                "total_cost_monthly": round(c_total, 0),
                "c_machine": round(c_machine, 0),
                "denda_epr": round(denda_epr, 0),
                "denda_karbon": round(denda_karbon, 0),
                "total_net_emission_kg": round(e_total, 1),
                "w_collected_kg": round(w_collected_total, 1),
                "target_epr_kg": round(target_epr_kg, 1),
                "epr_compliance_pct": round(100 * w_collected_total / target_epr_kg, 1)
                if target_epr_kg > 0
                else 0.0,
            }
        )

    if objective_mode == "cost":
        optimal = min(frontier, key=lambda d: d["total_cost_monthly"])
    else:
        optimal = min(frontier, key=lambda d: d["total_net_emission_kg"])

    return {
        "objective_mode": objective_mode,
        "frontier": frontier,
        "optimal": optimal,
        "ranked_candidates": ranked["customer_city"].tolist(),
        "excluded_near_dc": elig["excluded_near_dc"],
        "excluded_by_coverage": elig["excluded_by_coverage"],
    }


# =========================================================================
# 9. AGGREGATOR UNTUK ENDPOINT DASHBOARD
# =========================================================================

def get_kpi_summary(filters: Optional[dict] = None) -> dict:
    s = get_settings()
    scen = compute_scenario_table(open_cities=None, filters=filters)

    # "Bulan berjalan" = waste_month terbaru yang sudah benar-benar "terjadi"
    # relatif ke bulan sales terakhir yang tercatat -- BUKAN waste_month
    # absolut terjauh. Produk dengan masa_habis_pakai panjang (mis. eye
    # shadow/highlighter ~13-15 bulan) mendorong proyeksi waste_month sampai
    # 1-2 tahun ke depan dari produk terakhir yang laku, dan bulan itu
    # datanya nyaris kosong (cuma sisa produk berumur panjang tsb).
    current_reference_month = get_current_reference_month(build_waste_master_table())
    eligible = scen.loc[scen["waste_month"] <= current_reference_month, "waste_month"]
    latest_month = eligible.max() if not eligible.empty else scen["waste_month"].max()
    latest = scen[scen["waste_month"] == latest_month]

    total_baseline_kg = latest["e_baseline_kg"].sum()
    total_emission_kg = latest["e_pes_scenario_kg"].sum()
    total_w_kg = latest["w_kg"].sum()
    total_collected_kg = latest["w_collected_kg"].sum()
    target_epr_kg = s.target_epr_pct * total_w_kg
    compliance_pct = (100 * total_collected_kg / target_epr_kg) if target_epr_kg > 0 else 0

    shortfall = max(0.0, target_epr_kg - total_collected_kg)
    denda_epr = s.epr_fine_per_kg * shortfall
    excess_ton = max(0.0, (total_emission_kg - s.company_emission_cap_kg) / 1000.0)
    denda_karbon = s.carbon_price_per_ton * excess_ton

    n_open_cities = scen[scen["is_pes_open"]]["customer_city"].nunique()
    biaya_operasional = n_open_cities * (s.pes_capex_per_month + s.pes_opex_per_month)

    return {
        "period": str(pd.Timestamp(latest_month).strftime("%Y-%m")),
        "total_waste_volume_kg": round(total_w_kg, 1),
        "total_baseline_emission_kg": round(total_baseline_kg, 1),
        "total_baseline_emission_ton_co2e": round(total_baseline_kg / 1000.0, 2),
        "total_emission_ton_co2e": round(total_emission_kg / 1000.0, 2),
        "epr_compliance_pct": round(compliance_pct, 1),
        "total_biaya_operasional_pes": round(biaya_operasional, 0),
        "denda_epr": round(denda_epr, 0),
        "denda_karbon": round(denda_karbon, 0),
        "total_waste_kg": round(total_w_kg, 1),
        "total_collected_kg": round(total_collected_kg, 1),
    }


def get_emission_trend(filters: Optional[dict] = None) -> list[dict]:
    scen = compute_scenario_table(open_cities=None, filters=filters)
    g = (
        scen.groupby("waste_month")[["e_baseline_kg", "e_pes_scenario_kg"]]
        .sum()
        .reset_index()
        .sort_values("waste_month")
    )
    return [
        {
            "month": pd.Timestamp(r["waste_month"]).strftime("%Y-%m"),
            "baseline_ton_co2e": round(r["e_baseline_kg"] / 1000.0, 2),
            "pes_scenario_ton_co2e": round(r["e_pes_scenario_kg"] / 1000.0, 2),
        }
        for _, r in g.iterrows()
    ]


def get_emission_by_region(filters: Optional[dict] = None) -> list[dict]:
    scen = compute_scenario_table(open_cities=None, filters=filters)
    g = (
        scen.groupby("customer_city")[["e_baseline_kg", "e_pes_scenario_kg", "w_kg"]]
        .sum()
        .reset_index()
        .sort_values("e_pes_scenario_kg", ascending=False)
    )
    return [
        {
            "city": r["customer_city"],
            "baseline_ton_co2e": round(r["e_baseline_kg"] / 1000.0, 2),
            "pes_scenario_ton_co2e": round(r["e_pes_scenario_kg"] / 1000.0, 2),
            "waste_kg": round(r["w_kg"], 1),
        }
        for _, r in g.iterrows()
    ]


def get_emission_by_product() -> list[dict]:
    master = build_waste_master_table()
    # compute_baseline_emission_kg() cuma aritmetika -- panggil langsung dengan
    # kolom Series (vektor) alih-alih .apply(axis=1) yang sangat lambat di
    # dataset jutaan baris.
    master["e_baseline_kg"] = compute_baseline_emission_kg(
        master["w_kg"], master["faktor_emisi_tpa"], master["faktor_emisi_dibakar"]
    )
    g = (
        master.groupby("product_brand_name")[["w_kg", "e_baseline_kg"]]
        .sum()
        .reset_index()
        .sort_values("e_baseline_kg", ascending=False)
    )
    return [
        {
            "brand": r["product_brand_name"],
            "waste_kg": round(r["w_kg"], 1),
            "emission_ton_co2e": round(r["e_baseline_kg"] / 1000.0, 2),
        }
        for _, r in g.iterrows()
    ]


def get_emission_treemap(filters: Optional[dict] = None) -> list[dict]:
    """
    Hierarchy Brand -> Category -> Format untuk Treemap Chart (Tab 1 Monitoring).
    Emisi dihitung berbasis baseline (kondisi riil hari ini, PES belum tentu
    menjangkau semua wilayah).
    """
    master = build_waste_master_table()
    if filters:
        master = _apply_filters(master, **filters)
    master = master.copy()
    # compute_baseline_emission_kg() cuma aritmetika -- panggil langsung dengan
    # kolom Series (vektor) alih-alih .apply(axis=1) yang sangat lambat di
    # dataset jutaan baris.
    master["e_baseline_kg"] = compute_baseline_emission_kg(
        master["w_kg"], master["faktor_emisi_tpa"], master["faktor_emisi_dibakar"]
    )
    g = (
        master.groupby(["product_brand_name", "product_category_name", "product_format_name"])[
            ["w_kg", "e_baseline_kg"]
        ]
        .sum()
        .reset_index()
    )
    return [
        {
            "brand": r["product_brand_name"],
            "category": r["product_category_name"],
            "format": r["product_format_name"],
            "waste_kg": round(r["w_kg"], 1),
            "emission_ton_co2e": round(r["e_baseline_kg"] / 1000.0, 2),
        }
        for _, r in g.iterrows()
    ]


def get_epr_gap(filters: Optional[dict] = None) -> list[dict]:
    s = get_settings()
    scen = compute_scenario_table(open_cities=None, filters=filters)
    g = (
        scen.groupby("waste_month")[["w_kg", "w_collected_kg"]]
        .sum()
        .reset_index()
        .sort_values("waste_month")
    )
    g["target_epr_kg"] = s.target_epr_pct * g["w_kg"]
    return [
        {
            "month": pd.Timestamp(r["waste_month"]).strftime("%Y-%m"),
            "actual_collected_kg": round(r["w_collected_kg"], 1),
            "target_epr_kg": round(r["target_epr_kg"], 1),
            "gap_kg": round(r["target_epr_kg"] - r["w_collected_kg"], 1),
        }
        for _, r in g.iterrows()
    ]


def get_pes_locations(objective_mode: str = "cost") -> dict:
    result = run_location_allocation_optimization(objective_mode=objective_mode)
    city_summary = _city_level_summary().set_index("customer_city")
    optimal_cities = result["optimal"]["opened_cities"]
    candidates = [
        {
            "city": c,
            "lat": round(float(city_summary.loc[c, "city_lat"]), 4) if c in city_summary.index else None,
            "lon": round(float(city_summary.loc[c, "city_lon"]), 4) if c in city_summary.index else None,
            "selected": c in optimal_cities,
        }
        for c in city_summary.index
    ]
    return {"candidates": candidates, "optimal_n": result["optimal"]["n_stations"]}


def get_pes_candidate_table() -> list[dict]:
    """
    Tabel kandidat lokasi PES untuk Tab Simulation. Pakai solver yang SAMA
    dengan peta/scenario-comparison/trade-off (solve_optimal_network_pure_emission)
    supaya status "terpilih" konsisten di semua tempat -- sebelumnya tabel ini
    pakai run_location_allocation_optimization(objective="cost") yang bisa
    menghasilkan himpunan kota terpilih BERBEDA dari solver Tab 2, bikin
    bingung (mis. peta bilang 173 terpilih, tabel cuma bilang 3).

    Kolom: kabupaten/kota, kecamatan dengan volume tertinggi di kota tsb,
    lat/long, status (selected / near_dc / covered_by_other / candidate),
    ranking prioritas (urutan delta_E_j), estimasi penyerapan emisi.
    """
    sol = solve_optimal_network_pure_emission()
    optimal_cities = set(sol["opened_cities"])
    excluded_near_dc = set(sol["excluded_near_dc"])
    excluded_by_coverage = set(sol["excluded_by_coverage"])
    rank_by_city = {c: i + 1 for i, c in enumerate(sol["ranked_candidates"])}

    # customer_subdistrict = representasi "kecamatan" (customer_district
    # ~99.85% kosong di data riil). Tabel volume per kecamatan disimpan
    # terpisah dari master table utama (lihat get_district_volume_table())
    # supaya cache utama tidak ikut membengkak jutaan baris.
    district_vol = get_district_volume_table()
    top_district = (
        district_vol.sort_values("w_kg", ascending=False)
        .drop_duplicates(subset=["customer_city"], keep="first")
        .set_index("customer_city")
    )

    city_summary = _city_level_summary()
    rows = []
    for _, r in city_summary.iterrows():
        city = r["customer_city"]
        delta_e_kg = _emission_reduction_score(r)
        # Default: fallback ke centroid kota kalau kecamatan dengan volume
        # tertinggi tidak punya nama/koordinat valid (data kecamatan kosong).
        district_name = None
        lat, lon = r["city_lat"], r["city_lon"]
        if city in top_district.index:
            drow = top_district.loc[city]
            if pd.notna(drow["customer_subdistrict"]):
                district_name = drow["customer_subdistrict"]
            if pd.notna(drow["district_lat"]) and pd.notna(drow["district_lon"]):
                lat, lon = drow["district_lat"], drow["district_lon"]

        if city in optimal_cities:
            status = "selected"
        elif city in excluded_near_dc:
            status = "near_dc"
        elif city in excluded_by_coverage:
            status = "covered_by_other"
        else:
            status = "candidate"

        rows.append(
            {
                "city": city,
                "district": district_name,
                "lat": round(float(lat), 4) if pd.notna(lat) else None,
                "lon": round(float(lon), 4) if pd.notna(lon) else None,
                "selected": city in optimal_cities,
                "status": status,
                "rank": rank_by_city.get(city),
                "waste_kg_per_month": round(r["w_total_kg"] / max(r["n_months"], 1), 1),
                "estimated_emission_absorbed_ton_co2e_per_month": round(delta_e_kg / 1000.0, 2),
            }
        )
    rows.sort(key=lambda x: x["rank"] if x["rank"] is not None else 10**9)
    return rows


# =========================================================================
# 10. TAB 2 "SIMULATION" — Objective Function Murni Minimasi Emisi
#     dengan Constraint EPR >= 20% (Hard Constraint)
# =========================================================================
#
#   Minimize   E_total(x) = E_PES_Daur_Ulang + E_Truk_Logistik + E_Baseline_Fate
#
#   Subject to:
#     W_collected(x) >= 20% x Total_Berat_Kemasan_Terjual   (HARD constraint)
#     f_j = CEIL(Volume_PES_j / Kapasitas_Truk_per_Trip)     (round-trip truck)
#
# Algoritma (constrained greedy -- exact untuk struktur separable ini):
#   1. Ranking semua kabupaten berdasarkan delta_E_j (penurunan emisi bersih)
#      dari terbesar ke terkecil.
#   2. Buka kabupaten top-ranked satu per satu SAMPAI target EPR terpenuhi
#      (memastikan constraint keras terpenuhi dengan kabupaten paling
#      menguntungkan secara emisi terlebih dahulu).
#   3. Setelah constraint terpenuhi, tetap buka kabupaten LAIN yang delta_E_j
#      positif (karena itu tetap menurunkan E_total lebih lanjut, objective
#      belum tentu berhenti di titik constraint minimum).
#   4. Kabupaten dengan delta_E_j <= 0 HANYA dibuka jika masih diperlukan
#      untuk memenuhi constraint (jarang terjadi kalau EF & volume wajar).
# =========================================================================

def _network_metrics_for_opened(city_summary: pd.DataFrame, opened: set[str], s: Settings) -> dict:
    """Hitung e_total, e_baseline_total, w_collected_total untuk satu set `opened`."""
    e_total = 0.0
    e_baseline_total = 0.0
    w_collected_total = 0.0
    for _, r in city_summary.iterrows():
        w = r["w_total_kg"] / max(r["n_months"], 1)
        e_base = compute_baseline_emission_kg(w, r["ef_tpa"], r["ef_burn"])
        e_baseline_total += e_base
        if r["customer_city"] in opened:
            e_fate = compute_pes_fate_emission_kg(w, r["ef_tpa"], r["ef_burn"], r["ef_recycle"])
            e_truck = compute_truck_emission_kg(w, r["dist_to_dc_km"])
            e_total += e_fate + e_truck
            w_collected_total += s.pes_collected_pct * w
        else:
            e_total += e_base
    return {
        "e_total": e_total,
        "e_baseline_total": e_baseline_total,
        "w_collected_total": w_collected_total,
    }


@lru_cache
def solve_optimal_network_pure_emission() -> dict:
    """
    Solver untuk Tab 2 Simulation: Objective Function murni minimasi emisi
    total, dengan constraint keras EPR >= 20%. Lihat penjelasan algoritma di
    komentar bagian 10 di atas. Sekarang juga menerapkan 2 aturan modelling:
    exclude kota dekat DC (<=20km) & dedup coverage antar-PES (lihat
    _select_eligible_pes_cities()). Hasil juga menyertakan `trajectory`:
    metrik (n_stations, EPR compliance, emisi) di setiap titik saat stasiun
    dibuka satu-per-satu -- dipakai chart "n stasiun vs compliance & emisi".
    """
    s = get_settings()
    city_summary = _city_level_summary()
    city_summary["delta_e_kg"] = city_summary.apply(_emission_reduction_score, axis=1)
    ranked_all = city_summary.sort_values("delta_e_kg", ascending=False).reset_index(drop=True)

    elig = _select_eligible_pes_cities(city_summary, ranked_all["customer_city"].tolist())
    ranked = ranked_all[ranked_all["customer_city"].isin(elig["eligible"])].reset_index(drop=True)

    total_w_all = city_summary["w_total_kg"].sum() / max(city_summary["n_months"].mean(), 1)
    target_epr_kg = s.target_epr_pct * total_w_all

    opened: set[str] = set()
    cum_collected = 0.0
    trajectory = []

    def _record_step():
        m = _network_metrics_for_opened(city_summary, opened, s)
        compliance = 100 * cum_collected / target_epr_kg if target_epr_kg > 0 else 0.0
        reduction_pct = (
            100 * (m["e_baseline_total"] - m["e_total"]) / m["e_baseline_total"]
            if m["e_baseline_total"] > 0
            else 0.0
        )
        trajectory.append(
            {
                "n_stations": len(opened),
                "epr_compliance_pct": round(compliance, 1),
                "total_emission_ton_co2e": round(m["e_total"] / 1000.0, 2),
                "emission_reduction_pct": round(reduction_pct, 1),
            }
        )

    _record_step()  # n=0, baseline murni

    # Step 1: penuhi constraint EPR>=20% dengan kabupaten paling menguntungkan
    # secara emisi dulu (delta_e_kg tertinggi).
    for _, r in ranked.iterrows():
        if cum_collected >= target_epr_kg:
            break
        w = r["w_total_kg"] / max(r["n_months"], 1)
        opened.add(r["customer_city"])
        cum_collected += s.pes_collected_pct * w
        _record_step()

    # Step 2: constraint sudah terpenuhi -- tetap buka kabupaten lain yang
    # masih net-positive terhadap emisi (delta_e_kg > 0), karena objective
    # murni E_total belum tentu berhenti begitu constraint minimum tercapai.
    for _, r in ranked.iterrows():
        city = r["customer_city"]
        if city in opened:
            continue
        if r["delta_e_kg"] > 0:
            opened.add(city)
            w = r["w_total_kg"] / max(r["n_months"], 1)
            cum_collected += s.pes_collected_pct * w
            _record_step()

    m = _network_metrics_for_opened(city_summary, opened, s)
    e_total, e_baseline_total, w_collected_total = m["e_total"], m["e_baseline_total"], m["w_collected_total"]

    shortfall = max(0.0, target_epr_kg - w_collected_total)
    c_machine = len(opened) * (s.pes_capex_per_month + s.pes_opex_per_month)
    denda_epr = s.epr_fine_per_kg * shortfall

    return {
        "opened_cities": sorted(opened),
        "n_stations": len(opened),
        "target_epr_kg": round(target_epr_kg, 1),
        "w_collected_kg": round(w_collected_total, 1),
        "epr_compliance_pct": round(100 * w_collected_total / target_epr_kg, 1) if target_epr_kg > 0 else 0.0,
        "epr_constraint_satisfied": bool(shortfall <= 1e-6),
        "total_baseline_emission_kg": round(e_baseline_total, 1),
        "total_optimal_emission_kg": round(e_total, 1),
        "emission_saved_kg": round(e_baseline_total - e_total, 1),
        "emission_reduction_pct": round(100 * (e_baseline_total - e_total) / e_baseline_total, 1)
        if e_baseline_total > 0
        else 0.0,
        "c_machine_monthly": round(c_machine, 0),
        "denda_epr_monthly": round(denda_epr, 0),
        "ranked_candidates": ranked["customer_city"].tolist(),
        "excluded_near_dc": elig["excluded_near_dc"],
        "excluded_by_coverage": elig["excluded_by_coverage"],
        "trajectory": trajectory,
    }


def get_scenario_comparison() -> dict:
    """Side-by-side: Skenario 1 (Baseline Waste Fate) vs Skenario 2 (Jaringan PES Optimal)."""
    sol = solve_optimal_network_pure_emission()
    s = get_settings()
    baseline_denda_epr = s.epr_fine_per_kg * sol["target_epr_kg"]  # baseline collect = 0
    return {
        "baseline": {
            "label": "Baseline Waste Fate",
            "waste_fate_pct": {"tpa": 60, "burn": 30, "wild_as_tpa": 10, "pes": 0},
            "total_emission_kg": sol["total_baseline_emission_kg"],
            "total_emission_ton_co2e": round(sol["total_baseline_emission_kg"] / 1000.0, 2),
            "w_collected_kg": 0.0,
            "epr_compliance_pct": 0.0,
            "denda_epr_monthly": round(baseline_denda_epr, 0),
            "n_stations": 0,
        },
        "optimal": {
            "label": "Jaringan PES Optimal",
            "waste_fate_pct": {"tpa": 50, "burn": 20, "wild_as_tpa": 10, "pes": 20},
            "total_emission_kg": sol["total_optimal_emission_kg"],
            "total_emission_ton_co2e": round(sol["total_optimal_emission_kg"] / 1000.0, 2),
            "w_collected_kg": sol["w_collected_kg"],
            "epr_compliance_pct": sol["epr_compliance_pct"],
            "denda_epr_monthly": sol["denda_epr_monthly"],
            "n_stations": sol["n_stations"],
        },
        "emission_saved_kg": sol["emission_saved_kg"],
        "emission_saved_ton_co2e": round(sol["emission_saved_kg"] / 1000.0, 2),
        "emission_reduction_pct": sol["emission_reduction_pct"],
        "target_epr_kg": sol["target_epr_kg"],
        "epr_constraint_satisfied": sol["epr_constraint_satisfied"],
    }


# Bounding box geografis Pulau Jawa (di luar Bali & pulau lain) -- dipakai
# untuk menyaring tampilan peta Simulation supaya lebih cepat/fokus. Tidak
# dipakai untuk menyaring perhitungan optimasi/emisi (itu tetap nasional).
_JAVA_LAT_RANGE = (-8.8, -5.8)
_JAVA_LON_RANGE = (105.0, 114.6)


def _is_in_java(lat, lon) -> bool:
    if lat is None or lon is None:
        return False
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return False
    if pd.isna(lat) or pd.isna(lon):
        return False
    return _JAVA_LAT_RANGE[0] <= lat <= _JAVA_LAT_RANGE[1] and _JAVA_LON_RANGE[0] <= lon <= _JAVA_LON_RANGE[1]


def get_simulation_map_data() -> dict:
    """
    Data untuk peta Leaflet.js Tab 2: HANYA titik PES terpilih (bukan semua
    ratusan kandidat -- supaya peta cepat & fokus) beserta DC tujuan reverse
    logistics-nya, dibatasi ke Pulau Jawa saja (sesuai scope tampilan peta).
    Koordinat kota pakai centroid customer_lat/customer_long riil per kota
    (build_waste_master_table()), bukan daftar koordinat mock.
    """
    sol = solve_optimal_network_pure_emission()
    opened = set(sol["opened_cities"])
    city_summary = _city_level_summary()
    facilities = get_facilities_table()
    java_facilities = facilities[
        facilities.apply(lambda f: _is_in_java(f["lat"], f["long"]), axis=1)
    ]

    nodes = []
    routes = []
    for _, r in city_summary.iterrows():
        city = r["customer_city"]
        if city not in opened:
            continue
        lat, lon = r["city_lat"], r["city_lon"]
        if not _is_in_java(lat, lon):
            continue
        nodes.append(
            {
                "city": city,
                "lat": round(float(lat), 4),
                "lon": round(float(lon), 4),
                "selected": True,
                "density_usia_produktif": round(float(r["kepadatan_usia_produktif"]), 1),
                "waste_kg_per_month": round(r["w_total_kg"] / max(r["n_months"], 1), 1),
                "coverage_radius_km": _coverage_radius_km(city),
            }
        )
        best = None
        for _, f in java_facilities.iterrows():
            d = _haversine_km(lat, lon, f["lat"], f["long"])
            if best is None or d < best[0]:
                best = (d, f["dc_name"], f["lat"], f["long"])
        if best:
            routes.append(
                {
                    "from_city": city,
                    "from_lat": round(float(lat), 4),
                    "from_lon": round(float(lon), 4),
                    "to_dc": best[1],
                    "to_lat": best[2],
                    "to_lon": best[3],
                    "distance_km": round(best[0], 1),
                }
            )

    dc_list = [
        {"dc_name": f["dc_name"], "lat": f["lat"], "lon": f["long"]} for _, f in java_facilities.iterrows()
    ]
    return {"nodes": nodes, "routes": routes, "dc_facilities": dc_list, "n_stations": sol["n_stations"]}


def get_trade_off_summary() -> dict:
    """
    Financial & Emission Trade-off Summary (Tab 2): penghematan emisi bersih
    (net, sudah dikurangi emisi truk) dikonversi ke nilai finansial --
    Kredit Karbon IDXCarbon (Rp30.000/ton) & Denda Proxy EPR (Rp5.000/kg)
    yang berhasil dihindari, dikurangi biaya investasi mesin PES.
    """
    s = get_settings()
    sol = solve_optimal_network_pure_emission()
    emission_saved_kg = sol["emission_saved_kg"]
    carbon_credit_value = (emission_saved_kg / 1000.0) * s.carbon_price_per_ton
    baseline_denda_epr = s.epr_fine_per_kg * sol["target_epr_kg"]
    epr_fine_avoided = baseline_denda_epr - sol["denda_epr_monthly"]
    machine_cost_monthly = sol["c_machine_monthly"]
    net_financial_benefit = carbon_credit_value + epr_fine_avoided - machine_cost_monthly

    return {
        "emission_saved_kg": emission_saved_kg,
        "emission_saved_ton_co2e": round(emission_saved_kg / 1000.0, 2),
        "carbon_credit_value_rp": round(carbon_credit_value, 0),
        "epr_fine_avoided_rp": round(epr_fine_avoided, 0),
        "machine_cost_monthly_rp": machine_cost_monthly,
        "net_financial_benefit_rp": round(net_financial_benefit, 0),
        "n_stations": sol["n_stations"],
        "epr_compliance_pct": sol["epr_compliance_pct"],
        "epr_constraint_satisfied": sol["epr_constraint_satisfied"],
    }
