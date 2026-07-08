# Paragon Empties Station Network & Monitoring

Dashboard analytics (deskriptif) + simulasi optimasi (preskriptif) untuk
jaringan **Paragon Empties Station (PES)** — reverse vending machine berbasis
IoT untuk menarik kembali kemasan skincare kosong dari konsumen, guna
memenuhi kewajiban EPR (Permen LHK No. 75/2019, target wajib 20%) sambil
meminimalkan jejak emisi karbon.

Spesifikasi lengkap model & formula ada di `CLAUDE.md`.

---

## 1. Arsitektur 2-Tab

### Tab 1 — MONITORING (deskriptif/historis)
- Filter: Product Brand, Category, Format, Customer Channel
- KPI Cards: Total Volume Sampah di Pasar, Total Emisi Baseline, Estimasi
  Capaian EPR, Nilai Karbon Berjalan
- Trendline emisi bulanan (Jul 2025–Jun 2026)
- Bar chart spasial (emisi per kabupaten/kota customer)
- Treemap hirarki produk (Brand → Category → Format)
- Target Line Indicator (aktual tertarik vs target wajib EPR 20%)

### Tab 2 — SIMULATION (preskriptif/optimasi)
- Perbandingan side-by-side: **Skenario 1 Baseline Waste Fate** (60% TPA/30%
  bakar/10% hanyut) vs **Skenario 2 Jaringan PES Optimal** (50% TPA/20%
  bakar/20% PES/10% hanyut)
- Peta interaktif Leaflet.js: demand nodes (kepadatan penduduk usia
  produktif BPS), titik PES terpilih, DC facilities, rute reverse logistics
- Financial & Emission Trade-off Summary: emisi diselamatkan, nilai kredit
  karbon, denda EPR terhindarkan, biaya mesin, net financial impact

---

## 2. Objective Function (Tab 2 Simulation)

```
Minimize   E_total(x) = E_PES_Daur_Ulang + E_Truk_Logistik + E_Baseline_Fate

Subject to:
  W_collected(x) >= 20% x Total_Berat_Kemasan_Terjual     (HARD constraint)
  f_j = CEIL(Volume_PES_j / Kapasitas_Truk_per_Trip)        (round-trip truck)
```

Diselesaikan di `services/data_service.py` fungsi
`solve_optimal_network_pure_emission()` dengan algoritma constrained-greedy:

1. Ranking semua kabupaten berdasarkan `delta_E_j` (penurunan emisi bersih
   jika PES dibuka di kabupaten itu) dari terbesar ke terkecil.
2. Buka kabupaten top-ranked satu per satu **sampai constraint EPR≥20%
   terpenuhi** (pakai kabupaten yang paling menguntungkan emisi dulu).
3. Setelah constraint terpenuhi, tetap buka kabupaten lain yang `delta_E_j`
   masih positif (karena itu tetap menurunkan `E_total` lebih lanjut).
4. Kabupaten `delta_E_j <= 0` hanya dibuka kalau masih diperlukan untuk
   memenuhi constraint.

> Catatan: eksperimen dual-objective (cost vs emission tanpa constraint
> keras) dari iterasi sebelumnya tetap tersedia di endpoint legacy
> `/api/cost-emission-frontier` & `/api/pes-locations` untuk eksplorasi
> lanjutan, tapi Tab 2 Simulation di UI memakai solver constraint-EPR di atas
> (lebih sesuai brief).

---

## 3. Tech Stack

| Layer | Teknologi |
|---|---|
| Backend | Python (FastAPI) |
| Database | Snowflake / CSV lokal / mock (lihat §6) |
| Styling | Tailwind CSS (CDN) — light mode, navy sidebar, soft blue accent |
| Chart | Chart.js + `chartjs-chart-treemap` plugin |
| Peta | Leaflet.js + OpenStreetMap tiles |
| Interaktivitas | Alpine.js |
| Ikon | Font Awesome 6 |

---

## 4. Struktur Project

```
project/
├── main.py                       # entrypoint FastAPI, routing Tab1/Tab2
├── requirements.txt
├── .env.example
├── data/                          # taruh CSV lokal di sini (mode DATA_MODE=csv)
│   └── README.md
├── services/
│   ├── __init__.py
│   └── data_service.py           # query, filter, formula emisi, solver
└── templates/
    └── index.html                # dashboard 2-tab (Tailwind+Alpine+Chart.js+Leaflet)
```

## 5. Cara Menjalankan

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
uvicorn main:app --reload --port 8000
```
Buka `http://localhost:8000`.

## 6. Mode Data — Mock / CSV / Snowflake

Dikontrol lewat env var `DATA_MODE` di `.env`:

- **`DATA_MODE=mock`** (default) — data sintetis 15 kabupaten/kota Jawa–Bali.
- **`DATA_MODE=csv`** — baca dari file CSV lokal, lihat `data/README.md` untuk
  kolom wajib tiap file & cara set path-nya (`CSV_SALES_PATH` dkk di `.env`).
- **`DATA_MODE=snowflake`** — query live ke 5 tabel Snowflake, isi
  `SNOWFLAKE_ACCOUNT/USER/PASSWORD/WAREHOUSE/DATABASE/SCHEMA` di `.env`.

## 7. Endpoint API

**Tab 1 Monitoring** (semua terima query param opsional `?brand=&category=&product_format=&channel=`):

| Endpoint | Fungsi |
|---|---|
| `GET /api/filters` | Opsi dropdown filter |
| `GET /api/kpi` | KPI cards |
| `GET /api/emission-trend` | Trendline emisi bulanan |
| `GET /api/emission-by-region` | Bar chart spasial per kabupaten/kota |
| `GET /api/emission-treemap` | Treemap Brand → Category → Format |
| `GET /api/epr-target` | Target Line Indicator (aktual vs target 20%) |

**Tab 2 Simulation:**

| Endpoint | Fungsi |
|---|---|
| `GET /api/simulation/scenario-comparison` | Side-by-side Baseline vs PES Optimal |
| `GET /api/simulation/network-map` | Data peta: nodes, routes, DC facilities |
| `GET /api/simulation/tradeoff` | Financial & emission trade-off summary |

**Legacy/eksperimen:**

| Endpoint | Fungsi |
|---|---|
| `GET /api/cost-emission-frontier?objective=cost\|emission` | Dual-objective frontier (tanpa constraint keras) |
| `GET /api/pes-locations?objective=cost\|emission` | Kandidat lokasi dari dual-objective |
| `GET /api/location-crosswalk-report` | Audit rekonsiliasi nama wilayah |
| `GET /api/health` | Status server & mode data |

## 8. Data Gap yang Masih Perlu Dikonfirmasi

- `Cap_Emisi_Perusahaan` — belum ada keputusan bisnis, default 0.
- `Kapasitas_Truk_per_Trip` — asumsi default 500 kg/trip, perlu konfirmasi tim logistik.
- Baris `needs_review=True` di location crosswalk (kalau ada saat pakai data riil).
