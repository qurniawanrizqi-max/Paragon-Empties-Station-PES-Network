# Folder Data CSV Lokal

Letakkan 5 file CSV Anda di sini (atau di path lain, asal disesuaikan lewat
env var `CSV_*_PATH` di `.env` — lihat README.md utama §6a).

| File | Kolom wajib (minimal) |
|---|---|
| `data_sales_order_pes.csv` | `month_date, dc_code, dc_name, dc_lat, dc_long, customer_id, customer_lat, customer_long, customer_city, customer_province, product_code, packing_size_value_gram, product_brand_name, product_format_name, product_format_packaging, do_volume` |
| `dno_facilties.csv` | `dc_code, dc_name, lat, long` |
| `data_kepadatan_penduduk.csv` | `province, city, usia_produktif, luas_wilayah` (kolom `tingkat_kepadatan_penduduk` akan dihitung otomatis kalau belum ada) |
| `emission_standard_packaging_format.csv` | `product_format_packaging, faktor_emisi_dibakar, faktor_emisi_tpa, faktor_emisi_daur_ulang_pes` |
| `masa_habis_pakai_product_format.csv` | `product_form, masa_habis_pakai_median_bulan` |

Cara tercepat mendapatkan file ini dengan kolom yang sudah pas: jalankan
query yang ada di komentar `services/data_service.py` (fungsi `query_*()`)
langsung di Snowsight, lalu **Download Results as CSV** — otomatis nama
kolomnya sudah cocok.

Kalau nama kolom CSV Anda berbeda, rename dulu header-nya (paling gampang
lewat Excel) supaya sama persis dengan tabel di atas (huruf besar/kecil tidak
masalah, kode otomatis lowercase semua kolom).
