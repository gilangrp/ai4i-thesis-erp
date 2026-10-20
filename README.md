# Machine Failure Prediction (MFP)

Pipeline dari data mentah sampai model yang dilayani lewat API:

```
CSV ──(1) csv_to_oracle_insert.py──> file .sql ──(manual)──> Oracle 26ai
                                                                │
                                          (2) DAG Airflow etl_oracle_to_clickhouse_mfp
                                                                ▼
                                                           ClickHouse
                                                                │
                                          (3) notebook (papermill) train + log
                                                                ▼
                                            MLflow Tracking + Model Registry
                                                  alias: machine_failure_prediction_xgb@champion
                                                                │
                                          (4) FastAPI (uvicorn + pm2) memuat dari registry
                                                                ▼
                                                    POST /predict  ──> UI
```

Trigger otomatis: push ke `main` -> GitHub Actions SSH ke server -> `workflows/deploy_remote.sh`.

## Struktur folder

```
machine-failure-mlops/
├── .github/workflows/mlops-pipeline.yml   # CI/CD: SSH > DAG > notebook > reload API
├── dags/
│   ├── etl_oracle_to_clickhouse_mfp.py    # DAG Airflow (Oracle 26ai -> ClickHouse)
│   └── requirements-airflow.txt           # dependensi untuk env Airflow
├── data/ai4i2020.csv                      # data sumber
├── notebooks/
│   └── SMOTE_ai4i2020_xgboost_machine_failure.ipynb   # training + log ke MLflow
├── scripts/
│   ├── build_explorer.py                  # bangun ui/project_explorer.html
│   ├── csv_to_oracle_insert.py            # CSV -> INSERT INTO Oracle
│   ├── trigger_airflow_dag.py             # trigger + tunggu DAG via REST API
│   └── deploy_remote.sh                   # dijalankan di server oleh workflow
├── serving/app.py                         # FastAPI, model dari MLflow Registry
├── sql/insert_ai4i2020.sql                # hasil generate (tabel MACHINE_OPERATION)
├── ui/
│   ├── dummy_dashboard.html               # contoh form prediksi
│   ├── pipeline_monitor.html              # contoh status pipeline + model registry
│   └── project_explorer.html              # daftar file project + isinya
├── runs/                                  # hasil eksekusi notebook (papermill)
├── ecosystem.config.js                    # konfigurasi pm2
├── requirements.txt
└── .env.example
```

## 1. CSV ke script INSERT Oracle

```bash
python scripts/csv_to_oracle_insert.py \
  --csv data/ai4i2020.csv --out sql/insert_ai4i2020.sql \
  --table MACHINE_OPERATION [--schema NAMA_SCHEMA] [--batch-size 500] [--style multirow|single]
```

- `multirow` (default): `INSERT INTO t (...) VALUES (...),(...);` per 500 baris, sintaks Oracle 23ai/26ai. Maksimal 1000 baris per statement.
- `single`: satu INSERT per baris, untuk kompatibilitas.
- Kolom (sesuai urutan CSV): `UDI, PRODUCT_ID, TYPE, AIR_TEMP_K, PROCESS_TEMP_K, RPM, TORQUE_NM, TOOL_WEAR_MIN, MACHINE_FAILURE, TWF, HDF, PWF, OSF, RNF`. Kalau skema Anda memakai nama lain, ubah daftar `COLUMNS` di bagian atas script.
- `sql/insert_ai4i2020.sql` sudah dibuat dari CSV yang dilampirkan (10.000 baris, tanpa `CREATE TABLE`). Hanya berisi INSERT dan `COMMIT`.

## 2. DAG Airflow (Oracle ke ClickHouse)

`etl_oracle_to_clickhouse_mfp`: `create_clickhouse_table` > `extract_and_load` > `validate`. Tanpa jadwal, hanya dipicu lewat API. Full refresh: tabel ClickHouse di-`TRUNCATE` lalu diisi ulang, lalu jumlah baris dan jumlah failure dibandingkan dengan Oracle.

Yang perlu disiapkan di Airflow:

| Item | Nilai |
|---|---|
| Connection `oracle_mfp` | Host, Port 1521, Login, Password, Extra `{"service_name": "..."}` |
| Connection `clickhouse_mfp` | Host, Port 8123 (HTTP), Login, Password, **Schema = nama database** |
| Env `ORACLE_SOURCE_TABLE` | default `MACHINE_OPERATION`, boleh `SCHEMA.TABEL` |
| Env `CH_TABLE` | default `machine_operation` |
| Paket | `pip install -r dags/requirements-airflow.txt` |

DAG ditulis kompatibel Airflow 2.x dan 3.x. Nama kolom di Oracle harus sama dengan daftar di atas (query memakai kolom eksplisit).

## 3. Notebook ke MLflow

Notebook membaca ClickHouse (`DATA_SOURCE=clickhouse`; `csv` untuk eksperimen lokal), melatih baseline dan SMOTE, lalu:

- mencatat dua run (param, metrik val + test, confusion matrix, feature importance, model + signature),
- memilih champion (`MODEL_TO_REGISTER=auto` = **F1 validation** tertinggi, bukan test set),
- mendaftarkan ke registry dan memberi alias `champion`. Threshold, fitur, dan type_map disimpan sebagai **tag model version**.

Semua konfigurasi lewat environment variable (lihat `.env.example`). Jalankan manual:

```bash
set -a; source .env; set +a
papermill notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb runs/out.ipynb --cwd notebooks
```

## 4. FastAPI

```bash
cd serving && uvicorn app:app --reload --port 8000     # dev
pm2 start ecosystem.config.js                          # produksi
```

| Endpoint | Fungsi |
|---|---|
| `GET /health` | status + `model_loaded` |
| `GET /model-info` | nama, versi, threshold, fitur, run_id MLflow |
| `POST /predict` | satu pembacaan sensor |
| `POST /predict/batch` | `{"items": [...]}`, maks 1000 |
| `POST /reload` | muat ulang dari MLflow tanpa restart, header `X-API-Key: $ADMIN_API_KEY` |

```bash
curl -s localhost:8000/predict -H 'Content-Type: application/json' -d '{
  "type":"L","air_temp_k":300.5,"process_temp_k":310.9,
  "rpm":1500,"torque_nm":55.0,"tool_wear_min":215}'
```

Docs interaktif: `http://localhost:8000/docs`. Model dimuat dari `models:/<MLFLOW_MODEL_NAME>@<MLFLOW_MODEL_ALIAS>`, tidak ada file model di server. Kalau MLflow tidak terjangkau saat start, API tetap hidup dengan `model_loaded:false` dan `/predict` mengembalikan 503.

## 5. CI/CD (GitHub Actions)

File: `.github/workflows/mlops-pipeline.yml`. Jalan saat push ke `main` yang mengubah `dags/`, `notebooks/`, `serving/`, `scripts/`, `requirements.txt`, atau `ecosystem.config.js` (atau manual lewat *Run workflow*).

Urutan di server (`workflows/deploy_remote.sh`): update git > install dependency > copy DAG ke `AIRFLOW_DAGS_DIR` > trigger DAG via API dan tunggu sukses > jalankan notebook dengan papermill > `pm2 startOrReload` > cek `/health` memastikan model termuat. Kalau salah satu langkah gagal, workflow merah dan langkah berikutnya tidak jalan (API lama tetap melayani model lama).

**GitHub Secrets** (Settings > Secrets and variables > Actions):

| Secret | Isi |
|---|---|
| `SSH_HOST`, `SSH_USER` | tujuan SSH |
| `SSH_PORT` | opsional, default 22 |
| `SSH_PRIVATE_KEY` | private key khusus deploy |
| `SSH_KNOWN_HOSTS` | disarankan, output `ssh-keyscan -H host` (tanpa ini host key di-scan saat run) |
| `APP_DIR` | path project di server, mis. `/opt/machine-failure-mlops` |

Kredensial Airflow, ClickHouse, dan MLflow **tidak** disimpan di GitHub, tetapi di `.env` pada server.

**Setup server sekali saja:**

```bash
git clone <repo> /opt/machine-failure-mlops && cd /opt/machine-failure-mlops
cp .env.example .env && nano .env
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
pm2 startup   # agar API hidup lagi setelah reboot
```

Prasyarat: server punya akses `git fetch` ke repo (deploy key), `pm2`, `curl`, dan Airflow API aktif (`AIRFLOW_API_VERSION=v2` untuk Airflow 3, `v1` untuk Airflow 2).

## 6. UI

`ui/dummy_dashboard.html` bisa dibuka langsung di browser. Isinya form sensor, gauge probabilitas dengan penanda threshold, tiga indikator fitur turunan, dan riwayat. Angka hasilnya simulasi dari rumus sederhana di JavaScript, belum memanggil API. Field respons `/predict` (`label`, `probability`, `threshold`, `model_version`) sudah sejajar dengan elemen di halaman ini. Kalau nanti dihubungkan, set `CORS_ORIGINS` di `.env`.

Dua halaman contoh lain (sama-sama tanpa koneksi):

- `ui/project_explorer.html`: penjelajah file project dengan isi file tertanam. Jalankan `python scripts/build_explorer.py` setiap file project berubah. File `.csv` dan `.sql` hanya tampil metadatanya.
- `ui/pipeline_monitor.html`: gambaran status run GitHub Actions, model registry (versi, alias champion, threshold), dan perbandingan baseline vs SMOTE. Angka riwayat dan versi adalah ilustrasi.

## Catatan penting

- **Rumus fitur ada di dua tempat** (`make_features` di notebook dan `serving/app.py`). Daftar dan urutan fitur otomatis ikut model lewat tag MLflow, tetapi rumusnya tidak. Ubah keduanya bersamaan.
- **Baseline berhenti di iterasi 3** pada eksekusi awal (lihat output notebook asli), jadi perbandingan baseline vs SMOTE belum sepenuhnya adil. Kalau champion terpilih terasa janggal, cek dulu early stopping sebelum menyimpulkan.
- `TRUNCATE` + reload berarti ClickHouse kosong sesaat selama DAG berjalan. Untuk 10.000 baris ini hitungan detik.
- `/reload` hanya memuat ulang satu proses. Dengan `UVICORN_WORKERS` > 1, gunakan `pm2 reload` agar semua worker ikut.
- Dataset bersifat sintetis dengan label dari aturan ambang, jadi skor tinggi belum tentu berlaku di data pabrik nyata.
