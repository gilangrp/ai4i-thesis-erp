# Machine Failure Prediction (MFP) — MLOps Pipeline

PYTHON VERSION: 3.11.9

Arsitektur end-to-end Machine Learning Operations (MLOps) untuk prediksi kegagalan mesin industri (*predictive maintenance*), mengintegrasikan **Database SQL**, **Apache Airflow**, **ClickHouse**, **MLflow**, dan **FastAPI**.

---

## 🏗️ Arsitektur & Alur Data

```
[ Sensor / Data CSV ]
        │
        ▼ (csv_to_dbsql_quartal_insert.py)
[ Database SQL: MACHINE_OPERATION_STAGING ]  <── Buffer data baru mingguan (batch)
        │
        ▼ (Airflow DAG: etl_oracle_to_clickhouse_mfp | @hourly)
        ├──> Pindahkan 250 data ──> [ Database SQL: MACHINE_OPERATION ] (Histori OLTP)
        │
        ▼ (Append data baru)
[ ClickHouse: machine_operation ]      <── OLAP Data Warehouse untuk ML
        │
        ▼ (papermill notebook)
[ MLflow Tracking & Model Registry ]   <── Model: machine_failure_xgb@champion
        │
        ▼ (Load model via API)
[ FastAPI Serving (Uvicorn + PM2) ]    <── POST /predict & /predict/batch
        │
        ▼
[ Dashboard & Client UI ]
```

---

## 📁 Struktur Direktori

```
├── dags/
│   ├── etl_oracle_to_clickhouse_mfp.py      # DAG Airflow: Incremental batch (Staging -> ClickHouse & Main)
│   └── requirements-airflow.txt             # Dependensi Python di environment Airflow
├── data/
│   └── ai4i2020.csv                         # Dataset sumber Machine Failure
├── notebooks/
│   └── SMOTE_ai4i2020_xgboost_machine_failure.ipynb  # Eksperimen, training (XGBoost + SMOTE), & MLflow log
├── scripts/
│   ├── csv_to_dbsql_insert.py              # Konversi CSV ke file SQL INSERT Database SQL
│   ├── csv_to_dbsql_quartal_insert.py      # Split dataset ke training (8.000) & weekly (8 x 250)
│   ├── sql_to_csv.py                        # Konversi balik file SQL INSERT ke file CSV
│   └── trigger_airflow_dag.py               # Script pemicu & monitor DAG Airflow via REST API
├── serving/
│   └── app.py                               # REST API FastAPI (memuat model langsung dari MLflow)
├── sql/
│   ├── create_table.sql                     # DDL Database SQL: MACHINE_OPERATION & MACHINE_OPERATION_STAGING
│   ├── create_table_clickhouse.sql          # DDL ClickHouse: database erp_ai4i & tabel machine_operation
│   ├── insert_ai4i2020.sql                  # Batch INSERT 10.000 data ke staging
│   └── quartal/                             # Pembagian data simulasi:
│       ├── training/training.sql            # 8.000 data awal (baseline)
│       └── weekly/week1.sql - week8.sql     # 250 data per minggu (simulasi batch baru)
├── ui/
│   ├── dummy_dashboard.html                 # Simulator input sensor & visualisasi probabilitas failure
│   └── dataset_viewer.html                  # Viewer tabel dataset
├── workflows/
│   ├── deploy_remote.sh                     # Orchestrator update, ETL, retraining & reload di server
│   └── mlops-pipeline.yml                   # Template CI/CD GitHub Actions (SSH deployment)
├── ecosystem.config.js                      # Konfigurasi process manager PM2 untuk FastAPI
├── requirements.txt                         # Dependensi utama server (Notebook, API, MLflow)
└── .env.example                             # Template konfigurasi environment variable
```

---

## 🚀 Panduan Menjalankan Sistem

### 1. Inisialisasi Database
1. **Database SQL:** Jalankan `sql/create_table.sql` untuk membuat tabel `MACHINE_OPERATION` dan `MACHINE_OPERATION_STAGING`.
2. **ClickHouse:** Jalankan `sql/create_table_clickhouse.sql` untuk membuat database `erp_ai4i` dan tabel `machine_operation`.
3. **Simulasi Data Masuk:** Masukkan data awal dengan mengeksekusi `sql/quartal/training/training.sql` ke tabel `MACHINE_OPERATION_STAGING`.

### 2. Pipeline ETL Airflow (Incremental Ingestion)
- **DAG:** `etl_oracle_to_clickhouse_mfp`
- **Jadwal:** Berjalan otomatis setiap 1 jam sekali (`@hourly`).
- **Mekanisme:**
  1. Mengambil **250 data** (konfigurasi variabel `BATCH_SIZE`) dari `MACHINE_OPERATION_STAGING`.
  2. Menyimpan (*append*) data tersebut ke ClickHouse `machine_operation`.
  3. Memindahkan data yang berhasil dikirim ke tabel utama Database SQL `MACHINE_OPERATION` dan menghapusnya dari staging.
  4. Memvalidasi bahwa data sudah berhasil masuk ke ClickHouse.

**Konfigurasi Airflow Connections (Admin > Connections):**
- `oracle_mfp`: Generic | Host: `1521` | Schema/Service: `FREEPDB1`
- `clickhouse_mfp`: Generic | Host: `8123` (HTTP) | Schema: `erp_ai4i`

### 3. Pelatihan Model & MLflow Tracking
Notebook membaca data dari ClickHouse, melatih model XGBoost (Baseline vs SMOTE), memilih model terbaik berdasarkan F1-score validation, dan mendaftarkannya ke MLflow Registry dengan alias `@champion`.

Eksekusi manual:
```bash
set -a; source .env; set +a
papermill notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb runs/executed.ipynb --cwd notebooks
```

### 4. Menjalankan Serving API (FastAPI)
Model dimuat langsung dari MLflow Model Registry (`models:/machine_failure_xgb@champion`).

```bash
# Mode Development
cd serving && uvicorn app:app --reload --port 8000

# Mode Produksi (via PM2)
pm2 start ecosystem.config.js
```

**Daftar Endpoint API:**
| Method | Endpoint | Keterangan |
|---|---|---|
| `GET` | `/health` | Cek status API & verifikasi apakah model sudah termuat |
| `GET` | `/model-info` | Informasi detail model aktif (nama, versi, threshold, fitur) |
| `POST` | `/predict` | Prediksi untuk 1 pembacaan data sensor |
| `POST` | `/predict/batch` | Prediksi batch (hingga 1.000 data sekaligus) |
| `POST` | `/reload` | Muat ulang model terbaru dari MLflow tanpa restart service |

**Contoh Request Prediksi:**
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "type": "L",
    "air_temp_k": 300.5,
    "process_temp_k": 310.9,
    "rpm": 1500,
    "torque_nm": 55.0,
    "tool_wear_min": 215
  }'
```

---

## 🔄 Deployment Otomatis (CI/CD)

- File template CI/CD tersedia di: `workflows/mlops-pipeline.yml`.
- Ketika dipindahkan ke `.github/workflows/`, setiap push ke branch `main` akan memicu deployment via SSH ke server:
  1. Git checkout kode terbaru.
  2. Install/update dependensi Python.
  3. Sinkronisasi DAG ke folder Airflow.
  4. Trigger DAG dan pastikan data terbaru tersinkronisasi.
  5. Menjalankan retraining notebook via Papermill.
  6. Reload FastAPI via PM2 (`pm2 reload`) dan validasi `/health`.
