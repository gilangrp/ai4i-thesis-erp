# RUN before running 'airflow standalone'
# source ~/airflow/airflow-env.sh

# Efek variabel hanya berlaku di terminal tempat file ini di-source,
# dan hilang saat terminal ditutup. Tidak dimasukkan ke ~/.zshrc.


# ~/airflow/airflow-env.sh
# Environment variable untuk menjalankan Airflow di macOS
# supaya task tidak SIGSEGV saat konek ke PostgreSQL.

# Matikan fork-safety check Objective-C milik macOS.
# Penyebab utama crash saat proses task hasil fork memanggil library C (libpq).
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Lewati lookup proxy sistem macOS yang bisa memicu crash setelah fork.
# Huruf besar dan kecil keduanya ditulis karena library berbeda membaca versi berbeda.
export NO_PROXY="*"
export no_proxy="*"

# Jalankan tiap task di interpreter Python baru, bukan hasil fork dari proses induk.
# Menghindari state library C yang rusak akibat fork.
export AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=True