#!/usr/bin/env python3
"""
Convert file .sql (INSERT INTO statements) -> file .csv.

Mendukung:
- Oracle / PostgreSQL / MySQL multirow INSERT:
    INSERT INTO table (col1, col2) VALUES (val1, val2), (val3, val4);
- Single row INSERT:
    INSERT INTO table (col1, col2) VALUES (val1, val2);
- Parsing aman untuk string bervalue kutip ('string with ''quote'''), angka, dan NULL.
- Memory-efficient streaming parsing (cocok untuk file SQL besar).

Contoh penggunaan:
    python scripts/sql_to_csv.py sql/quartal/training/training.sql
    python scripts/sql_to_csv.py sql/quartal/training/training.sql -o data/training.csv
    python scripts/sql_to_csv.py -i sql/quartal/training/training.sql --null-value ""
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Generator, List, Optional, Tuple


def parse_sql_values_tokens(text: str, null_val: str = "") -> Generator[List[str], None, None]:
    """
    Parser streaming berbasis state machine untuk membaca baris-baris (val1, val2, ...)
    dari potongan teks SQL yang berisi statement VALUES (...).
    """
    in_string = False
    in_tuple = False
    cur_val: List[str] = []
    cur_row: List[str] = []
    i = 0
    n = len(text)

    while i < n:
        ch = text[i]

        if in_string:
            if ch == "'":
                # Cek escape quote SQL: ''
                if i + 1 < n and text[i + 1] == "'":
                    cur_val.append("'")
                    i += 2
                    continue
                else:
                    in_string = False
                    i += 1
                    continue
            else:
                cur_val.append(ch)
                i += 1
                continue

        # Di luar string
        if not in_tuple:
            if ch == "(":
                in_tuple = True
                cur_row = []
                cur_val = []
            i += 1
            continue

        # Sedang di dalam tuple (...)
        if ch == "'":
            in_string = True
            i += 1
            continue
        elif ch == ",":
            val_str = "".join(cur_val).strip()
            if val_str.upper() == "NULL":
                cur_row.append(null_val)
            else:
                cur_row.append(val_str)
            cur_val = []
            i += 1
            continue
        elif ch == ")":
            val_str = "".join(cur_val).strip()
            if val_str.upper() == "NULL":
                cur_row.append(null_val)
            else:
                cur_row.append(val_str)
            yield cur_row
            cur_row = []
            cur_val = []
            in_tuple = False
            i += 1
            continue
        else:
            cur_val.append(ch)
            i += 1


def extract_columns_and_rows(sql_path: Path, null_val: str = "") -> Tuple[List[str], Generator[List[str], None, None]]:
    """
    Mengekstrak list nama kolom dan generator baris nilai dari file SQL.
    """
    # Regex untuk mencari nama kolom dari INSERT INTO <table> (<cols>) VALUES
    insert_pattern = re.compile(
        r"INSERT\s+INTO\s+[A-Za-z0-9_$.#\"`']+\s*\(([^)]+)\)\s*VALUES",
        re.IGNORECASE
    )

    detected_columns: List[str] = []

    def row_generator():
        nonlocal detected_columns
        with open(sql_path, "r", encoding="utf-8") as f:
            buffer = []
            collecting_values = False

            for line in f:
                stripped = line.strip()

                # Lewati baris komentar SQL tunggal atau baris kosong
                if stripped.startswith("--") or not stripped:
                    continue
                if stripped.upper().startswith("COMMIT"):
                    continue

                # Cek apakah baris ini memuat INSERT INTO
                match = insert_pattern.search(line)
                if match:
                    if not detected_columns:
                        raw_cols = match.group(1)
                        detected_columns = [
                            c.strip().strip('"').strip('`').strip("'")
                            for c in raw_cols.split(",")
                        ]
                    collecting_values = True
                    # Ambil bagian setelah VALUES
                    val_idx = match.end()
                    buffer.append(line[val_idx:])
                    continue

                if collecting_values:
                    buffer.append(line)
                    # Jika statement berakhir dengan titik koma, proses buffer
                    if ";" in line:
                        chunk = "".join(buffer)
                        buffer = []
                        collecting_values = False
                        for row in parse_sql_values_tokens(chunk, null_val=null_val):
                            yield row

            # Jika masih ada sisa buffer di akhir file
            if buffer:
                chunk = "".join(buffer)
                for row in parse_sql_values_tokens(chunk, null_val=null_val):
                    yield row

    gen = row_generator()

    # Pre-fetch baris pertama untuk memastikan kolom sudah terdeteksi
    try:
        first_row = next(gen)
    except StopIteration:
        return detected_columns, (r for r in [])

    def full_generator():
        yield first_row
        yield from gen

    # Jika tidak ada kolom yang dideteksi di INSERT INTO, buat nama default
    if not detected_columns:
        detected_columns = [f"column_{i+1}" for i in range(len(first_row))]

    return detected_columns, full_generator()


def sql_to_csv(sql_file: Path, csv_file: Path, null_val: str = "") -> int:
    """
    Mengonversi file SQL menjadi file CSV. Mengembalikan total baris yang dikonversi.
    """
    if not sql_file.is_file():
        raise FileNotFoundError(f"File SQL tidak ditemukan: {sql_file}")

    csv_file.parent.mkdir(parents=True, exist_ok=True)

    columns, rows = extract_columns_and_rows(sql_file, null_val=null_val)

    count = 0
    with open(csv_file, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        if columns:
            writer.writerow(columns)

        for row in rows:
            writer.writerow(row)
            count += 1

    return count


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "sql_input",
        nargs="?",
        default=None,
        help="Path ke file SQL input (misal: sql/quartal/training/training.sql)"
    )
    parser.add_argument(
        "-i", "--sql",
        dest="sql_flag",
        default=None,
        help="Path ke file SQL input (opsional, jika tidak memakai argumen posisi)"
    )
    parser.add_argument(
        "-o", "--csv",
        dest="csv_output",
        default=None,
        help="Path ke file CSV output (default: sama dengan path SQL namun berekstensi .csv)"
    )
    parser.add_argument(
        "--null-value",
        default="",
        help="Representasi nilai NULL di file CSV (default: string kosong '')"
    )

    args = parser.parse_args()

    input_path = args.sql_input or args.sql_flag
    if not input_path:
        parser.print_help()
        print("\nError: Mohon sediakan path file SQL input.", file=sys.stderr)
        return 1

    sql_path = Path(input_path).resolve()
    if not sql_path.exists():
        print(f"Error: File '{sql_path}' tidak ditemukan.", file=sys.stderr)
        return 1

    if args.csv_output:
        csv_path = Path(args.csv_output).resolve()
    else:
        csv_path = sql_path.with_suffix(".csv")

    print(f"[*] Mengonversi: {sql_path}")
    print(f"[*] Output CSV : {csv_path}")

    try:
        total_rows = sql_to_csv(sql_path, csv_path, null_val=args.null_value)
        print(f"[✓] Berhasil mengonversi {total_rows} baris ke '{csv_path}'.")
        return 0
    except Exception as e:
        print(f"[!] Gagal melakukan konversi: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
