"""MCP Server read-only untuk tabel PostgreSQL machine_operation."""

import hmac
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import psycopg
import uvicorn
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from psycopg import sql
from psycopg.rows import dict_row

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("mcp-machine-operation")

# ---------------------------------------------------------------- konfigurasi
HOST = os.getenv("MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("MCP_PORT", "8000"))
AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN", "")
ALLOW_NO_AUTH = os.getenv("ALLOW_NO_AUTH", "false").lower() == "true"

TABLE_SCHEMA = os.getenv("TABLE_SCHEMA", "public")
TABLE_NAME = os.getenv("TABLE_NAME", "machine_operation")
ORDER_BY_COLUMN = os.getenv("ORDER_BY_COLUMN", "").strip()
ORDER_DIRECTION = "ASC" if os.getenv("ORDER_DIRECTION", "DESC").upper() == "ASC" else "DESC"
DATE_COLUMN = os.getenv("DATE_COLUMN", "").strip()

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "50"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "200"))
STATEMENT_TIMEOUT_MS = int(os.getenv("STATEMENT_TIMEOUT_MS", "15000"))
TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Jakarta"))

TABLE = sql.Identifier(TABLE_SCHEMA, TABLE_NAME)

# host=0.0.0.0 -> mematikan proteksi DNS-rebinding bawaan FastMCP yang hanya
# mengizinkan Host "localhost" (akan menolak request lewat domain/reverse proxy).
# Keamanan dijaga oleh bearer token + reverse proxy HTTPS.
mcp = FastMCP(
    "mcp-machine-operation",
    host=HOST,
    port=PORT,
    stateless_http=True,
    json_response=True,
)


# -------------------------------------------------------------------- helper
def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
        connect_timeout=10,
        row_factory=dict_row,
        options=(
            "-c default_transaction_read_only=on "
            f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"
        ),
    )


def _jsonable(value: Any) -> Any:
    """Ubah tipe PostgreSQL (Decimal, datetime, UUID, ...) agar aman jadi JSON."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _clamp_limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_LIMIT))


def _parse_date(value: str) -> date:
    v = value.strip().lower()
    if v == "today":
        return datetime.now(TZ).date()
    if v == "yesterday":
        return datetime.now(TZ).date() - timedelta(days=1)
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        raise ValueError(
            f"Format tanggal tidak valid: '{value}'. Gunakan YYYY-MM-DD, 'today', atau 'yesterday'."
        ) from None


def _table_columns(conn: psycopg.Connection) -> list[dict[str, str]]:
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (TABLE_SCHEMA, TABLE_NAME),
    ).fetchall()
    if not rows:
        raise ValueError(
            f"Tabel {TABLE_SCHEMA}.{TABLE_NAME} tidak ditemukan atau user tidak punya akses."
        )
    return rows


def _require_column(name: str, columns: list[dict[str, str]], env_name: str) -> str:
    if name not in {c["column_name"] for c in columns}:
        raise ValueError(f"Kolom '{name}' (dari {env_name}) tidak ada di tabel.")
    return name


def _order_clause(columns: list[dict[str, str]]) -> sql.Composable:
    if not ORDER_BY_COLUMN:
        return sql.SQL("")
    col = _require_column(ORDER_BY_COLUMN, columns, "ORDER_BY_COLUMN")
    return sql.SQL(" ORDER BY {} {}").format(sql.Identifier(col), sql.SQL(ORDER_DIRECTION))


def _safe_call(fn):
    """Jalankan fn; kembalikan pesan aman jika gagal (tanpa bocor kredensial)."""
    try:
        return fn()
    except ValueError as exc:  # pesan validasi buatan sendiri, aman ditampilkan
        return {"error": str(exc)}
    except Exception:
        log.exception("Gagal menjalankan tool")
        return {"error": "Gagal mengambil data. Periksa log MCP server."}


# --------------------------------------------------------------------- tools
@mcp.tool()
def get_table_schema() -> dict[str, Any]:
    """Tampilkan daftar kolom dan tipe data tabel machine_operation.

    Panggil ini lebih dulu bila perlu mengetahui struktur data.
    """

    def run():
        with _connect() as conn:
            cols = _table_columns(conn)
        return {"table": f"{TABLE_SCHEMA}.{TABLE_NAME}", "columns": cols}

    return _safe_call(run)


@mcp.tool()
def get_machine_operations(limit: int = DEFAULT_LIMIT, offset: int = 0) -> dict[str, Any]:
    """Ambil record dari tabel machine_operation (dengan pagination).

    Args:
        limit: jumlah baris maksimum (dibatasi MAX_LIMIT).
        offset: jumlah baris yang dilewati, untuk halaman berikutnya.
    """

    def run():
        safe_limit = _clamp_limit(limit)
        safe_offset = max(0, int(offset))
        with _connect() as conn:
            cols = _table_columns(conn)
            query = sql.SQL("SELECT * FROM {t}{order} LIMIT %s OFFSET %s").format(
                t=TABLE, order=_order_clause(cols)
            )
            rows = conn.execute(query, (safe_limit, safe_offset)).fetchall()
        return {
            "table": f"{TABLE_SCHEMA}.{TABLE_NAME}",
            "count": len(rows),
            "limit": safe_limit,
            "offset": safe_offset,
            "ordered_by": ORDER_BY_COLUMN or None,
            "records": _jsonable(rows),
        }

    return _safe_call(run)


@mcp.tool()
def get_machine_operations_by_date(
    start_date: str,
    end_date: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    """Ambil record machine_operation berdasarkan rentang tanggal (inklusif).

    Args:
        start_date: YYYY-MM-DD, 'today', atau 'yesterday'.
        end_date: YYYY-MM-DD / 'today' / 'yesterday'. Kosong = sama dengan start_date.
        limit: jumlah baris maksimum (dibatasi MAX_LIMIT).
        offset: jumlah baris yang dilewati.
    """

    def run():
        if not DATE_COLUMN:
            raise ValueError("DATE_COLUMN belum dikonfigurasi di server (.env).")
        start = _parse_date(start_date)
        end = _parse_date(end_date) if end_date else start
        if end < start:
            raise ValueError("end_date tidak boleh lebih awal dari start_date.")
        safe_limit = _clamp_limit(limit)
        safe_offset = max(0, int(offset))
        with _connect() as conn:
            cols = _table_columns(conn)
            col = sql.Identifier(_require_column(DATE_COLUMN, cols, "DATE_COLUMN"))
            query = sql.SQL(
                "SELECT * FROM {t} WHERE {c} >= %s AND {c} < %s{order} LIMIT %s OFFSET %s"
            ).format(t=TABLE, c=col, order=_order_clause(cols))
            rows = conn.execute(
                query, (start, end + timedelta(days=1), safe_limit, safe_offset)
            ).fetchall()
        return {
            "table": f"{TABLE_SCHEMA}.{TABLE_NAME}",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "count": len(rows),
            "limit": safe_limit,
            "offset": safe_offset,
            "records": _jsonable(rows),
        }

    return _safe_call(run)


@mcp.tool()
def count_machine_operations(
    start_date: str | None = None, end_date: str | None = None
) -> dict[str, Any]:
    """Hitung jumlah record, opsional dalam rentang tanggal (butuh DATE_COLUMN).

    Args:
        start_date: YYYY-MM-DD / 'today' / 'yesterday'. Kosong = hitung semua.
        end_date: kosong = sama dengan start_date.
    """

    def run():
        with _connect() as conn:
            if not start_date:
                total = conn.execute(
                    sql.SQL("SELECT COUNT(*) AS n FROM {t}").format(t=TABLE)
                ).fetchone()["n"]
                return {"table": f"{TABLE_SCHEMA}.{TABLE_NAME}", "total": total}

            if not DATE_COLUMN:
                raise ValueError("DATE_COLUMN belum dikonfigurasi di server (.env).")
            start = _parse_date(start_date)
            end = _parse_date(end_date) if end_date else start
            if end < start:
                raise ValueError("end_date tidak boleh lebih awal dari start_date.")
            cols = _table_columns(conn)
            col = sql.Identifier(_require_column(DATE_COLUMN, cols, "DATE_COLUMN"))
            query = sql.SQL(
                "SELECT COUNT(*) AS n FROM {t} WHERE {c} >= %s AND {c} < %s"
            ).format(t=TABLE, c=col)
            total = conn.execute(query, (start, end + timedelta(days=1))).fetchone()["n"]
        return {
            "table": f"{TABLE_SCHEMA}.{TABLE_NAME}",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "total": total,
        }

    return _safe_call(run)


# ------------------------------------------------------- auth + health (ASGI)
class AuthMiddleware:
    """Wajibkan header 'Authorization: Bearer <token>'. /health dikecualikan."""

    def __init__(self, app, token: str):
        self.app = app
        self.expected = f"Bearer {token}".encode()

    async def _reply(self, send, status: int, body: dict, extra_headers=()):
        payload = json.dumps(body).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode()),
                    *extra_headers,
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        if scope["path"] == "/health":
            return await self._reply(send, 200, {"status": "ok"})

        if self.expected != b"Bearer ":
            headers = dict(scope["headers"])
            provided = headers.get(b"authorization", b"")
            if not hmac.compare_digest(provided, self.expected):
                log.warning("Akses ditolak (token salah/hilang) path=%s", scope["path"])
                return await self._reply(
                    send,
                    401,
                    {"error": "unauthorized"},
                    [(b"www-authenticate", b"Bearer")],
                )
        return await self.app(scope, receive, send)


def main() -> None:
    if not AUTH_TOKEN and not ALLOW_NO_AUTH:
        raise SystemExit(
            "MCP_AUTH_TOKEN belum diisi. Buat dengan `openssl rand -hex 32`, "
            "atau set ALLOW_NO_AUTH=true hanya untuk pengujian lokal."
        )
    if not AUTH_TOKEN:
        log.warning("PERINGATAN: server berjalan TANPA autentikasi (ALLOW_NO_AUTH=true).")

    app = AuthMiddleware(mcp.streamable_http_app(), AUTH_TOKEN)
    log.info("Menjalankan MCP di %s:%s (endpoint /mcp)", HOST, PORT)
    uvicorn.run(app, host=HOST, port=PORT, log_level=os.getenv("LOG_LEVEL", "info").lower())


if __name__ == "__main__":
    main()
