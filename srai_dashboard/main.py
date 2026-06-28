import io
import json
import logging
import math
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL        = os.getenv("DATABASE_URL",        "postgresql://srai:srai_pass@postgres:5432/srai_db")
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT",      "minio:9000")
MINIO_ACCESS        = os.getenv("MINIO_ACCESS_KEY",    "minioadmin")
MINIO_SECRET        = os.getenv("MINIO_SECRET_KEY",    "minioadmin123")
MINIO_BUCKET        = os.getenv("MINIO_BUCKET",        "srai-images")
MAX_CAPTURES_TODAY  = int(os.getenv("MAX_CAPTURES_TODAY", "150"))

db_pool: asyncpg.Pool = None

# Regex to replace JSON-invalid NaN/Infinity literals before parsing
_NAN_RE = re.compile(r'\bNaN\b')


def _safe_json_loads(s: str):
    """json.loads tolerante a NaN (convierte a null)."""
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return json.loads(_NAN_RE.sub('null', s))


async def _init_conn(conn):
    """Registra codec JSONB explícitamente para garantizar decodificación a dict."""
    await conn.set_type_codec(
        'jsonb',
        encoder=json.dumps,
        decoder=_safe_json_loads,
        schema='pg_catalog',
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, init=_init_conn
    )
    logger.info("Pool de BD inicializado con codec JSONB")
    yield
    await db_pool.close()


app = FastAPI(title="SRAI Dashboard", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


def _minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def _clean_probs(v) -> dict:
    """Normaliza el campo probabilidades a dict con valores float finitos."""
    if isinstance(v, str):
        try:
            v = _safe_json_loads(v)
        except Exception:
            return {}
    if not isinstance(v, dict):
        return {}
    return {
        ck: round(float(cv), 6)
        for ck, cv in v.items()
        if cv is not None and isinstance(cv, (int, float)) and math.isfinite(float(cv))
    }


def _to_dict(record) -> dict:
    d = dict(record)
    for k, v in list(d.items()):
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, Decimal):
            d[k] = float(v)
        elif k == 'probabilidades':
            d[k] = _clean_probs(v)
    return d


# ─── Static ──────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def index():
    return FileResponse("static/index.html")


@app.get("/api/image/{path:path}", include_in_schema=False)
async def proxy_image(path: str):
    """Proxy MinIO → browser para evitar resolución de hostname interno."""
    try:
        client = _minio()
        response = client.get_object(MINIO_BUCKET, path)
        data = response.read()
        response.close()
        return StreamingResponse(
            io.BytesIO(data),
            media_type="image/jpeg",
            headers={"Cache-Control": "no-cache"},
        )
    except S3Error as e:
        raise HTTPException(status_code=404, detail=f"Imagen no encontrada: {e}")


# ─── Captures / Inferencias ──────────────────────────────────────────────────

@app.get("/api/latest-inference")
async def latest_inference():
    """Última captura procesada con resultado de inferencia."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT capture_id, device_id, received_at, processed_at,
                   minio_path, clase, confianza, probabilidades
            FROM captures
            ORDER BY received_at DESC
            LIMIT 1
        """)
    if not row:
        return {"data": None}
    d = _to_dict(row)
    d["capture_id"] = str(d["capture_id"])
    if d.get("minio_path"):
        d["image_url"] = f"/api/image/{d['minio_path']}"
    return {"data": d}


@app.get("/api/today-captures")
async def today_captures():
    """Capturas del día actual, límite configurable por MAX_CAPTURES_TODAY."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT capture_id, device_id, received_at, processed_at,
                   minio_path, clase, confianza, probabilidades
            FROM captures
            WHERE DATE(received_at) = CURRENT_DATE
            ORDER BY received_at DESC
            LIMIT $1
        """, MAX_CAPTURES_TODAY)
    captures = []
    for row in rows:
        d = _to_dict(row)
        d["capture_id"] = str(d["capture_id"])
        if d.get("minio_path"):
            d["image_url"] = f"/api/image/{d['minio_path']}"
        captures.append(d)
    return {"data": captures, "count": len(captures)}


# ─── Sensores ────────────────────────────────────────────────────────────────

@app.get("/api/sensors/all-latest")
async def sensors_all_latest():
    """Última lectura de cada dispositivo ESP32 registrado."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (device_id) *
            FROM sensor_readings
            ORDER BY device_id, recorded_at DESC
        """)
    devices = {}
    for row in rows:
        d = _to_dict(row)
        devices[d["device_id"]] = d
    return {"data": devices, "devices": sorted(devices.keys())}


@app.get("/api/sensors/averages")
async def sensors_averages():
    """
    Promedio de sensores homólogos entre todos los ESP32 registrados en la
    última hora.  Fórmula: SUM(sensor_X de todos los dispositivos) / N lecturas
    """
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(DISTINCT device_id)                                               AS device_count,
                COUNT(*)                                                                AS reading_count,
                ROUND(AVG(temperatura)::numeric,    2)                                  AS avg_temperatura,
                ROUND(AVG(hum_ambiente)::numeric,   2)                                  AS avg_hum_ambiente,
                ROUND(AVG(hum_suelo_z1)::numeric,   1)                                  AS avg_hum_suelo_z1,
                ROUND(AVG(hum_suelo_z2)::numeric,   1)                                  AS avg_hum_suelo_z2,
                ROUND(AVG((hum_suelo_z1::float + hum_suelo_z2::float) / 2)::numeric, 1) AS avg_hum_suelo_global,
                ROUND(AVG(co2_ppm)::numeric,        1)                                  AS avg_co2_ppm,
                ROUND(AVG(co_ppm)::numeric,         4)                                  AS avg_co_ppm,
                ROUND(AVG(nh3_ppm)::numeric,        4)                                  AS avg_nh3_ppm,
                ROUND(AVG(alcohol_ppm)::numeric,    4)                                  AS avg_alcohol_ppm,
                ROUND(AVG(humo_ppm)::numeric,       4)                                  AS avg_humo_ppm,
                ROUND(AVG(tolueno_ppm)::numeric,    4)                                  AS avg_tolueno_ppm,
                ROUND(AVG(acetona_ppm)::numeric,    4)                                  AS avg_acetona_ppm,
                MAX(recorded_at)                                                        AS newest_reading
            FROM sensor_readings
            WHERE recorded_at > NOW() - INTERVAL '1 hour'
        """)
    if not row or row["device_count"] == 0:
        return {"data": None, "message": "Sin lecturas en la última hora"}
    return {"data": _to_dict(row)}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "srai_dashboard",
        "timestamp": datetime.utcnow().isoformat(),
    }
