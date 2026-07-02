import asyncio
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
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL         = os.getenv("DATABASE_URL",         "postgresql://srai:srai_pass@postgres:5432/srai_db")
MINIO_ENDPOINT       = os.getenv("MINIO_ENDPOINT",       "minio:9000")
MINIO_ACCESS         = os.getenv("MINIO_ACCESS_KEY",     "minioadmin")
MINIO_SECRET         = os.getenv("MINIO_SECRET_KEY",     "minioadmin123")
MINIO_BUCKET         = os.getenv("MINIO_BUCKET",         "srai-images")
MAX_CAPTURES_TODAY   = int(os.getenv("MAX_CAPTURES_TODAY",   "150"))
DB_RETENTION_MONTHS  = int(os.getenv("DB_RETENTION_MONTHS",  "6"))
MINIO_RETENTION_DAYS = int(os.getenv("MINIO_RETENTION_DAYS", "30"))

db_pool: asyncpg.Pool = None

_NAN_RE = re.compile(r'\bNaN\b')


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_json_loads(s: str):
    """json.loads tolerante a NaN (convierte a null)."""
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return json.loads(_NAN_RE.sub('null', s))


async def _init_conn(conn):
    """Registra codec JSONB para garantizar decodificación a dict."""
    await conn.set_type_codec(
        'jsonb',
        encoder=json.dumps,
        decoder=_safe_json_loads,
        schema='pg_catalog',
    )


def _clean_probs(v) -> dict:
    """Normaliza probabilidades a dict con valores float finitos."""
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
            f = float(v)
            d[k] = f if math.isfinite(f) else None      # NaN/Inf → null (JSON válido)
        elif isinstance(v, float):
            d[k] = v if math.isfinite(v) else None
        elif k == 'probabilidades':
            d[k] = _clean_probs(v)
    return d


def _minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def _existing_paths():
    """
    Conjunto de object_names presentes actualmente en MinIO. Devuelve None si
    MinIO no responde (en ese caso no se filtra, para no ocultar todo ante una
    caída temporal del almacenamiento).
    """
    try:
        return {obj.object_name for obj in _minio().list_objects(MINIO_BUCKET, recursive=True)}
    except Exception as exc:
        logger.warning("No se pudo listar MinIO; no se filtran capturas: %s", exc)
        return None


# ─── Retención de datos ───────────────────────────────────────────────────────

def _configure_minio_lifecycle():
    """
    Registra en MinIO una política de ciclo de vida que expira objetos
    automáticamente a los MINIO_RETENTION_DAYS días.
    MinIO aplica el borrado en segundo plano; no requiere intervención manual.
    """
    try:
        from minio.commonconfig import ENABLED
        from minio.lifecycleconfig import Expiration, Filter, LifecycleConfig, Rule

        config = LifecycleConfig([
            Rule(
                ENABLED,
                rule_filter=Filter(prefix=""),
                rule_id="srai-auto-expire",
                expiration=Expiration(days=MINIO_RETENTION_DAYS),
            )
        ])
        _minio().set_bucket_lifecycle(MINIO_BUCKET, config)
        logger.info(
            "Política MinIO configurada: imágenes se eliminan a los %d días",
            MINIO_RETENTION_DAYS,
        )
    except Exception as exc:
        logger.warning("No se pudo configurar lifecycle MinIO: %s", exc)


async def _run_db_cleanup():
    """
    Elimina de las tres tablas los registros más antiguos que DB_RETENTION_MONTHS
    meses. Usa make_interval para evitar hardcodeo y pasar el valor como parámetro.
    """
    try:
        async with db_pool.acquire() as conn:
            r1 = await conn.execute(
                "DELETE FROM sensor_readings "
                "WHERE recorded_at < NOW() - make_interval(months => $1)",
                DB_RETENTION_MONTHS,
            )
            r2 = await conn.execute(
                "DELETE FROM sensor_alerts "
                "WHERE alerted_at  < NOW() - make_interval(months => $1)",
                DB_RETENTION_MONTHS,
            )
            r3 = await conn.execute(
                "DELETE FROM captures "
                "WHERE received_at < NOW() - make_interval(months => $1)",
                DB_RETENTION_MONTHS,
            )
        logger.info(
            "Limpieza BD (%d meses): %s lecturas | %s alertas | %s capturas",
            DB_RETENTION_MONTHS, r1, r2, r3,
        )
    except Exception as exc:
        logger.error("Error en limpieza de BD: %s", exc)


async def _cleanup_loop():
    """Tarea de fondo: limpieza diaria de datos históricos."""
    while True:
        await asyncio.sleep(24 * 3600)   # primera ejecución 24 h después del arranque
        logger.info("Iniciando limpieza periódica de datos históricos…")
        await _run_db_cleanup()


# ─── Lifecycle ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, init=_init_conn
    )
    logger.info("Pool de BD inicializado con codec JSONB")

    _configure_minio_lifecycle()
    cleanup_task = asyncio.create_task(_cleanup_loop())

    yield

    cleanup_task.cancel()
    await db_pool.close()


app = FastAPI(title="SRAI Dashboard", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


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
    """
    Capturas del día actual (límite MAX_CAPTURES_TODAY). Se descartan las capturas
    cuya imagen ya no existe en MinIO, para que el carrusel no muestre huecos ni
    un contador desfasado cuando se borra el bucket.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT capture_id, device_id, received_at, processed_at,
                   minio_path, clase, confianza, probabilidades
            FROM captures
            WHERE DATE(received_at) = CURRENT_DATE
              AND minio_path IS NOT NULL
            ORDER BY received_at DESC
            LIMIT $1
        """, MAX_CAPTURES_TODAY)

    existing = _existing_paths()   # None si MinIO no responde → no se filtra
    captures = []
    for row in rows:
        d = _to_dict(row)
        d["capture_id"] = str(d["capture_id"])
        path = d.get("minio_path")
        if not path:
            continue
        if existing is not None and path not in existing:
            continue   # imagen borrada del bucket → se excluye
        d["image_url"] = f"/api/image/{path}"
        captures.append(d)
    return {"data": captures, "count": len(captures)}


# ─── Sensores ────────────────────────────────────────────────────────────────

@app.get("/api/sensors/all-latest")
async def sensors_all_latest():
    """Última lectura de cada zona de cada dispositivo ESP32 registrado."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (device_id, zona) *
            FROM sensor_readings
            ORDER BY device_id, zona, recorded_at DESC
        """)
    devices = {}
    for row in rows:
        d = _to_dict(row)
        devices.setdefault(d["device_id"], {})[d["zona"]] = d
    return {"data": devices, "devices": sorted(devices.keys())}


@app.get("/api/sensors/averages")
async def sensors_averages():
    """
    Promedio general de cada tipo de sensor calculado a partir de la ÚLTIMA
    lectura de cada zona (una por device_id+zona), NO sobre todas las lecturas
    históricas de la base de datos.
    """
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            WITH latest AS (
                SELECT DISTINCT ON (device_id, zona) *
                FROM sensor_readings
                ORDER BY device_id, zona, recorded_at DESC
            )
            SELECT
                COUNT(DISTINCT device_id)            AS device_count,
                COUNT(*)                             AS zone_count,
                COUNT(*)                             AS reading_count,
                ROUND(AVG(temperatura)::numeric,  2) AS avg_temperatura,
                ROUND(AVG(hum_ambiente)::numeric, 2) AS avg_hum_ambiente,
                ROUND(AVG(hum_suelo)::numeric,    1) AS avg_hum_suelo,
                ROUND(AVG(co2_ppm)::numeric,      1) AS avg_co2_ppm,
                ROUND(AVG(co_ppm)::numeric,       4) AS avg_co_ppm,
                ROUND(AVG(nh3_ppm)::numeric,      4) AS avg_nh3_ppm,
                ROUND(AVG(alcohol_ppm)::numeric,  4) AS avg_alcohol_ppm,
                ROUND(AVG(humo_ppm)::numeric,     4) AS avg_humo_ppm,
                ROUND(AVG(tolueno_ppm)::numeric,  4) AS avg_tolueno_ppm,
                ROUND(AVG(acetona_ppm)::numeric,  4) AS avg_acetona_ppm,
                MAX(recorded_at)                     AS newest_reading
            FROM latest
        """)
    if not row or row["device_count"] == 0:
        return {"data": None, "message": "Sin lecturas registradas"}
    return {"data": _to_dict(row)}


# ─── Históricos ──────────────────────────────────────────────────────────────

# Valores derivados de whitelist, nunca de entrada del usuario — safe para f-string
_PERIOD_CFG = {
    "day":   {"trunc": "hour", "interval": "1 day"},
    "week":  {"trunc": "day",  "interval": "7 days"},
    "month": {"trunc": "day",  "interval": "30 days"},
}


@app.get("/api/sensors/history")
async def sensors_history(
    period: str = Query("day", pattern="^(day|week|month)$"),
):
    """Promedio de sensores agrupado por hora (day) o por día (week/month)."""
    cfg = _PERIOD_CFG[period]
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                date_trunc('{cfg["trunc"]}', recorded_at)   AS periodo,
                COUNT(*)                                     AS registros,
                COUNT(DISTINCT (device_id, zona))            AS zone_count,
                ROUND(AVG(temperatura)::numeric,   1)        AS avg_temperatura,
                ROUND(AVG(hum_ambiente)::numeric,  1)        AS avg_hum_ambiente,
                ROUND(AVG(hum_suelo)::numeric,     1)        AS avg_hum_suelo,
                ROUND(AVG(co2_ppm)::numeric,       1)        AS avg_co2_ppm,
                ROUND(AVG(co_ppm)::numeric,        4)        AS avg_co_ppm,
                ROUND(AVG(nh3_ppm)::numeric,       4)        AS avg_nh3_ppm
            FROM sensor_readings
            WHERE recorded_at > NOW() - INTERVAL '{cfg["interval"]}'
            GROUP BY 1
            ORDER BY 1 DESC
        """)
    return {"data": [_to_dict(r) for r in rows], "period": period}


@app.get("/api/inference/history")
async def inference_history(
    period: str = Query("day", pattern="^(day|week|month)$"),
):
    """Conteo e inferencia promedio agrupado por hora (day) o por día (week/month)."""
    cfg = _PERIOD_CFG[period]
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                date_trunc('{cfg["trunc"]}', received_at)   AS periodo,
                clase,
                COUNT(*)                                     AS total,
                ROUND(AVG(confianza)::numeric, 3)            AS avg_confianza
            FROM captures
            WHERE received_at > NOW() - INTERVAL '{cfg["interval"]}'
            GROUP BY 1, 2
            ORDER BY 1 DESC, total DESC
        """)
    return {"data": [_to_dict(r) for r in rows], "period": period}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "srai_dashboard",
        "db_retention_months": DB_RETENTION_MONTHS,
        "minio_retention_days": MINIO_RETENTION_DAYS,
        "timestamp": datetime.utcnow().isoformat(),
    }
