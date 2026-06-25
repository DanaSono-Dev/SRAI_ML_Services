import io
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",     "srai-images")
INFERENCE_URL  = os.getenv("INFERENCE_URL",    "http://inference_api:8000/v1/predict")
DATABASE_URL   = os.getenv("DATABASE_URL",     "postgresql://srai:srai_pass@postgres:5432/srai_db")
WEBHOOK_URL    = os.getenv("WEBHOOK_URL",      "")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False,
)

db_pool: asyncpg.Pool = None

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS captures (
    id             SERIAL PRIMARY KEY,
    capture_id     UUID         UNIQUE NOT NULL,
    device_id      VARCHAR(64),
    esp_timestamp  VARCHAR(32),
    received_at    TIMESTAMPTZ,
    processed_at   TIMESTAMPTZ  DEFAULT NOW(),
    minio_path     VARCHAR(512),
    image_size     INTEGER,
    clase          VARCHAR(64),
    confianza      DOUBLE PRECISION,
    probabilidades JSONB,
    webhook_sent   BOOLEAN      DEFAULT FALSE
);
"""


def _ensure_bucket():
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logger.info("Bucket '%s' creado", MINIO_BUCKET)
        else:
            logger.info("Bucket '%s' ya existe", MINIO_BUCKET)
    except S3Error as exc:
        logger.error("Error MinIO al verificar bucket: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE)
    logger.info("Tabla 'captures' verificada")
    _ensure_bucket()
    yield
    await db_pool.close()


app = FastAPI(title="SRAI Image Pipeline", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record_to_dict(record) -> dict:
    d = dict(record)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
    return d


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/v1/process")
async def process_image(
    image:         UploadFile = File(...),
    device_id:     str = Form(...),
    esp_timestamp: str = Form(...),
    received_at:   str = Form(...),
    image_size:    str = Form(...),
):
    """
    Recibe imagen del srai_receiver y ejecuta el pipeline completo:
    1. Guarda imagen en MinIO
    2. Llama a la API de inferencia
    3. Persiste resultado en PostgreSQL
    4. Envía webhook a la app movil
    """
    capture_id  = uuid.uuid4()
    image_bytes = await image.read()
    now         = datetime.now(timezone.utc)

    # --- 1. MinIO -----------------------------------------------------------
    date_str   = now.strftime("%Y-%m-%d")
    time_str   = now.strftime("%H%M%S")
    short_id   = str(capture_id)[:8]
    minio_path = f"{device_id}/{date_str}/{time_str}_{short_id}.jpg"

    try:
        minio_client.put_object(
            MINIO_BUCKET,
            minio_path,
            io.BytesIO(image_bytes),
            length=len(image_bytes),
            content_type="image/jpeg",
        )
        logger.info("Imagen guardada en MinIO: %s", minio_path)
    except S3Error as exc:
        raise HTTPException(status_code=500, detail=f"Error MinIO: {exc}")

    # --- 2. Inferencia -------------------------------------------------------
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                INFERENCE_URL,
                files={"image": ("capture.jpg", image_bytes, "image/jpeg")},
            )
            resp.raise_for_status()
            inference = resp.json()
        logger.info(
            "Inferencia: clase=%s confianza=%.4f",
            inference["clase"], inference["confianza"],
        )
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Error inferencia: {exc.response.text}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Error comunicando con inferencia: {exc}")

    # --- 3. PostgreSQL -------------------------------------------------------
    try:
        parsed_received_at = datetime.fromisoformat(received_at)
    except ValueError:
        parsed_received_at = now

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO captures
                    (capture_id, device_id, esp_timestamp, received_at,
                     minio_path, image_size, clase, confianza, probabilidades)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                capture_id,
                device_id,
                esp_timestamp,
                parsed_received_at,
                minio_path,
                int(image_size),
                inference["clase"],
                float(inference["confianza"]),
                json.dumps(inference["probabilidades"]),
            )
        logger.info("Registro guardado en BD: capture_id=%s", capture_id)
    except Exception as exc:
        logger.error("Error al guardar en BD: %s", exc)

    # --- 4. Webhook ----------------------------------------------------------
    webhook_sent = False
    if WEBHOOK_URL:
        webhook_payload = {
            "capture_id":   str(capture_id),
            "device_id":    device_id,
            "received_at":  received_at,
            "processed_at": now.isoformat(),
            "minio_path":   minio_path,
            "inference":    inference,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                wh_resp = await client.post(WEBHOOK_URL, json=webhook_payload)
                webhook_sent = wh_resp.status_code < 300
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE captures SET webhook_sent=$1 WHERE capture_id=$2",
                    webhook_sent,
                    capture_id,
                )
            logger.info("Webhook enviado: status=%d", wh_resp.status_code)
        except Exception as exc:
            logger.error("Error enviando webhook: %s", exc)
    else:
        logger.info("WEBHOOK_URL no configurada, omitiendo envio")

    return JSONResponse(
        status_code=200,
        content={
            "capture_id":   str(capture_id),
            "device_id":    device_id,
            "received_at":  received_at,
            "processed_at": now.isoformat(),
            "minio_path":   minio_path,
            "inference":    inference,
            "webhook_sent": webhook_sent,
        },
    )


@app.get("/v1/captures")
async def list_captures(limit: int = 20, device_id: str = None):
    """Retorna los ultimos registros de capturas con sus resultados de inferencia."""
    if device_id:
        rows = await db_pool.fetch(
            "SELECT * FROM captures WHERE device_id=$1 ORDER BY processed_at DESC LIMIT $2",
            device_id, limit,
        )
    else:
        rows = await db_pool.fetch(
            "SELECT * FROM captures ORDER BY processed_at DESC LIMIT $1",
            limit,
        )
    return [_record_to_dict(r) for r in rows]


@app.get("/v1/captures/{capture_id}")
async def get_capture(capture_id: str):
    """Retorna un registro especifico por capture_id."""
    try:
        cid = uuid.UUID(capture_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="capture_id invalido")

    row = await db_pool.fetchrow(
        "SELECT * FROM captures WHERE capture_id=$1", cid
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Captura no encontrada")
    return _record_to_dict(row)


@app.get("/health")
async def health():
    return {"status": "ok"}
