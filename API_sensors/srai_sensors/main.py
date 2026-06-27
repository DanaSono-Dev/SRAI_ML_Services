"""
Servicio de ingesta de datos desde ESP32 vía MQTT → PostgreSQL.

Soporta N dispositivos ESP32 simultáneos. El device_id se extrae
del tópico MQTT: invernadero/jitomate/{MQTT_CLIENT_ID}/estado
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import asyncpg
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MQTT_BROKER    = os.getenv("MQTT_BROKER",    "mosquitto")
MQTT_PORT      = int(os.getenv("MQTT_PORT",  "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "srai_sensors")
DATABASE_URL   = os.getenv("DATABASE_URL",   "postgresql://srai:srai_pass@postgres:5432/srai_db")

# Wildcards — captura todos los ESP32 sin importar cuántos haya
TOPIC_SUB_ESTADO  = "invernadero/jitomate/+/estado"
TOPIC_SUB_ALERTAS = "invernadero/jitomate/+/alertas"

db_pool: asyncpg.Pool = None
_event_loop = None

_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    id              SERIAL       PRIMARY KEY,
    device_id       VARCHAR(64)  NOT NULL,
    recorded_at     TIMESTAMPTZ  DEFAULT NOW(),
    -- DHT22
    temperatura     FLOAT,
    temp_estado     VARCHAR(16),
    hum_ambiente    FLOAT,
    hum_amb_estado  VARCHAR(16),
    -- Sensor de suelo zona 1
    hum_suelo_z1    INTEGER,
    suelo_estado_z1 VARCHAR(16),
    valvula_z1      BOOLEAN,
    -- Sensor de suelo zona 2
    hum_suelo_z2    INTEGER,
    suelo_estado_z2 VARCHAR(16),
    valvula_z2      BOOLEAN,
    -- MQ-135
    co2_ppm         FLOAT,
    co2_estado      VARCHAR(16),
    co_ppm          FLOAT,
    nh3_ppm         FLOAT,
    alcohol_ppm     FLOAT,
    humo_ppm        FLOAT,
    tolueno_ppm     FLOAT,
    acetona_ppm     FLOAT
);

CREATE TABLE IF NOT EXISTS sensor_alerts (
    id          SERIAL       PRIMARY KEY,
    device_id   VARCHAR(64)  NOT NULL,
    alerted_at  TIMESTAMPTZ  DEFAULT NOW(),
    mensaje     TEXT
);

CREATE INDEX IF NOT EXISTS idx_readings_device_time
    ON sensor_readings(device_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_device_time
    ON sensor_alerts(device_id, alerted_at DESC);
"""


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def _device_id_from_topic(topic: str) -> str:
    """Extrae el MQTT_CLIENT_ID del tópico: invernadero/jitomate/{device_id}/..."""
    parts = topic.split("/")
    return parts[2] if len(parts) >= 4 else "desconocido"


def _periodo_where(period: str, col: str) -> str:
    if period == "day":
        return f"date_trunc('day', {col}) = date_trunc('day', $2)"
    elif period == "week":
        return (
            f"{col} >= date_trunc('week', $2) "
            f"AND {col} < date_trunc('week', $2) + INTERVAL '7 days'"
        )
    else:  # month
        return f"date_trunc('month', {col}) = date_trunc('month', $2)"


def _to_dict(record) -> dict:
    d = dict(record)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, Decimal):
            d[k] = float(v)
    return d


# ---------------------------------------------------------------------------
# MQTT — callbacks
# ---------------------------------------------------------------------------

def _on_connect(client, userdata, _flags, rc):
    if rc == 0:
        logger.info("MQTT conectado a %s:%d", MQTT_BROKER, MQTT_PORT)
        client.subscribe([(TOPIC_SUB_ESTADO, 0), (TOPIC_SUB_ALERTAS, 0)])
        logger.info("Suscrito a: %s | %s", TOPIC_SUB_ESTADO, TOPIC_SUB_ALERTAS)
    else:
        logger.error("MQTT error de conexion: rc=%d", rc)


def _on_disconnect(client, userdata, rc):
    logger.warning("MQTT desconectado: rc=%d", rc)


def _on_message(client, userdata, msg):
    try:
        device_id = _device_id_from_topic(msg.topic)

        if msg.topic.endswith("/estado"):
            _handle_estado(msg, device_id)
        elif msg.topic.endswith("/alertas"):
            _handle_alerta(msg, device_id)
    except Exception:
        logger.exception("Error procesando mensaje topic=%s", msg.topic)


def _handle_estado(msg, device_id: str):
    payload = msg.payload.decode()

    # Mensaje de conexión del ESP32 — no es una lectura de sensores
    if '"evento"' in payload:
        logger.info("Dispositivo online: %s", device_id)
        return

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        logger.warning("JSON invalido de %s: %s", device_id, payload[:120])
        return

    if _event_loop:
        asyncio.run_coroutine_threadsafe(
            _guardar_lectura(device_id, data), _event_loop
        )


def _handle_alerta(msg, device_id: str):
    mensaje = msg.payload.decode().strip()
    if not mensaje:
        return

    if _event_loop:
        asyncio.run_coroutine_threadsafe(
            _guardar_alerta(device_id, mensaje), _event_loop
        )


# ---------------------------------------------------------------------------
# Persistencia
# ---------------------------------------------------------------------------

async def _guardar_lectura(device_id: str, data: dict):
    if db_pool is None:
        return

    zona1 = data.get("zona1", {})
    zona2 = data.get("zona2", {})

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sensor_readings (
                    device_id,
                    temperatura,     temp_estado,
                    hum_ambiente,    hum_amb_estado,
                    hum_suelo_z1,    suelo_estado_z1, valvula_z1,
                    hum_suelo_z2,    suelo_estado_z2, valvula_z2,
                    co2_ppm,         co2_estado,
                    co_ppm,          nh3_ppm,         alcohol_ppm,
                    humo_ppm,        tolueno_ppm,     acetona_ppm
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
                )
                """,
                device_id,
                data.get("temperatura"),
                data.get("temp_estado"),
                data.get("hum_ambiente"),
                data.get("hum_amb_estado"),
                zona1.get("hum_suelo"),
                zona1.get("suelo_estado"),
                zona1.get("valvula") == "ABIERTA",
                zona2.get("hum_suelo"),
                zona2.get("suelo_estado"),
                zona2.get("valvula") == "ABIERTA",
                data.get("co2_ppm"),
                data.get("co2_estado"),
                data.get("co_ppm"),
                data.get("nh3_ppm"),
                data.get("alcohol_ppm"),
                data.get("humo_ppm"),
                data.get("tolueno_ppm"),
                data.get("acetona_ppm"),
            )
        logger.info(
            "Lectura guardada | device=%s temp=%.1f co2=%.1f",
            device_id, data.get("temperatura") or 0, data.get("co2_ppm") or 0,
        )
    except Exception:
        logger.exception("Error guardando lectura en BD | device=%s", device_id)


async def _guardar_alerta(device_id: str, mensaje: str):
    if db_pool is None:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sensor_alerts (device_id, mensaje) VALUES ($1, $2)",
                device_id, mensaje,
            )
        logger.info("Alerta guardada | device=%s", device_id)
    except Exception:
        logger.exception("Error guardando alerta en BD | device=%s", device_id)


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------

_mqtt = mqtt.Client(client_id=MQTT_CLIENT_ID)
_mqtt.on_connect    = _on_connect
_mqtt.on_disconnect = _on_disconnect
_mqtt.on_message    = _on_message


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, _event_loop
    _event_loop = asyncio.get_running_loop()

    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute(_CREATE_TABLES)
    logger.info("Tablas verificadas")

    _mqtt.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    _mqtt.loop_start()

    yield

    _mqtt.loop_stop()
    _mqtt.disconnect()
    await db_pool.close()


app = FastAPI(title="SRAI Sensor Service", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints de consulta
# ---------------------------------------------------------------------------

@app.get("/v1/readings")
async def get_readings(
    device_id: str            = Query(..., description="MQTT_CLIENT_ID del ESP32"),
    period: str               = Query(default="day", pattern="^(day|week|month)$"),
    fecha: Optional[date]     = Query(default=None, description="YYYY-MM-DD (default: hoy)"),
    limit: int                = Query(default=100, le=1000),
):
    """Lecturas históricas de un dispositivo por período."""
    ref_dt = datetime.combine(fecha or date.today(), datetime.min.time())
    where  = _periodo_where(period, "recorded_at")

    rows = await db_pool.fetch(
        f"SELECT * FROM sensor_readings WHERE device_id=$1 AND {where} "
        f"ORDER BY recorded_at DESC LIMIT $3",
        device_id, ref_dt, limit,
    )
    return [_to_dict(r) for r in rows]


@app.get("/v1/readings/latest")
async def get_latest(
    device_id: str = Query(..., description="MQTT_CLIENT_ID del ESP32"),
):
    """Última lectura registrada del dispositivo."""
    row = await db_pool.fetchrow(
        "SELECT * FROM sensor_readings WHERE device_id=$1 ORDER BY recorded_at DESC LIMIT 1",
        device_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Sin lecturas para este dispositivo")
    return _to_dict(row)


@app.get("/v1/readings/summary")
async def get_summary(
    device_id: str            = Query(..., description="MQTT_CLIENT_ID del ESP32"),
    period: str               = Query(default="day", pattern="^(day|week|month)$"),
    fecha: Optional[date]     = Query(default=None, description="YYYY-MM-DD (default: hoy)"),
):
    """
    Promedios agrupados:
    - day   → por hora
    - week  → por día
    - month → por día
    """
    ref_dt = datetime.combine(fecha or date.today(), datetime.min.time())
    trunc  = "hour" if period == "day" else "day"
    where  = _periodo_where(period, "recorded_at")

    rows = await db_pool.fetch(
        f"""
        SELECT
            date_trunc('{trunc}', recorded_at)     AS periodo,
            COUNT(*)                                AS registros,
            ROUND(AVG(temperatura)::numeric,  1)    AS temp_promedio,
            ROUND(AVG(hum_ambiente)::numeric, 1)    AS hum_amb_promedio,
            ROUND(AVG(hum_suelo_z1)::numeric, 1)   AS suelo_z1_promedio,
            ROUND(AVG(hum_suelo_z2)::numeric, 1)   AS suelo_z2_promedio,
            ROUND(AVG(co2_ppm)::numeric,      1)    AS co2_promedio,
            ROUND(AVG(nh3_ppm)::numeric,      2)    AS nh3_promedio
        FROM sensor_readings
        WHERE device_id = $1 AND {where}
        GROUP BY 1
        ORDER BY 1
        """,
        device_id, ref_dt,
    )
    return [_to_dict(r) for r in rows]


@app.get("/v1/alerts")
async def get_alerts(
    device_id: str            = Query(..., description="MQTT_CLIENT_ID del ESP32"),
    period: str               = Query(default="day", pattern="^(day|week|month)$"),
    fecha: Optional[date]     = Query(default=None, description="YYYY-MM-DD (default: hoy)"),
    limit: int                = Query(default=50, le=500),
):
    """Historial de alertas de un dispositivo por período."""
    ref_dt = datetime.combine(fecha or date.today(), datetime.min.time())
    where  = _periodo_where(period, "alerted_at")

    rows = await db_pool.fetch(
        f"SELECT * FROM sensor_alerts WHERE device_id=$1 AND {where} "
        f"ORDER BY alerted_at DESC LIMIT $3",
        device_id, ref_dt, limit,
    )
    return [_to_dict(r) for r in rows]


@app.get("/v1/devices")
async def list_devices():
    """Lista todos los ESP32 que han enviado datos."""
    rows = await db_pool.fetch(
        """
        SELECT
            device_id,
            COUNT(*)                        AS total_lecturas,
            MAX(recorded_at)                AS ultima_lectura,
            MIN(recorded_at)                AS primera_lectura
        FROM sensor_readings
        GROUP BY device_id
        ORDER BY ultima_lectura DESC
        """
    )
    return [_to_dict(r) for r in rows]


@app.get("/health")
async def health():
    return {
        "status":         "ok",
        "mqtt_connected": _mqtt.is_connected(),
    }
