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
    zona            VARCHAR(32)  NOT NULL,
    recorded_at     TIMESTAMPTZ  DEFAULT NOW(),
    -- Cada zona trae su propio DHT
    temperatura     FLOAT,
    temp_estado     VARCHAR(16),
    hum_ambiente    FLOAT,
    hum_amb_estado  VARCHAR(16),
    -- Sensor de suelo y válvula de la zona
    hum_suelo       INTEGER,
    suelo_estado    VARCHAR(16),
    valvula         BOOLEAN,
    -- Cada zona trae su propio MQ-135
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

CREATE INDEX IF NOT EXISTS idx_readings_device_zona_time
    ON sensor_readings(device_id, zona, recorded_at DESC);

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
    """
    Inserta una fila por cada zona presente en el payload. El ESP32 ahora
    puede traer N zonas (zona1..zonaN) y cada una incluye su propio set
    completo de sensores (DHT, suelo/válvula, MQ-135).
    """
    if db_pool is None:
        return

    zonas = {
        k: v for k, v in data.items()
        if k.lower().startswith("zona") and isinstance(v, dict)
    }
    if not zonas:
        logger.warning("Mensaje sin zonas reconocibles | device=%s", device_id)
        return

    try:
        async with db_pool.acquire() as conn:
            for zona_nombre, z in zonas.items():
                await conn.execute(
                    """
                    INSERT INTO sensor_readings (
                        device_id,    zona,
                        temperatura,  temp_estado,
                        hum_ambiente, hum_amb_estado,
                        hum_suelo,    suelo_estado,  valvula,
                        co2_ppm,      co2_estado,
                        co_ppm,       nh3_ppm,        alcohol_ppm,
                        humo_ppm,     tolueno_ppm,    acetona_ppm
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
                    )
                    """,
                    device_id, zona_nombre,
                    z.get("temperatura"),
                    z.get("temp_estado"),
                    z.get("hum_ambiente"),
                    z.get("hum_amb_estado"),
                    z.get("hum_suelo"),
                    z.get("suelo_estado"),
                    z.get("valvula") == "ABIERTA",
                    z.get("co2_ppm"),
                    z.get("co2_estado"),
                    z.get("co_ppm"),
                    z.get("nh3_ppm"),
                    z.get("alcohol_ppm"),
                    z.get("humo_ppm"),
                    z.get("tolueno_ppm"),
                    z.get("acetona_ppm"),
                )
        logger.info(
            "Lectura guardada | device=%s zonas=%s",
            device_id, list(zonas.keys()),
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
    zona: Optional[str]       = Query(default=None, description="Filtrar por zona, ej. zona1"),
    period: str               = Query(default="day", pattern="^(day|week|month)$"),
    fecha: Optional[date]     = Query(default=None, description="YYYY-MM-DD (default: hoy)"),
    limit: int                = Query(default=100, le=1000),
):
    """Lecturas históricas de un dispositivo (opcionalmente filtradas por zona) por período."""
    ref_dt = datetime.combine(fecha or date.today(), datetime.min.time())
    where  = _periodo_where(period, "recorded_at")

    if zona:
        rows = await db_pool.fetch(
            f"SELECT * FROM sensor_readings WHERE device_id=$1 AND {where} AND zona=$3 "
            f"ORDER BY recorded_at DESC LIMIT $4",
            device_id, ref_dt, zona, limit,
        )
    else:
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
    """Última lectura registrada de cada zona del dispositivo."""
    rows = await db_pool.fetch(
        "SELECT DISTINCT ON (zona) * FROM sensor_readings "
        "WHERE device_id=$1 ORDER BY zona, recorded_at DESC",
        device_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Sin lecturas para este dispositivo")
    return [_to_dict(r) for r in rows]


@app.get("/v1/readings/summary")
async def get_summary(
    device_id: str            = Query(..., description="MQTT_CLIENT_ID del ESP32"),
    period: str               = Query(default="day", pattern="^(day|week|month)$"),
    fecha: Optional[date]     = Query(default=None, description="YYYY-MM-DD (default: hoy)"),
):
    """
    Promedios agrupados (todas las zonas combinadas, no se distingue por zona):
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
            COUNT(DISTINCT zona)                    AS zonas,
            ROUND(AVG(temperatura)::numeric,  1)    AS temp_promedio,
            ROUND(AVG(hum_ambiente)::numeric, 1)    AS hum_amb_promedio,
            ROUND(AVG(hum_suelo)::numeric,    1)    AS suelo_promedio,
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
    """Lista todos los ESP32 que han enviado datos, con su cantidad de zonas."""
    rows = await db_pool.fetch(
        """
        SELECT
            device_id,
            COUNT(*)                        AS total_lecturas,
            COUNT(DISTINCT zona)            AS zonas,
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
