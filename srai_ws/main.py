"""
SRAI WebSocket Service — Gateway en tiempo real para la app Android.

Puentes:
  MQTT sensor readings → WebSocket /ws/sensors/*
  Pipeline webhook     → WebSocket /ws/diseases

WebSocket:
  /ws/sensors/overview        — Promedio de todos los sensores de todas las zonas
  /ws/sensors/zones           — Datos de todas las zonas agrupadas
  /ws/sensors/zone/{zona}     — Datos de una zona específica
  /ws/diseases                — Alertas de enfermedades en tiempo real

REST:
  GET  /api/diseases/history            — Historial de capturas por día
  GET  /api/diseases/{capture_id}       — Detalle de enfermedad con info clínica
  GET  /api/sensors/zones/latest        — Última lectura por zona
  GET  /api/sensors/averages            — Promedios históricos (day/week/month)
  GET  /api/sensors/zone/{zona}/history — Lecturas individuales de una zona por día

Notificaciones push:
  El Android implementa un ForegroundService que mantiene la conexión a /ws/diseases
  y muestra notificaciones locales via NotificationManager cuando llega un disease_alert.
  No requiere internet ni Firebase.
"""

import asyncio
import json
import logging
import os
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Optional, Set

import asyncpg
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
MQTT_BROKER       = os.getenv("MQTT_BROKER",       "mosquitto")
MQTT_PORT         = int(os.getenv("MQTT_PORT",     "1883"))
DATABASE_URL      = os.getenv("DATABASE_URL",      "postgresql://srai:srai_pass@postgres:5432/srai_db")
MINIO_IMAGE_PROXY = os.getenv("MINIO_IMAGE_PROXY", "http://srai_dashboard:8080/api/image")
CLASES_ENFERMEDAD = set(os.getenv("CLASES_ENFERMEDAD", "tizon_temprano,moho_foliar,TYLCV").split(","))

# ─── Estado global ────────────────────────────────────────────────────────────
db_pool: asyncpg.Pool = None
_mqtt_queue: asyncio.Queue = None

# ─── Información clínica de enfermedades ──────────────────────────────────────
ENFERMEDAD_INFO = {
    "tizon_temprano": {
        "nombre":      "Tizón Temprano",
        "descripcion": "Enfermedad fúngica causada por Alternaria solani. Aparece como manchas oscuras necróticas en hojas y tallos.",
        "tratamiento": "Aplicar fungicidas con mancozeb o clorotalonil. Retirar y destruir hojas afectadas.",
    },
    "moho_foliar": {
        "nombre":      "Moho Foliar",
        "descripcion": "Causado por Fulvia fulva. Manchas amarillas en el haz y moho oliváceo en el envés de las hojas.",
        "tratamiento": "Mejorar ventilación del invernadero, reducir humedad relativa y aplicar fungicidas sistémicos.",
    },
    "TYLCV": {
        "nombre":      "Virus del Enrollamiento de la Hoja (TYLCV)",
        "descripcion": "Virus transmitido por mosca blanca (Bemisia tabaci). Provoca enrollamiento y amarillamiento de hojas jóvenes.",
        "tratamiento": "Controlar población de mosca blanca, eliminar plantas infectadas y usar variedades resistentes.",
    },
    "sanas": {
        "nombre":      "Planta Sana",
        "descripcion": "No se detectaron síntomas de enfermedad en la muestra.",
        "tratamiento": None,
    },
}


# ─── WebSocket Connection Manager ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._channels: Dict[str, Set[WebSocket]] = {}

    async def connect(self, ws: WebSocket, channel: str):
        await ws.accept()
        self._channels.setdefault(channel, set()).add(ws)
        logger.info("WS conectado  canal=%s  activos=%d", channel, len(self._channels[channel]))

    def disconnect(self, ws: WebSocket, channel: str):
        self._channels.get(channel, set()).discard(ws)

    async def broadcast(self, channel: str, message: dict):
        dead: Set[WebSocket] = set()
        for ws in list(self._channels.get(channel, set())):
            try:
                await ws.send_json(message)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._channels.get(channel, set()).discard(ws)


manager = ConnectionManager()


# ─── MQTT Bridge ──────────────────────────────────────────────────────────────
def _compute_zone_averages(zones_data: dict) -> dict:
    sums: dict = {}
    counts: dict = {}
    for data in zones_data.values():
        for k, v in data.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool) and not k.endswith("_estado"):
                sums[k] = sums.get(k, 0.0) + float(v)
                counts[k] = counts.get(k, 0) + 1
    return {k: round(sums[k] / counts[k], 2) for k in sums}


def _mqtt_on_message(client, userdata, msg):
    loop: asyncio.AbstractEventLoop = userdata["loop"]
    queue: asyncio.Queue = userdata["queue"]
    try:
        asyncio.run_coroutine_threadsafe(
            queue.put((msg.topic, msg.payload.decode("utf-8", errors="replace"))),
            loop,
        )
    except Exception as exc:
        logger.error("Error encolando mensaje MQTT: %s", exc)


def _run_mqtt(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    client = mqtt.Client(client_id="srai_ws_bridge", userdata={"loop": loop, "queue": queue})
    client.on_message = _mqtt_on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.subscribe("invernadero/jitomate/+/estado")
        logger.info("MQTT conectado a %s:%d", MQTT_BROKER, MQTT_PORT)
        client.loop_forever()
    except Exception as exc:
        logger.error("Error MQTT: %s", exc)


async def _process_mqtt_queue():
    while True:
        try:
            topic, payload = await asyncio.wait_for(_mqtt_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break

        try:
            if "/estado" not in topic:
                continue
            parts = topic.split("/")
            device_id = parts[2] if len(parts) > 2 else "unknown"
            zones_data: dict = json.loads(payload)
            ts = datetime.now(timezone.utc).isoformat()

            # Canal por zona individual
            for zona, data in zones_data.items():
                await manager.broadcast(f"sensors/zone/{zona}", {
                    "type":      "sensor_zone",
                    "device_id": device_id,
                    "zona":      zona,
                    "data":      data,
                    "timestamp": ts,
                })

            # Canal todas las zonas agrupadas
            await manager.broadcast("sensors/zones", {
                "type":      "zones_data",
                "device_id": device_id,
                "zones":     zones_data,
                "timestamp": ts,
            })

            # Canal resumen promedio entre zonas
            await manager.broadcast("sensors/overview", {
                "type":      "sensor_overview",
                "device_id": device_id,
                "data":      _compute_zone_averages(zones_data),
                "zones":     list(zones_data.keys()),
                "timestamp": ts,
            })

        except Exception as exc:
            logger.error("Error procesando mensaje MQTT: %s", exc)


# ─── DB Migrations ────────────────────────────────────────────────────────────
async def _run_migrations():
    async with db_pool.acquire() as conn:
        # Agrega zona a captures (para asociar cámara con zona del invernadero)
        await conn.execute("""
            DO $$ BEGIN
                ALTER TABLE captures ADD COLUMN zona VARCHAR(32);
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$;
        """)
    logger.info("Migraciones de srai_ws completadas")


# ─── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, _mqtt_queue

    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await _run_migrations()

    loop = asyncio.get_running_loop()
    _mqtt_queue = asyncio.Queue(maxsize=2000)

    threading.Thread(
        target=_run_mqtt,
        args=(loop, _mqtt_queue),
        daemon=True,
        name="mqtt-bridge",
    ).start()

    processor = asyncio.create_task(_process_mqtt_queue(), name="mqtt-processor")

    yield

    processor.cancel()
    await db_pool.close()


# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="SRAI WebSocket Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# ─── WebSocket Endpoints ──────────────────────────────────────────────────────

@app.websocket("/ws/sensors/overview")
async def ws_sensors_overview(ws: WebSocket):
    """Promedio de todos los sensores de todas las zonas en tiempo real (pantalla 1 y 4)."""
    await manager.connect(ws, "sensors/overview")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, "sensors/overview")


@app.websocket("/ws/sensors/zones")
async def ws_sensors_zones(ws: WebSocket):
    """Datos de todas las zonas agrupadas en tiempo real (pantalla 3)."""
    await manager.connect(ws, "sensors/zones")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, "sensors/zones")


@app.websocket("/ws/sensors/zone/{zona}")
async def ws_sensor_zone(ws: WebSocket, zona: str):
    """Datos de una zona específica en tiempo real (pantalla 5)."""
    channel = f"sensors/zone/{zona}"
    await manager.connect(ws, channel)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, channel)


@app.websocket("/ws/diseases")
async def ws_diseases(ws: WebSocket):
    """Alertas de enfermedades detectadas en tiempo real (pantalla 2)."""
    await manager.connect(ws, "diseases/live")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, "diseases/live")


# ─── Internal Webhook (recibe notificaciones desde srai_pipeline) ─────────────
class DiseaseEvent(BaseModel):
    capture_id:  str
    device_id:   str
    received_at: str
    processed_at: str
    minio_path:  str
    inference:   dict
    zona:        Optional[str] = None
    webhook_sent: bool = False


@app.post("/internal/disease-event", include_in_schema=False)
async def receive_disease_event(event: DiseaseEvent):
    clase     = event.inference.get("clase", "sanas")
    confianza = float(event.inference.get("confianza", 0.0))
    image_url = f"{MINIO_IMAGE_PROXY}/{event.minio_path}"

    # Zona: primero del evento, luego de la BD, luego device_id como fallback
    zona = event.zona
    if not zona:
        try:
            cid = uuid.UUID(event.capture_id)
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT zona FROM captures WHERE capture_id = $1", cid
                )
            zona = row["zona"] if row and row["zona"] else None
        except Exception:
            zona = None
    zona = zona or event.device_id

    info = ENFERMEDAD_INFO.get(clase, {"nombre": clase, "descripcion": None, "tratamiento": None})

    msg = {
        "type":          "disease_alert",
        "capture_id":    event.capture_id,
        "device_id":     event.device_id,
        "zona":          zona,
        "clase":         clase,
        "nombre":        info["nombre"],
        "confianza":     confianza,
        "probabilidades": event.inference.get("probabilidades", {}),
        "image_url":     image_url,
        "detected_at":   event.processed_at,
        "es_enfermedad": clase in CLASES_ENFERMEDAD,
    }

    await manager.broadcast("diseases/live", msg)

    return {"ok": True}


# ─── REST: Enfermedades ───────────────────────────────────────────────────────

@app.get("/api/diseases/history")
async def get_disease_history(
    date_str: Optional[str] = Query(None, alias="date"),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Historial de capturas del día indicado ordenadas por hora.
    Incluye: hora, zona, clase de enfermedad, confianza, URL de imagen.
    """
    target = date.fromisoformat(date_str) if date_str else date.today()
    start  = datetime.combine(target, datetime.min.time())
    end    = start + timedelta(days=1)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT capture_id, device_id, zona, minio_path,
                   clase, confianza, probabilidades, processed_at
            FROM captures
            WHERE processed_at >= $1 AND processed_at < $2
              AND clase IS NOT NULL
            ORDER BY processed_at DESC
            LIMIT $3
            """,
            start, end, limit,
        )

    return {
        "date":    target.isoformat(),
        "total":   len(rows),
        "capturas": [
            {
                "capture_id":    str(r["capture_id"]),
                "device_id":     r["device_id"],
                "zona":          r["zona"] or r["device_id"],
                "hora":          r["processed_at"].strftime("%H:%M:%S") if r["processed_at"] else None,
                "clase":         r["clase"],
                "nombre":        ENFERMEDAD_INFO.get(r["clase"] or "", {}).get("nombre", r["clase"]),
                "confianza":     r["confianza"],
                "probabilidades": r["probabilidades"],
                "image_url":     f"{MINIO_IMAGE_PROXY}/{r['minio_path']}",
                "es_enfermedad": r["clase"] in CLASES_ENFERMEDAD,
            }
            for r in rows
        ],
    }


@app.get("/api/diseases/{capture_id}")
async def get_disease_detail(capture_id: str):
    """
    Detalle completo de una captura con información clínica de la enfermedad detectada.
    """
    try:
        cid = uuid.UUID(capture_id)
    except ValueError:
        raise HTTPException(400, "capture_id inválido")

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT capture_id, device_id, zona, minio_path, clase, confianza,
                   probabilidades, received_at, processed_at, image_size
            FROM captures WHERE capture_id = $1
            """,
            cid,
        )

    if not row:
        raise HTTPException(404, "Captura no encontrada")

    clase = row["clase"] or "sanas"
    info  = ENFERMEDAD_INFO.get(clase, {"nombre": clase, "descripcion": None, "tratamiento": None})

    return {
        "capture_id":    str(row["capture_id"]),
        "device_id":     row["device_id"],
        "zona":          row["zona"] or row["device_id"],
        "hora":          row["processed_at"].strftime("%H:%M:%S") if row["processed_at"] else None,
        "fecha":         row["processed_at"].date().isoformat() if row["processed_at"] else None,
        "clase":         clase,
        "es_enfermedad": clase in CLASES_ENFERMEDAD,
        "confianza":     row["confianza"],
        "probabilidades": row["probabilidades"],
        "image_url":     f"{MINIO_IMAGE_PROXY}/{row['minio_path']}",
        "image_size":    row["image_size"],
        "recibido_at":   row["received_at"].isoformat() if row["received_at"] else None,
        "procesado_at":  row["processed_at"].isoformat() if row["processed_at"] else None,
        **info,
    }


# ─── REST: Sensores ───────────────────────────────────────────────────────────

@app.get("/api/sensors/zones/latest")
async def get_zones_latest():
    """
    Última lectura de cada zona. Devuelve un dict con zona como clave.
    Útil para mostrar el estado actual de todas las zonas (pantalla 3).
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (zona)
                device_id, zona, recorded_at,
                temperatura, temp_estado,
                hum_ambiente, hum_amb_estado,
                hum_suelo, suelo_estado, valvula,
                co2_ppm, co2_estado, co_ppm, nh3_ppm,
                alcohol_ppm, humo_ppm, tolueno_ppm, acetona_ppm
            FROM sensor_readings
            ORDER BY zona, recorded_at DESC
            """
        )

    def to_dict(r):
        return {
            "device_id":      r["device_id"],
            "zona":           r["zona"],
            "recorded_at":    r["recorded_at"].isoformat() if r["recorded_at"] else None,
            "temperatura":    r["temperatura"],
            "temp_estado":    r["temp_estado"],
            "hum_ambiente":   r["hum_ambiente"],
            "hum_amb_estado": r["hum_amb_estado"],
            "hum_suelo":      r["hum_suelo"],
            "suelo_estado":   r["suelo_estado"],
            "valvula":        r["valvula"],
            "co2_ppm":        r["co2_ppm"],
            "co2_estado":     r["co2_estado"],
            "co_ppm":         r["co_ppm"],
            "nh3_ppm":        r["nh3_ppm"],
            "alcohol_ppm":    r["alcohol_ppm"],
            "humo_ppm":       r["humo_ppm"],
            "tolueno_ppm":    r["tolueno_ppm"],
            "acetona_ppm":    r["acetona_ppm"],
        }

    zonas = {r["zona"]: to_dict(r) for r in rows}
    return {"zones": zonas, "count": len(zonas)}


@app.get("/api/sensors/averages")
async def get_sensor_averages(
    period: str = Query("day", pattern="^(day|week|month)$"),
    zona: Optional[str] = None,
    fecha: Optional[str] = None,
):
    """
    Promedios históricos de sensores agrupados por período y zona.
    - period=day   → promedios por hora del día indicado
    - period=week  → promedios por día de la semana indicada
    - period=month → promedios por día del mes indicado
    Pantalla 4: monitoreo sensores promedio con histórico.
    """
    target = date.fromisoformat(fecha) if fecha else date.today()

    if period == "day":
        start = datetime.combine(target, datetime.min.time())
        end   = start + timedelta(days=1)
        trunc = "hour"
    elif period == "week":
        start = datetime.combine(target - timedelta(days=target.weekday()), datetime.min.time())
        end   = start + timedelta(weeks=1)
        trunc = "day"
    else:  # month
        start = datetime.combine(target.replace(day=1), datetime.min.time())
        nxt   = (target.replace(day=28) + timedelta(days=4)).replace(day=1)
        end   = datetime.combine(nxt, datetime.min.time())
        trunc = "day"

    zona_clause = "AND zona = $3" if zona else ""
    params = [start, end] + ([zona] if zona else [])

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                date_trunc('{trunc}', recorded_at) AS periodo,
                zona,
                ROUND(AVG(temperatura)::numeric,  2) AS temperatura,
                ROUND(AVG(hum_ambiente)::numeric, 2) AS hum_ambiente,
                ROUND(AVG(hum_suelo)::numeric,    2) AS hum_suelo,
                ROUND(AVG(co2_ppm)::numeric,      2) AS co2_ppm,
                ROUND(AVG(co_ppm)::numeric,       4) AS co_ppm,
                ROUND(AVG(nh3_ppm)::numeric,      4) AS nh3_ppm,
                COUNT(*) AS lecturas
            FROM sensor_readings
            WHERE recorded_at >= $1 AND recorded_at < $2 {zona_clause}
            GROUP BY periodo, zona
            ORDER BY periodo ASC, zona ASC
            """,
            *params,
        )

    def safe_float(v):
        return float(v) if v is not None else None

    return {
        "period": period,
        "zona":   zona,
        "start":  start.isoformat(),
        "end":    end.isoformat(),
        "data": [
            {
                "periodo":      r["periodo"].isoformat() if r["periodo"] else None,
                "zona":         r["zona"],
                "temperatura":  safe_float(r["temperatura"]),
                "hum_ambiente": safe_float(r["hum_ambiente"]),
                "hum_suelo":    safe_float(r["hum_suelo"]),
                "co2_ppm":      safe_float(r["co2_ppm"]),
                "co_ppm":       safe_float(r["co_ppm"]),
                "nh3_ppm":      safe_float(r["nh3_ppm"]),
                "lecturas":     r["lecturas"],
            }
            for r in rows
        ],
    }


@app.get("/api/sensors/zone/{zona}/history")
async def get_zone_history(
    zona: str,
    fecha: Optional[str] = None,
    limit: int = Query(200, ge=1, le=500),
):
    """
    Lecturas individuales de una zona en el día indicado.
    Ordenadas cronológicamente para graficar el histórico del día (pantalla 5).
    """
    target = date.fromisoformat(fecha) if fecha else date.today()
    start  = datetime.combine(target, datetime.min.time())
    end    = start + timedelta(days=1)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT recorded_at, device_id,
                   temperatura, temp_estado,
                   hum_ambiente, hum_amb_estado,
                   hum_suelo, suelo_estado, valvula,
                   co2_ppm, co2_estado, co_ppm, nh3_ppm,
                   alcohol_ppm, humo_ppm, tolueno_ppm, acetona_ppm
            FROM sensor_readings
            WHERE zona = $1 AND recorded_at >= $2 AND recorded_at < $3
            ORDER BY recorded_at ASC
            LIMIT $4
            """,
            zona, start, end, limit,
        )

    return {
        "zona":     zona,
        "date":     target.isoformat(),
        "total":    len(rows),
        "lecturas": [
            {
                "hora":           r["recorded_at"].strftime("%H:%M:%S") if r["recorded_at"] else None,
                "device_id":      r["device_id"],
                "temperatura":    r["temperatura"],
                "temp_estado":    r["temp_estado"],
                "hum_ambiente":   r["hum_ambiente"],
                "hum_amb_estado": r["hum_amb_estado"],
                "hum_suelo":      r["hum_suelo"],
                "suelo_estado":   r["suelo_estado"],
                "valvula":        r["valvula"],
                "co2_ppm":        r["co2_ppm"],
                "co2_estado":     r["co2_estado"],
                "co_ppm":         r["co_ppm"],
                "nh3_ppm":        r["nh3_ppm"],
                "alcohol_ppm":    r["alcohol_ppm"],
                "humo_ppm":       r["humo_ppm"],
                "tolueno_ppm":    r["tolueno_ppm"],
                "acetona_ppm":    r["acetona_ppm"],
            }
            for r in rows
        ],
    }


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "srai_ws", "version": "1.0.0"}
