import asyncio
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
import paho.mqtt.client as mqtt
from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MQTT_BROKER    = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT      = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "srai_receiver")
PIPELINE_URL   = os.getenv("PIPELINE_URL", "http://srai_pipeline:8002/v1/process")

TOPIC_INICIO = "srai/camara/inicio"
TOPIC_CHUNK  = "srai/camara/chunk"
TOPIC_FIN    = "srai/camara/fin"

# Sesiones activas: clave = esp_timestamp (valor de millis() del ESP32)
sessions: Dict[str, Dict[str, Any]] = {}
_sessions_lock = threading.Lock()
_event_loop: Optional[asyncio.AbstractEventLoop] = None


# ---------------------------------------------------------------------------
# Handlers MQTT
# ---------------------------------------------------------------------------

def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("MQTT conectado a %s:%s", MQTT_BROKER, MQTT_PORT)
        client.subscribe([(TOPIC_INICIO, 0), (TOPIC_CHUNK, 0), (TOPIC_FIN, 0)])
    else:
        logger.error("MQTT fallo de conexion: rc=%d", rc)


def _on_disconnect(client, userdata, rc):
    logger.warning("MQTT desconectado: rc=%d", rc)


def _on_message(client, userdata, msg):
    try:
        if msg.topic == TOPIC_INICIO:
            _handle_inicio(msg)
        elif msg.topic == TOPIC_CHUNK:
            _handle_chunk(msg)
        elif msg.topic == TOPIC_FIN:
            _handle_fin(msg)
    except Exception:
        logger.exception("Error procesando mensaje MQTT topic=%s", msg.topic)


def _handle_inicio(msg):
    data = json.loads(msg.payload.decode())
    ts = data["timestamp"]

    # El ESP32 usa millis() — la fecha/hora real se registra en el servidor.
    received_at = datetime.now(timezone.utc).isoformat()

    with _sessions_lock:
        sessions[ts] = {
            "esp_timestamp": ts,       # millis() del ESP32, no es fecha real
            "received_at":  received_at,
            "total_chunks": int(data["total_chunks"]),
            "image_size":   int(data["size"]),
            "chunks":       {},
            # device_id = MQTT client_id configurado en el ESP32 (esp32cam_srai)
            "device_id":    "esp32cam_srai",
        }
    logger.info(
        "Sesion iniciada | device=esp32cam_srai ts=%s chunks=%s size=%sB",
        ts, data["total_chunks"], data["size"],
    )


def _handle_chunk(msg):
    payload = bytes(msg.payload)
    if len(payload) < 3:
        logger.warning("Chunk demasiado pequeno, ignorado")
        return

    # Primeros 2 bytes: indice del chunk (big-endian), resto: datos JPEG
    chunk_idx  = (payload[0] << 8) | payload[1]
    chunk_data = payload[2:]

    with _sessions_lock:
        if not sessions:
            logger.warning("Chunk recibido sin sesion activa, ignorado")
            return
        # El ESP32 envia una imagen a la vez; asignamos al ultimo session abierto.
        ts = list(sessions.keys())[-1]
        sessions[ts]["chunks"][chunk_idx] = chunk_data

    logger.debug("Chunk %d recibido (%dB)", chunk_idx, len(chunk_data))


def _handle_fin(msg):
    ts = msg.payload.decode().strip()

    with _sessions_lock:
        session = sessions.pop(ts, None)

    if session is None:
        logger.warning("Sesion no encontrada para ts=%s", ts)
        return

    total   = session["total_chunks"]
    chunks  = session["chunks"]
    missing = [i for i in range(total) if i not in chunks]

    if missing:
        logger.warning("Chunks faltantes: %s (se continua con los recibidos)", missing)

    try:
        image_bytes = b"".join(chunks[i] for i in range(total) if i in chunks)
    except Exception:
        logger.exception("Error ensamblando imagen ts=%s", ts)
        return

    logger.info(
        "Imagen lista | device=%s size=%dB chunks=%d/%d",
        session["device_id"], len(image_bytes), len(chunks), total,
    )

    if _event_loop is not None:
        asyncio.run_coroutine_threadsafe(
            _enviar_a_pipeline(image_bytes, session),
            _event_loop,
        )
    else:
        logger.error("Event loop no disponible, imagen descartada")


async def _enviar_a_pipeline(image_bytes: bytes, session: dict):
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                PIPELINE_URL,
                files={"image": ("capture.jpg", image_bytes, "image/jpeg")},
                data={
                    "device_id":     session["device_id"],
                    "esp_timestamp": session["esp_timestamp"],
                    "received_at":   session["received_at"],
                    "image_size":    str(session["image_size"]),
                },
            )
        if resp.status_code == 200:
            logger.info("Pipeline respondio OK: %s", resp.json().get("capture_id"))
        else:
            logger.error("Pipeline error %d: %s", resp.status_code, resp.text[:200])
    except Exception:
        logger.exception("Error comunicando con pipeline")


# ---------------------------------------------------------------------------
# Cliente MQTT
# ---------------------------------------------------------------------------

_mqtt = mqtt.Client(client_id=MQTT_CLIENT_ID)
_mqtt.on_connect    = _on_connect
_mqtt.on_disconnect = _on_disconnect
_mqtt.on_message    = _on_message


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _event_loop
    _event_loop = asyncio.get_running_loop()
    _mqtt.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    _mqtt.loop_start()
    logger.info("MQTT loop iniciado")
    yield
    _mqtt.loop_stop()
    _mqtt.disconnect()
    logger.info("MQTT desconectado")


app = FastAPI(title="SRAI Image Receiver", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status":          "ok",
        "mqtt_connected":  _mqtt.is_connected(),
        "active_sessions": len(sessions),
    }
