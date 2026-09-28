import io
import logging
import os

import numpy as np
from PIL import Image

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

import tensorflow as tf
from tensorflow.keras.applications.mobilenet_v2 import (
    MobileNetV2,
    preprocess_input as gate_preprocess_input,
    decode_predictions,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("srai_inference")

CLASES = [c.strip() for c in os.getenv("CLASES", "").split(",")]

# filtro planta/no-planta
GATE_ENABLED             = os.getenv("GATE_ENABLED", "true").lower() == "true"
GATE_TOPK                = int(os.getenv("GATE_TOPK", 5))
GATE_MIN_TOPK_PROB       = float(os.getenv("GATE_MIN_TOPK_PROB", 0.03))
GATE_CONFUSION_THRESHOLD = float(os.getenv("GATE_CONFUSION_THRESHOLD", 0.25))

# clases de ImageNet relacionadas a plantas/frutos/hongos
PLANT_LABELS = {
    "daisy", "yellow_lady's_slipper", "corn", "acorn", "hip", "buckeye",
    "coral_fungus", "agaric", "gyromitra", "stinkhorn", "earthstar",
    "hen-of-the-woods", "bolete", "ear", "rapeseed",
    "granny_smith", "strawberry", "orange", "lemon", "fig", "pineapple",
    "banana", "jackfruit", "custard_apple", "pomegranate",
    "head_cabbage", "broccoli", "cauliflower", "zucchini",
    "spaghetti_squash", "acorn_squash", "butternut_squash", "cucumber",
    "artichoke", "cardoon", "mushroom",
}

app = FastAPI(title="SRAI Inference API")

try:
    MODEL_PATH = os.getenv("RUTA")
    model = tf.keras.models.load_model(MODEL_PATH)
    logger.info("Modelo de enfermedades cargado desde %s", MODEL_PATH)
except Exception as e:
    raise RuntimeError(f"No se pudo cargar el modelo de enfermedades: {e}")

gate_model = None
if GATE_ENABLED:
    try:
        gate_model = MobileNetV2(weights="imagenet")
        logger.info("Filtro planta/no-planta (MobileNetV2 + ImageNet) cargado")
    except Exception as e:
        logger.error("No se pudo cargar el filtro planta/no-planta, se omitira: %s", e)
        gate_model = None


def preprocesar(imagen: Image.Image) -> np.ndarray:
    imagen = imagen.resize((256, 256))
    array = np.array(imagen, dtype=np.float32) / 255.0
    return np.expand_dims(array, axis=0)


def es_planta(imagen: Image.Image) -> dict:
    if gate_model is None:
        return {"es_planta": True, "confianza": 1.0, "prediccion": "gate_deshabilitado"}

    entrada = imagen.resize((224, 224))
    array = np.expand_dims(np.array(entrada, dtype=np.float32), axis=0)
    array = gate_preprocess_input(array)

    pred = gate_model.predict(array, verbose=0)
    top = decode_predictions(pred, top=GATE_TOPK)[0]  # [(id, label, prob), ...]

    top1_label, top1_prob = top[0][1], float(top[0][2])

    coincidencia = next(
        ((label, float(prob)) for (_, label, prob) in top
         if label.lower() in PLANT_LABELS and prob >= GATE_MIN_TOPK_PROB),
        None,
    )
    if coincidencia is not None:
        label, prob = coincidencia
        return {"es_planta": True, "confianza": prob, "prediccion": label}

    if top1_prob < GATE_CONFUSION_THRESHOLD:
        return {"es_planta": True, "confianza": top1_prob, "prediccion": f"ambiguo:{top1_label}"}

    return {"es_planta": False, "confianza": top1_prob, "prediccion": top1_label}


@app.post("/v1/predict")
async def predict(image: UploadFile = File(...)):
    contenido = await image.read()

    try:
        imagen = Image.open(io.BytesIO(contenido)).convert("RGB")
    except Exception:
        raise HTTPException(status_code=422, detail="No se pudo procesar la imagen.")

    try:
        gate = es_planta(imagen)
    except Exception:
        logger.exception("Error en el filtro planta/no-planta, se omite y continua")
        gate = {"es_planta": True, "confianza": 0.0, "prediccion": "error_gate"}

    if not gate["es_planta"]:
        probabilidades = {c: 0.0 for c in CLASES}
        return JSONResponse(content={
            "es_planta": False,
            "clase": "no_planta",
            "confianza": round(gate["confianza"], 6),
            "probabilidades": probabilidades,
            "detalle": (
                "La imagen no parece ser una planta u hoja "
                f"(el filtro identifico: {gate['prediccion']})."
            ),
        })

    entrada = preprocesar(imagen)
    prediccion = model.predict(entrada, verbose=0)[0]

    clase_idx = int(np.argmax(prediccion))
    clase = CLASES[clase_idx]
    confianza = float(prediccion[clase_idx])

    probabilidades = {c: round(float(p), 6) for c, p in zip(CLASES, prediccion)}

    return JSONResponse(content={
        "es_planta": True,
        "clase": clase,
        "confianza": round(confianza, 6),
        "probabilidades": probabilidades,
    })


@app.post("/v1/es_planta")
async def check_es_planta(image: UploadFile = File(...)):
    contenido = await image.read()
    try:
        imagen = Image.open(io.BytesIO(contenido)).convert("RGB")
        gate = es_planta(imagen)
    except Exception:
        raise HTTPException(status_code=422, detail="No se pudo procesar la imagen.")

    return JSONResponse(content={
        "es_planta": gate["es_planta"],
        "confianza": round(gate["confianza"], 6),
        "prediccion": gate["prediccion"],
    })


@app.get("/v1/health")
async def health():
    return {"status": "ok", "gate_habilitado": gate_model is not None}
