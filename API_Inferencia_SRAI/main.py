import io
import numpy as np
from PIL import Image
import os

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

import tensorflow as tf

CLASES = [c.strip() for c in os.getenv("CLASES","").split(",")]

app = FastAPI(title="SRAI Inference API")

try:
    MODEL_PATH = os.getenv("RUTA")
    model = tf.keras.models.load_model(MODEL_PATH)
except Exception as e:
    raise RuntimeError(f"No se pudo cargar el modelo: {e}")


def preprocesar(imagen_bytes: bytes) -> np.ndarray:
    imagen = Image.open(io.BytesIO(imagen_bytes)).convert("RGB")
    imagen = imagen.resize((256, 256))
    array = np.array(imagen, dtype=np.float32) / 255.0
    return np.expand_dims(array, axis=0)


@app.post("/v1/predict")
async def predict(image: UploadFile = File(...)):
    contenido = await image.read()

    try:
        entrada = preprocesar(contenido)
    except Exception:
        raise HTTPException(status_code=422, detail="No se pudo procesar la imagen.")

    prediccion = model.predict(entrada, verbose=0)[0]

    clase_idx = int(np.argmax(prediccion))
    clase = CLASES[clase_idx]
    confianza = float(prediccion[clase_idx])

    probabilidades = {c: round(float(p), 6) for c, p in zip(CLASES, prediccion)}

    return JSONResponse(content={
        "clase": clase,
        "confianza": round(confianza, 6),
        "probabilidades": probabilidades,
    })


@app.get("/v1/health")
async def health():
    return {"status": "ok"}