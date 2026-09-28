# descarga pesos de MobileNetV2 y el mapeo de clases de ImageNet
import numpy as np
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, decode_predictions

MobileNetV2(weights="imagenet")
decode_predictions(np.zeros((1, 1000), dtype="float32"), top=1)
