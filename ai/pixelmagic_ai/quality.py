"""Cheap objective quality measures of a decoded photo."""

import numpy as np
from PIL import Image
from scipy.ndimage import laplace, uniform_filter

_SHARPNESS_PX = 1024


def measure(img: Image.Image) -> dict[str, float]:
    grey = img.convert("L")
    g = np.asarray(grey, dtype=np.float64) / 255.0

    small = grey
    if max(grey.size) > _SHARPNESS_PX:
        small = grey.copy()
        small.thumbnail((_SHARPNESS_PX, _SHARPNESS_PX), Image.Resampling.LANCZOS)
    sharpness = float(np.var(laplace(np.asarray(small, dtype=np.float64) / 255.0)))

    hp = g - uniform_filter(g, size=3, mode="reflect")
    noise = float(np.median(np.abs(hp - np.median(hp))))

    return {
        "sharpness": sharpness,
        "brightness": float(g.mean()),
        "highlights": float((g >= 250 / 255).mean()),
        "shadows": float((g <= 5 / 255).mean()),
        "noise": noise,
    }
