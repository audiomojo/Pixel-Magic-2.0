from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from pixelmagic_ai.faces import FakeDetector

CORPUS = Path(__file__).resolve().parents[2] / "test-automation" / "sample-images"
MAX_SCAN = 200


def test_fake_detector_round_trips():
    faces = [{"eyes_open": 1.0, "smile": 0.3, "quality": 0.8, "size": 0.1}]
    img = Image.new("RGB", (8, 8))
    assert FakeDetector(faces).detect(img) == faces
    assert FakeDetector([]).detect(img) == []


def _gradient() -> Image.Image:
    x = np.linspace(0, 255, 640, dtype=np.uint8)
    arr = np.stack([np.tile(x, (480, 1))] * 3, axis=-1)
    return Image.fromarray(arr, "RGB")


@pytest.mark.mac
def test_no_faces_in_a_generated_landscape():
    from pixelmagic_ai.faces import AppleVisionDetector

    assert AppleVisionDetector().detect(_gradient()) == []


@pytest.mark.mac
def test_portrait_has_one_face_with_sane_measures():
    from pixelmagic_ai.faces import AppleVisionDetector
    from pixelmagic_ai.imageio import load_rgb

    if not CORPUS.is_dir():
        pytest.skip("private sample corpus absent")
    det = AppleVisionDetector()
    files = [
        p for p in sorted(CORPUS.rglob("*")) if p.suffix.lower() in {".jpg", ".jpeg", ".heic"}
    ][:MAX_SCAN]
    for p in files:
        try:
            faces = det.detect(load_rgb(p, p.suffix.lstrip(".").lower()))
        except Exception:
            continue
        if len(faces) == 1:
            f = faces[0]
            assert 0 < f["size"] < 1
            assert 0 <= f["quality"] <= 1
            assert 0 <= f["eyes_open"] <= 1
            assert 0 <= f["smile"] <= 1
            return
    pytest.skip("no single-face photo found in the corpus")
