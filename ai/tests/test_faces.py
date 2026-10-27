from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from pixelmagic_ai.faces import FakeDetector, _eye_ratio, _eyes_open, _smile
from pixelmagic_ai.imageio import UndecodableError

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
            img = load_rgb(p, p.suffix.lstrip(".").lower())
        except (UndecodableError, OSError):
            continue
        faces = det.detect(img)
        if len(faces) == 1:
            f = faces[0]
            assert 0.002 < f["size"] < 1
            assert f["quality"] > 0
            assert 0 <= f["quality"] <= 1
            assert 0 <= f["eyes_open"] <= 1
            assert 0 <= f["smile"] <= 1
            return
    pytest.skip("no single-face photo found in the corpus")


def _eye(w: float, h: float):
    # a rectangle-ish eye of normalised width w and height h
    return [(0.0, 0.0), (w, 0.0), (w, h), (0.0, h)]


def test_eye_ratio_scales_by_box_pixel_axes():
    pts = _eye(0.4, 0.1)
    assert _eye_ratio(pts, 100, 100) == pytest.approx(0.25)
    assert _eye_ratio(pts, 100, 200) == pytest.approx(0.5)  # taller box, same points
    assert _eye_ratio(_eye(0.4, 0.02), 100, 100) < _eye_ratio(_eye(0.4, 0.2), 100, 100)


def test_eyes_open_normalises_and_caps():
    def f(ratio, size=0.1):
        e = _eye(1.0, ratio)
        return _eyes_open(e, e, size, 1.0, 1.0)

    assert f(0.1) == pytest.approx(0.5)
    assert f(0.2) == pytest.approx(1.0)
    assert f(0.3) == 1.0
    assert f(2.0) == 0.5  # implausible
    assert f(0.3, size=0.001) == 0.5  # tiny face
    assert _eyes_open([], [], 0.1, 1.0, 1.0) == 0.5  # no landmarks


def test_smile_sign_and_clamp():
    # bottom-left origin: larger y is higher
    up = [(0.0, 0.6), (0.5, 0.4), (1.0, 0.6)]
    down = [(0.0, 0.4), (0.5, 0.6), (1.0, 0.4)]
    assert _smile(up, 1.0, 1.0) > 0
    assert _smile(down, 1.0, 1.0) == 0.0
    assert _smile([(0.0, 0.0), (0.5, -5.0), (1.0, 0.0)], 1.0, 1.0) == 1.0
    assert _smile([], 1.0, 1.0) == 0.0
    # y scaled by the box's pixel height
    assert _smile(up, 1.0, 2.0) > _smile(up, 1.0, 1.0)
