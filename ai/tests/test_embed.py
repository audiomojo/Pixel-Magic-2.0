import numpy as np
import pytest
from PIL import Image, ImageDraw, ImageOps

from pixelmagic_ai.embed import EMBED_DIM, FEATURE_VERSION, FakeEmbedder


def _scene(seed: int = 0) -> Image.Image:
    img = Image.new("RGB", (320, 240), (30, 60, 120))
    d = ImageDraw.Draw(img)
    d.ellipse((20, 40, 140, 160), fill=(220, 40, 40))
    d.rectangle((180, 100, 300, 220), fill=(240, 220, 60))
    d.polygon([(10, 230), (100, 120), (200, 230)], fill=(30, 160, 60))
    return img


def _other() -> Image.Image:
    rng = np.random.default_rng(5)
    return Image.fromarray(rng.integers(0, 255, (240, 320, 3), dtype=np.uint8))


def test_constants():
    assert EMBED_DIM == 1152
    assert FEATURE_VERSION == 1


def test_fake_deterministic_unit_length():
    f = FakeEmbedder()
    a, b = f.embed(_scene()), f.embed(_scene())
    assert a.dtype == np.float32 and a.shape == (8,)
    assert np.array_equal(a, b)
    assert abs(float(np.linalg.norm(a)) - 1.0) < 1e-5
    assert not np.array_equal(a, f.embed(_other()))


def test_fake_aesthetic_range_and_determinism():
    f = FakeEmbedder()
    s = f.aesthetic(_scene())
    assert isinstance(s, float) and 1.0 <= s <= 10.0
    assert s == f.aesthetic(_scene())


@pytest.fixture(scope="module")
def siglip():
    from pixelmagic_ai.embed import SiglipEmbedder

    return SiglipEmbedder()


@pytest.mark.ml
def test_siglip_similarity(siglip):
    a = siglip.embed(_scene())
    assert a.dtype == np.float32 and a.shape == (EMBED_DIM,)
    assert abs(float(np.linalg.norm(a)) - 1.0) < 1e-4
    assert float(a @ siglip.embed(_scene())) >= 0.999
    flipped = float(a @ siglip.embed(ImageOps.mirror(_scene())))
    other = float(a @ siglip.embed(_other()))
    assert flipped > 0.9
    assert other < flipped


@pytest.mark.ml
def test_siglip_aesthetic(siglip):
    s = siglip.aesthetic(_scene())
    assert isinstance(s, float) and 1.0 <= s <= 10.0
