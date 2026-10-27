import io
from pathlib import Path

import numpy as np
import pytest
import rawpy
from PIL import Image, ImageFilter

from pixelmagic_ai import imageio
from pixelmagic_ai.imageio import UndecodableError, load_rgb
from pixelmagic_ai.quality import measure

HEIC = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "scene0.heic"


def checkerboard(size: int = 256, cell: int = 8) -> Image.Image:
    y, x = np.indices((size, size))
    a = (((x // cell) + (y // cell)) % 2 * 255).astype(np.uint8)
    return Image.fromarray(a).convert("RGB")


def jpeg_bytes(img: Image.Image, **kw) -> bytes:
    buf = io.BytesIO()
    img.save(buf, "JPEG", **kw)
    return buf.getvalue()


def test_blur_lowers_sharpness():
    sharp = checkerboard()
    blurred = sharp.filter(ImageFilter.GaussianBlur(3))
    assert measure(blurred)["sharpness"] < measure(sharp)["sharpness"]


def test_measure_keys():
    assert set(measure(checkerboard())) == {
        "sharpness", "brightness", "highlights", "shadows", "noise",
    }


def test_white_image_highlights():
    m = measure(Image.new("RGB", (64, 64), (255, 255, 255)))
    assert m["highlights"] == pytest.approx(1.0)
    assert m["brightness"] == pytest.approx(1.0)
    assert m["shadows"] == 0.0


def test_black_image_shadows():
    m = measure(Image.new("RGB", (64, 64), (0, 0, 0)))
    assert m["shadows"] == pytest.approx(1.0)
    assert m["brightness"] == 0.0


def test_noise_higher_for_noisy_image():
    rng = np.random.default_rng(0)
    flat = Image.new("RGB", (128, 128), (128, 128, 128))
    noisy = Image.fromarray(rng.integers(100, 156, (128, 128, 3), dtype=np.uint8))
    assert measure(noisy)["noise"] > measure(flat)["noise"] == 0.0


def test_heic_loads_oriented():
    img = load_rgb(HEIC, "heic")
    assert img.mode == "RGB"
    with Image.open(HEIC) as raw:
        from PIL import ImageOps
        expect = ImageOps.exif_transpose(raw).size
    scale = min(1.0, 1600 / max(expect))
    assert max(img.size) <= 1600
    assert img.size == (round(expect[0] * scale), round(expect[1] * scale))


def test_exif_rotated_jpeg_upright(tmp_path):
    src = Image.new("RGB", (200, 100), (10, 20, 30))
    exif = Image.Exif()
    exif[0x0112] = 6
    p = tmp_path / "r.jpg"
    src.save(p, exif=exif)
    assert load_rgb(p, "jpg").size == (100, 200)


def test_downscaled_to_max_px(tmp_path):
    p = tmp_path / "big.png"
    Image.new("RGB", (3200, 800), (1, 2, 3)).save(p)
    img = load_rgb(p, "png")
    assert img.size == (1600, 400)
    assert load_rgb(p, "png", max_px=400).size == (400, 100)


def test_small_image_not_upscaled(tmp_path):
    p = tmp_path / "s.png"
    Image.new("RGBA", (50, 40)).save(p)
    img = load_rgb(p, "png")
    assert img.size == (50, 40) and img.mode == "RGB"


def test_truncated_jpeg_raises(tmp_path):
    rng = np.random.default_rng(1)
    data = jpeg_bytes(Image.fromarray(rng.integers(0, 255, (300, 300, 3), dtype=np.uint8)))
    p = tmp_path / "t.jpg"
    p.write_bytes(data[: len(data) // 2])
    with pytest.raises(UndecodableError) as e:
        load_rgb(p, "jpg")
    assert e.value.reason


def test_garbage_raises(tmp_path):
    p = tmp_path / "g.bmp"
    p.write_bytes(b"not an image at all")
    with pytest.raises(UndecodableError):
        load_rgb(p, "bmp")


def test_missing_file_propagates(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_rgb(tmp_path / "nope.jpg", "jpg")


def test_raw_extensions_match_formats_ts():
    ts = (Path(__file__).resolve().parents[2] / "src" / "formats.ts").read_text()
    for ext in imageio.RAW_EXTENSIONS:
        assert ext in ts


class FakeRaw:
    def __init__(self, thumb):
        self._thumb = thumb

    def extract_thumb(self):
        return self._thumb

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_raw_uses_jpeg_preview(tmp_path, monkeypatch):
    p = tmp_path / "x.dng"
    p.write_bytes(b"raw")
    thumb = rawpy.Thumbnail(rawpy.ThumbFormat.JPEG, jpeg_bytes(Image.new("RGB", (120, 80))))
    monkeypatch.setattr(rawpy, "imread", lambda path: FakeRaw(thumb))
    assert load_rgb(p, "dng").size == (120, 80)


def test_raw_bitmap_preview(tmp_path, monkeypatch):
    p = tmp_path / "x.cr2"
    p.write_bytes(b"raw")
    arr = np.zeros((60, 90, 3), dtype=np.uint8)
    thumb = rawpy.Thumbnail(rawpy.ThumbFormat.BITMAP, arr)
    monkeypatch.setattr(rawpy, "imread", lambda path: FakeRaw(thumb))
    assert load_rgb(p, "cr2").size == (90, 60)


def test_raw_without_preview_raises(tmp_path, monkeypatch):
    p = tmp_path / "x.nef"
    p.write_bytes(b"raw")

    def no_thumb():
        raise rawpy.LibRawNoThumbnailError("none")

    fake = FakeRaw(None)
    fake.extract_thumb = no_thumb
    monkeypatch.setattr(rawpy, "imread", lambda path: fake)
    with pytest.raises(UndecodableError):
        load_rgb(p, "nef")
