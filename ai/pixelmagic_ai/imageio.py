"""Decode a photo to an EXIF-oriented, size-capped RGB image."""

import io
from pathlib import Path

import numpy as np
import rawpy
from PIL import Image, ImageOps, UnidentifiedImageError
from pillow_heif import register_heif_opener

register_heif_opener()

# Keep in sync with the keys of RAW_MIME in src/formats.ts.
RAW_EXTENSIONS = frozenset({"dng", "cr2", "cr3", "nef", "arw", "orf", "rw2", "raf"})

# I/O errors are transient for the caller (retried), never "undecodable".
_TRANSIENT = (FileNotFoundError, PermissionError, IsADirectoryError)


class UndecodableError(Exception):
    """The file can never be decoded (corrupt, unsupported, no RAW preview)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _raw_preview(path: Path) -> Image.Image:
    try:
        with rawpy.imread(str(path)) as raw:
            thumb = raw.extract_thumb()
    except rawpy.LibRawNoThumbnailError as e:
        raise UndecodableError("raw has no JPEG preview") from e
    except rawpy.LibRawUnsupportedThumbnailError as e:
        raise UndecodableError("raw has no JPEG preview") from e
    if thumb.format == rawpy.ThumbFormat.JPEG:
        img = Image.open(io.BytesIO(thumb.data))
        img.load()
        return img
    if thumb.format == rawpy.ThumbFormat.BITMAP:
        return Image.fromarray(np.asarray(thumb.data))
    raise UndecodableError("raw has no JPEG preview")


def load_rgb(path: Path, ext: str, max_px: int = 1600) -> Image.Image:
    try:
        if ext.lower().lstrip(".") in RAW_EXTENSIONS:
            img = _raw_preview(path)
        else:
            with Image.open(path) as f:  # read-only
                f.load()
                img = f.copy()
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
    except UndecodableError:
        raise
    except _TRANSIENT:
        raise
    except (OSError, ValueError, SyntaxError, UnidentifiedImageError, EOFError,
            rawpy.LibRawError) as e:
        raise UndecodableError(f"{type(e).__name__}: {e}") from e
    if max(img.size) > max_px:
        img.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
    return img
