"""Face measures: eyes open, smile, capture quality and size per detected face."""

import io
from typing import Protocol

from PIL import Image

OPEN_EYE_RATIO = 0.2  # an eye aspect ratio at or above this counts as fully open
IMPLAUSIBLE_RATIO = 1.0  # above this the landmarks are bad (tiny or non-frontal face)
TINY_FACE = 0.002  # face area / image area below this: landmarks are unreliable
NEUTRAL = 0.5
SMILE_GAIN = 4.0  # a broad smile lifts the corners about a quarter of the mouth width


class FaceDetector(Protocol):
    def detect(self, img: Image.Image) -> list[dict[str, float]]: ...


class FakeDetector:
    """Returns a fixed list of faces; for tests and platforms without Vision."""

    def __init__(self, faces: list[dict[str, float]]) -> None:
        self._faces = faces

    def detect(self, img: Image.Image) -> list[dict[str, float]]:
        return list(self._faces)


def _points(region) -> list[tuple[float, float]]:
    if region is None:
        return []
    # as_tuple is required: indexing normalizedPoints() directly bus-errors.
    return [(p[0], p[1]) for p in region.normalizedPoints().as_tuple(region.pointCount())]


def _eye_ratio(pts, scale_x: float, scale_y: float) -> float | None:
    """Eye height / width in pixels. Points are normalised to the face box, so
    each axis is scaled by the box's pixel size before taking the ratio."""
    if len(pts) < 3:
        return None
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    dx = (max(xs) - min(xs)) * scale_x
    if dx <= 0:
        return None
    return (max(ys) - min(ys)) * scale_y / dx


def _smile(pts, scale_x: float, scale_y: float) -> float:
    """Smile 0..1 from how far the mouth corners sit above the mouth centre.

    The corners are the leftmost and rightmost mouth points, the centre is the
    mean height of all mouth points. smile = clamp(SMILE_GAIN * (mean corner y -
    mean y) / mouth width, 0, 1), in pixel units (Vision's y axis points up, so
    corners above the centre give a positive value). No landmarks gives 0.0.
    """
    if len(pts) < 3:
        return 0.0
    left = min(pts, key=lambda p: p[0])
    right = max(pts, key=lambda p: p[0])
    width = (right[0] - left[0]) * scale_x
    if width <= 0:
        return 0.0
    centre_y = sum(p[1] for p in pts) / len(pts)
    lift = ((left[1] + right[1]) / 2 - centre_y) * scale_y
    return max(0.0, min(1.0, SMILE_GAIN * lift / width))


def _eyes_open(left, right, size: float, scale_x: float, scale_y: float) -> float:
    ratios = [r for r in (_eye_ratio(left, scale_x, scale_y), _eye_ratio(right, scale_x, scale_y)) if r is not None]
    if size < TINY_FACE or not ratios:
        return NEUTRAL
    ratio = sum(ratios) / len(ratios)
    if ratio > IMPLAUSIBLE_RATIO:
        return NEUTRAL
    return min(1.0, ratio / OPEN_EYE_RATIO)


def _iou(a, b) -> float:
    ax2, ay2 = a.origin.x + a.size.width, a.origin.y + a.size.height
    bx2, by2 = b.origin.x + b.size.width, b.origin.y + b.size.height
    iw = min(ax2, bx2) - max(a.origin.x, b.origin.x)
    ih = min(ay2, by2) - max(a.origin.y, b.origin.y)
    if iw <= 0 or ih <= 0:
        return 0.0
    inter = iw * ih
    return inter / (a.size.width * a.size.height + b.size.width * b.size.height - inter)


def _quality_for(bb, qualities) -> float:
    """Capture quality of the result whose box best overlaps bb; 0.0 if none."""
    best, best_iou = 0.0, 0.5
    for box, q in qualities:
        iou = _iou(bb, box)
        if iou > best_iou:
            best, best_iou = q, iou
    return best


class AppleVisionDetector:
    """Face landmarks and capture quality from the macOS Vision framework."""

    def __init__(self) -> None:
        import Vision  # lazy: unavailable off macOS

        self._vision = Vision

    def detect(self, img: Image.Image) -> list[dict[str, float]]:
        from Foundation import NSData

        V = self._vision
        w, h = img.size
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=92)
        data = NSData.dataWithBytes_length_(buf.getvalue(), len(buf.getvalue()))
        # Options must be None; an empty dict crashes the handler.
        handler = V.VNImageRequestHandler.alloc().initWithData_options_(data, None)
        landmarks = V.VNDetectFaceLandmarksRequest.alloc().init()
        quality = V.VNDetectFaceCaptureQualityRequest.alloc().init()
        ok, _err = handler.performRequests_error_([landmarks, quality], None)
        if not ok:
            return []

        qualities = []
        for r in quality.results() or []:
            q = r.faceCaptureQuality()
            if q is not None:
                qualities.append((r.boundingBox(), float(q)))

        faces = []
        for r in landmarks.results() or []:
            bb = r.boundingBox()
            size = float(bb.size.width * bb.size.height)
            sx, sy = bb.size.width * w, bb.size.height * h
            lm = r.landmarks()
            if lm is None:
                continue
            mouth = _points(lm.outerLips()) or _points(lm.innerLips())
            faces.append(
                {
                    "eyes_open": _eyes_open(_points(lm.leftEye()), _points(lm.rightEye()), size, sx, sy),
                    "smile": _smile(mouth, sx, sy),
                    "quality": _quality_for(bb, qualities),
                    "size": size,
                }
            )
        return faces
