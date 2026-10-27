"""Image embeddings (SigLIP 2) and aesthetic score (aesthetic-predictor-v2.5).

torch/transformers are imported lazily so this module imports without the
``ml`` extra (Linux CI installs only core dependencies).
"""

import hashlib
from typing import Protocol

import numpy as np
from PIL import Image

EMBED_DIM = 1152
FEATURE_VERSION = 1

EMBED_MODEL = "google/siglip2-so400m-patch14-384"


class Embedder(Protocol):
    def embed(self, img: Image.Image) -> np.ndarray:
        """float32, L2-normalised, length EMBED_DIM."""
        ...

    def aesthetic(self, img: Image.Image) -> float:
        """Aesthetic score in [1, 10]."""
        ...


class SiglipEmbedder:
    def __init__(self, device: str = "mps") -> None:
        import torch
        from aesthetic_predictor_v2_5 import convert_v2_5_from_siglip
        from transformers import AutoModel, AutoProcessor

        if device == "mps" and not torch.backends.mps.is_available():
            device = "cpu"
        self._torch = torch
        self._device = device
        self._dtype = torch.float16 if device == "mps" else torch.float32

        self._model = (
            AutoModel.from_pretrained(EMBED_MODEL, torch_dtype=self._dtype)
            .to(device)
            .eval()
        )
        self._processor = AutoProcessor.from_pretrained(EMBED_MODEL)

        # The aesthetic head only accepts SigLIP v1 features, so it keeps its own tower.
        aes_model, aes_processor = convert_v2_5_from_siglip()
        self._aes_model = aes_model.to(self._dtype).to(device).eval()
        self._aes_processor = aes_processor

    def embed(self, img: Image.Image) -> np.ndarray:
        torch = self._torch
        inputs = self._processor(images=img, return_tensors="pt")
        pixel_values = inputs["pixel_values"].to(self._device, self._dtype)
        with torch.inference_mode():
            feats = self._model.get_image_features(pixel_values=pixel_values)
            feats = getattr(feats, "pooler_output", feats)
            feats = torch.nn.functional.normalize(feats.float(), dim=-1)
        return feats[0].cpu().numpy().astype(np.float32)

    def aesthetic(self, img: Image.Image) -> float:
        torch = self._torch
        inputs = self._aes_processor(images=img, return_tensors="pt")
        pixel_values = inputs["pixel_values"].to(self._device, self._dtype)
        with torch.inference_mode():
            score = self._aes_model(pixel_values).logits.squeeze().float().item()
        return float(min(10.0, max(1.0, score)))


class FakeEmbedder:
    """Deterministic stand-in derived from the image's pixel bytes."""

    def __init__(self, dim: int = 8) -> None:
        self.dim = dim

    def _rng(self, img: Image.Image) -> np.random.Generator:
        digest = hashlib.sha256(img.tobytes()).digest()
        return np.random.default_rng(int.from_bytes(digest[:8], "big"))

    def embed(self, img: Image.Image) -> np.ndarray:
        v = self._rng(img).standard_normal(self.dim).astype(np.float32)
        return (v / np.linalg.norm(v)).astype(np.float32)

    def aesthetic(self, img: Image.Image) -> float:
        rng = self._rng(img)
        rng.standard_normal(self.dim)
        return float(1.0 + 9.0 * rng.random())
