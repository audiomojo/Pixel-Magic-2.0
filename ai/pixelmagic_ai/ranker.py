"""The taste model: per-set features, the hand-written rule score, and a conditional-logit
choice model fitted to the user's picks. Pure numpy/scipy — no database, no Temporal."""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from scipy.optimize import minimize
from scipy.special import logsumexp

from .db import Described, Pick
from .embed import EMBED_DIM, FEATURE_VERSION

PCA_DIM = 32
FEATURE_NAMES = [
    "rule",
    "sharpness_rel",
    "brightness_dev",
    "clipped",
    "noise_rel",
    "faces_max",
    "eyes_open_all",
    "eyes_open_min",
    "smile_mean",
    "face_quality_mean",
    "face_size_max",
    "aesthetic_rel",
] + [f"emb_{k}" for k in range(PCA_DIM)]
N_FEATURES = len(FEATURE_NAMES)
MIN_PICKS = 20
LAMS = (0.01, 0.1, 1.0, 10.0)
_IDX = {name: i for i, name in enumerate(FEATURE_NAMES)}


@dataclass
class Model:
    weights: np.ndarray
    lam: float
    pca: np.ndarray
    feature_version: int


def RULE_ONLY(pca: np.ndarray) -> Model:
    w = np.zeros(N_FEATURES)
    w[_IDX["rule"]] = 1.0
    return Model(w, 0.0, pca, FEATURE_VERSION)


def _is_rule_only(model: Model) -> bool:
    return np.array_equal(model.weights, RULE_ONLY(model.pca).weights)


def _z(x: np.ndarray) -> np.ndarray:
    sd = x.std()
    return np.zeros_like(x) if sd == 0 else (x - x.mean()) / sd


def _faces(faces: list[dict]) -> list[float]:
    """eyes_open_all, eyes_open_min, smile_mean, face_quality_mean, face_size_max."""
    if not faces:
        return [0.0] * 5
    eyes = np.array([f["eyes_open"] for f in faces], dtype=float)
    return [
        float((eyes >= 0.5).mean()),
        float(eyes.min()),
        float(np.mean([f["smile"] for f in faces])),
        float(np.mean([f["quality"] for f in faces])),
        float(max(f["size"] for f in faces)),
    ]


def set_matrix(cands: list[Described], pca: np.ndarray) -> np.ndarray:
    """One row per candidate, features relative to the rest of the set."""
    n = len(cands)
    q = {k: np.array([c.quality[k] for c in cands], dtype=float)
         for k in ("sharpness", "brightness", "highlights", "shadows", "noise", "aesthetic")}
    x = np.zeros((n, N_FEATURES))
    sharp = np.maximum(q["sharpness"], 1e-9)
    x[:, _IDX["sharpness_rel"]] = np.log(sharp / max(sharp.max(), 1e-9))
    x[:, _IDX["brightness_dev"]] = np.abs(q["brightness"] - 0.5)
    x[:, _IDX["clipped"]] = q["highlights"] + q["shadows"]
    x[:, _IDX["noise_rel"]] = q["noise"] - q["noise"].mean()
    counts = np.array([len(c.faces) for c in cands])
    x[:, _IDX["faces_max"]] = (counts == counts.max()).astype(float)
    start = _IDX["eyes_open_all"]
    x[:, start : start + 5] = [_faces(c.faces) for c in cands]
    x[:, _IDX["aesthetic_rel"]] = q["aesthetic"] - q["aesthetic"].mean()
    emb = np.stack([c.embedding for c in cands]).astype(float)
    x[:, _IDX["emb_0"] :] = (emb - emb.mean(axis=0)) @ pca
    x[:, _IDX["rule"]] = (
        _z(x[:, _IDX["face_quality_mean"]])
        + x[:, _IDX["eyes_open_all"]]
        + 0.5 * _z(x[:, _IDX["sharpness_rel"]])
        + 0.5 * _z(x[:, _IDX["aesthetic_rel"]])
        - x[:, _IDX["clipped"]]
    )
    return x


def fit_pca(sets: dict[int, list[Described]]) -> np.ndarray:
    """Principal directions of set-centred embeddings, (dim × 32), zero beyond the rank."""
    rows = [np.stack([c.embedding for c in cands]).astype(float) for cands in sets.values() if cands]
    if not rows:
        return np.zeros((EMBED_DIM, PCA_DIM))
    centred = np.concatenate([r - r.mean(axis=0) for r in rows])
    dim = centred.shape[1]
    pca = np.zeros((dim, PCA_DIM))
    k = max(0, min(PCA_DIM, dim, centred.shape[0] - 1))
    if k:
        _, _, vt = np.linalg.svd(centred, full_matrices=False)
        pca[:, :k] = vt[:k].T
    return pca


def _rows(sets: dict[int, list[Described]], pca: np.ndarray) -> dict[str, np.ndarray]:
    """sha256 → its feature row, computed within its own set."""
    out: dict[str, np.ndarray] = {}
    for cands in sets.values():
        if cands:
            for c, row in zip(cands, set_matrix(cands, pca)):
                out.setdefault(c.sha256, row)
    return out


def _usable(rows: dict[str, np.ndarray], picks: list[Pick]) -> list[tuple[np.ndarray, int]]:
    """(shown-candidate matrix, chosen index) for each pick still described enough to use."""
    out = []
    for p in picks:
        shown = [s for s in dict.fromkeys(p.shown) if s in rows]
        if p.chosen in shown and len(shown) >= 2:
            out.append((np.stack([rows[s] for s in shown]), shown.index(p.chosen)))
    return out


def _fit_prepared(prepared: list[tuple[np.ndarray, int]], pca: np.ndarray, lam: float) -> Model:
    if len(prepared) < MIN_PICKS:
        return RULE_ONLY(pca)

    def objective(w: np.ndarray) -> tuple[float, np.ndarray]:
        loss = lam * (w @ w)
        grad = 2 * lam * w
        for x, c in prepared:
            u = x @ w
            lse = logsumexp(u)
            loss -= u[c] - lse
            grad -= x[c] - np.exp(u - lse) @ x
        return float(loss), grad

    res = minimize(objective, np.zeros(N_FEATURES), jac=True, method="L-BFGS-B")
    return Model(res.x, float(lam), pca, FEATURE_VERSION)


def fit(sets: dict[int, list[Described]], picks: list[Pick], pca: np.ndarray, lam: float) -> Model:
    """Maximise Σ log softmax(X w)[chosen] − lam·‖w‖²; fewer than 20 usable picks → RULE_ONLY."""
    return _fit_prepared(_usable(_rows(sets, pca), picks), pca, lam)


def _agrees(model: Model, x: np.ndarray, c: int) -> bool:
    s = x @ model.weights
    return bool(np.all(s[c] > np.delete(s, c)))


def _agreement(model: Model, prepared: list[tuple[np.ndarray, int]]) -> float:
    if not prepared:
        return 0.0
    return sum(_agrees(model, x, c) for x, c in prepared) / len(prepared)


def cv_agreement(
    sets: dict[int, list[Described]],
    picks: list[Pick],
    pca: np.ndarray,
    lam: float,
    folds: int = 5,
    seed: int = 7,
) -> float:
    """Share of held-out picks whose chosen candidate scores strictly highest."""
    prepared = _usable(_rows(sets, pca), picks)
    n = len(prepared)
    if n == 0:
        return 0.0
    fold = np.empty(n, dtype=int)
    fold[np.random.default_rng(seed).permutation(n)] = np.arange(n) % folds
    right = 0
    for f in range(folds):
        test = [prepared[i] for i in range(n) if fold[i] == f]
        if not test:
            continue
        model = _fit_prepared([prepared[i] for i in range(n) if fold[i] != f], pca, lam)
        right += sum(_agrees(model, x, c) for x, c in test)
    return right / n


def train(
    sets: dict[int, list[Described]], picks: list[Pick], current: Model | None
) -> tuple[Model, float, bool]:
    """Choose lam by CV and fit on every pick. Promoted when its CV agreement is at least the
    current model's config (its lam, or the rule score when it is RULE_ONLY) on the same folds."""
    pca = fit_pca(sets)
    prepared = _usable(_rows(sets, pca), picks)
    if len(prepared) < MIN_PICKS:
        return RULE_ONLY(pca), _agreement(RULE_ONLY(pca), prepared), current is None
    cvs = {lam: cv_agreement(sets, picks, pca, lam) for lam in LAMS}
    lam = max(LAMS, key=lambda l: cvs[l])  # first of the best on a tie
    model = _fit_prepared(prepared, pca, lam)
    if current is None:
        return model, cvs[lam], True
    if _is_rule_only(current):
        baseline = _agreement(RULE_ONLY(pca), prepared)
    else:
        baseline = cv_agreement(sets, picks, pca, current.lam)
    return model, cvs[lam], cvs[lam] >= baseline


def score(model: Model, sets: dict[int, list[Described]]) -> dict[str, float]:
    """sha256 → score for every described candidate, each scored within its own set."""
    out: dict[str, float] = {}
    for cands in sets.values():
        if cands:
            for c, s in zip(cands, set_matrix(cands, model.pca) @ model.weights):
                out[c.sha256] = float(s)
    return out


def save(model: Model, path: str | Path) -> None:
    with open(path, "wb") as f:
        np.savez(
            f,
            weights=model.weights,
            lam=model.lam,
            pca=model.pca,
            feature_version=model.feature_version,
        )


def load(path: str | Path) -> Model:
    with np.load(path) as data:
        missing = {"weights", "lam", "pca", "feature_version"} - set(data.files)
        if missing:
            raise ValueError(f"model file {path} lacks {sorted(missing)}")
        weights = np.array(data["weights"], dtype=float)
        if weights.shape != (N_FEATURES,):
            raise ValueError(f"model file {path} has {weights.size} weights, not {N_FEATURES}")
        return Model(weights, float(data["lam"]), np.array(data["pca"]), int(data["feature_version"]))
