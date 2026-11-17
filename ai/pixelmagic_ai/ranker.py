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
FOLDS = 5
# Fewest usable picks for which every CV training fold still has MIN_PICKS (25 with 5 folds);
# below it every fold would fall back to RULE_ONLY and the CV would say nothing about the fit.
MIN_TRAIN_PICKS = -(-MIN_PICKS * FOLDS // (FOLDS - 1))
LAMS = (0.01, 0.1, 1.0, 10.0)
TIE = 1e-9  # two scores this close count as tied
_IDX = {name: i for i, name in enumerate(FEATURE_NAMES)}


@dataclass
class Model:
    weights: np.ndarray
    lam: float
    pca: np.ndarray
    feature_version: int


@dataclass
class TrainResult:
    """What train() found. The six figures after `promoted` are the scorecard's baselines, all
    measured on the same held-out picks; the two credit arrays are per pick, in pick order, and
    are not stored (the counts are)."""

    model: Model
    cv: float
    promoted: bool
    cv_n: int
    chance: float | None
    rule_agreement: float | None
    rule_ties: int | None
    model_wins: int
    rule_wins: int
    model_credits: np.ndarray
    rule_credits: np.ndarray


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


Prepared = list[tuple[np.ndarray, int, int]]


def _usable(rows: dict[str, np.ndarray], picks: list[Pick]) -> Prepared:
    """(shown-candidate matrix, chosen index, set id) for each pick still described enough to
    use."""
    out = []
    for p in picks:
        shown = [s for s in dict.fromkeys(p.shown) if s in rows]
        if p.chosen in shown and len(shown) >= 2:
            out.append((np.stack([rows[s] for s in shown]), shown.index(p.chosen), p.set_id))
    return out


def _fit_prepared(prepared: Prepared, pca: np.ndarray, lam: float) -> Model:
    if len(prepared) < MIN_PICKS:
        return RULE_ONLY(pca)

    def objective(w: np.ndarray) -> tuple[float, np.ndarray]:
        loss = lam * (w @ w)
        grad = 2 * lam * w
        for x, c, _ in prepared:
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


def credit(model: Model, x: np.ndarray, c: int) -> float:
    """How far the model agrees with one pick: 1 when the chosen candidate alone scores highest,
    1/k when it is one of k tied at the top (the model has no preference among them), else 0."""
    s = x @ model.weights
    top = s.max() - TIE
    return float(1 / np.sum(s >= top)) if s[c] >= top else 0.0


def _tied_at_top(model: Model, x: np.ndarray) -> bool:
    s = x @ model.weights
    return int(np.sum(s >= s.max() - TIE)) > 1


def _credits(model: Model, prepared: Prepared) -> np.ndarray:
    return np.array([credit(model, x, c) for x, c, _ in prepared], dtype=float)


def _agreement(model: Model, prepared: Prepared) -> float:
    if not prepared:
        return 0.0
    return float(_credits(model, prepared).mean())


def fold_ids(set_ids: list[int], folds: int, seed: int) -> np.ndarray:
    """A fold for each pick, by set: every pick of a set lands in one fold, so a second pick on a
    set can never be held out against a model that trained on the first. Sets are numbered in
    order of first pick and permuted; with one pick per set this is a permutation of the picks."""
    order = list(dict.fromkeys(set_ids))
    index = {s: i for i, s in enumerate(order)}
    set_fold = np.empty(len(order), dtype=int)
    set_fold[np.random.default_rng(seed).permutation(len(order))] = np.arange(len(order)) % folds
    return np.array([set_fold[index[s]] for s in set_ids], dtype=int)


def cv_credits(
    sets: dict[int, list[Described]],
    picks: list[Pick],
    pca: np.ndarray,
    lam: float,
    folds: int = FOLDS,
    seed: int = 7,
) -> np.ndarray:
    """Each usable pick's credit (in pick order) from the model fitted on the other folds."""
    prepared = _usable(_rows(sets, pca), picks)
    n = len(prepared)
    fold = fold_ids([s for _, _, s in prepared], folds, seed)
    out = np.zeros(n)
    for f in range(folds):
        test = [i for i in range(n) if fold[i] == f]
        if not test:
            continue
        model = _fit_prepared([prepared[i] for i in range(n) if fold[i] != f], pca, lam)
        for i in test:
            out[i] = credit(model, prepared[i][0], prepared[i][1])
    return out


def cv_agreement(
    sets: dict[int, list[Described]],
    picks: list[Pick],
    pca: np.ndarray,
    lam: float,
    folds: int = FOLDS,
    seed: int = 7,
) -> float:
    """Mean credit over the held-out picks; 0.0 with no usable pick."""
    credits = cv_credits(sets, picks, pca, lam, folds, seed)
    return float(credits.mean()) if len(credits) else 0.0


class _Graded:
    """The rule score and chance graded on the usable picks; builds a TrainResult for a model
    graded on the same picks, with the paired counts against the rule."""

    def __init__(self, prepared: Prepared, pca: np.ndarray) -> None:
        self.prepared = prepared
        self.rule = RULE_ONLY(pca)
        self.rule_credits = _credits(self.rule, prepared)
        self.n = len(prepared)
        self.chance = float(np.mean([1 / len(x) for x, _, _ in prepared])) if self.n else None
        self.rule_agreement = float(self.rule_credits.mean()) if self.n else None
        self.rule_ties = sum(_tied_at_top(self.rule, x) for x, _, _ in prepared) if self.n else None

    def result(self, model: Model, credits: np.ndarray, promoted: bool) -> TrainResult:
        wins = int(np.sum(credits > self.rule_credits + TIE))
        losses = int(np.sum(self.rule_credits > credits + TIE))
        cv = float(credits.mean()) if self.n else 0.0
        return TrainResult(model, cv, promoted, self.n, self.chance, self.rule_agreement,
                           self.rule_ties, wins, losses, credits, self.rule_credits)

    def rule_result(self, promoted: bool) -> TrainResult:
        """The rule is the model here: its own figure is the CV, and no paired counts."""
        r = self.result(self.rule, self.rule_credits, promoted)
        r.model_wins = r.rule_wins = 0
        return r


def train(sets: dict[int, list[Described]], picks: list[Pick], current: Model | None) -> TrainResult:
    """Choose lam by CV and fit on every pick. Promoted when its CV agreement is at least the
    current model's config (its lam, or the rule score when it is RULE_ONLY) on the same folds.
    Below MIN_TRAIN_PICKS usable picks the CV cannot test a fit, so it returns RULE_ONLY, graded
    on every usable pick. The rule score and chance are always graded on the same picks."""
    pca = fit_pca(sets)
    prepared = _usable(_rows(sets, pca), picks)
    graded = _Graded(prepared, pca)
    if graded.n < MIN_TRAIN_PICKS:
        return graded.rule_result(current is None)
    cvs = {lam: cv_credits(sets, picks, pca, lam) for lam in LAMS}
    means = {lam: float(cvs[lam].mean()) for lam in LAMS}
    lam = max(LAMS, key=lambda l: means[l])  # first of the best on a tie
    model = _fit_prepared(prepared, pca, lam)
    if current is None:
        return graded.result(model, cvs[lam], True)
    if _is_rule_only(current):
        baseline = graded.rule_agreement
    elif current.lam in cvs:
        baseline = means[current.lam]  # same picks, same folds: the value is identical
    else:
        baseline = float(cv_credits(sets, picks, pca, current.lam).mean())
    return graded.result(model, cvs[lam], means[lam] >= baseline)


def evaluate(sets: dict[int, list[Described]], picks: list[Pick], lam: float | None) -> TrainResult:
    """train() at a fixed lam (None: the rule score), for re-measuring an old version on the
    picks it had. `promoted` is False; the stored row says what happened."""
    pca = fit_pca(sets)
    prepared = _usable(_rows(sets, pca), picks)
    graded = _Graded(prepared, pca)
    if lam is None or graded.n < MIN_TRAIN_PICKS:
        return graded.rule_result(False)
    return graded.result(_fit_prepared(prepared, pca, lam), cv_credits(sets, picks, pca, lam), False)


def picks_used(model: Model, sets: dict[int, list[Described]], picks: list[Pick]) -> int:
    """Picks the model was fitted on: the usable ones, or 0 for the rule score."""
    if _is_rule_only(model):
        return 0
    return len(_usable(_rows(sets, model.pca), picks))


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
