import numpy as np
import pytest

from pixelmagic_ai import ranker
from pixelmagic_ai.db import Described, Pick
from pixelmagic_ai.ranker import (
    FEATURE_NAMES,
    RULE_ONLY,
    Model,
    cv_agreement,
    fit,
    fit_pca,
    load,
    save,
    score,
    set_matrix,
    train,
)

DIM = 8
EYES = FEATURE_NAMES.index("eyes_open_all")


def cand(sha, rng, eyes=None, sharpness=None, n_faces=4):
    quality = {
        "sharpness": float(sharpness if sharpness is not None else rng.uniform(50, 500)),
        "brightness": float(rng.uniform(0.3, 0.7)),
        "highlights": float(rng.uniform(0, 0.05)),
        "shadows": float(rng.uniform(0, 0.05)),
        "noise": float(rng.uniform(0, 0.1)),
        "aesthetic": float(rng.uniform(3, 7)),
    }
    eyes = eyes if eyes is not None else list(rng.uniform(0, 1, n_faces))
    faces = [
        {
            "eyes_open": float(e),
            "smile": float(rng.uniform()),
            "quality": float(rng.uniform(0.4, 0.6)),
            "size": float(rng.uniform(0.05, 0.3)),
        }
        for e in eyes
    ]
    return Described(sha, rng.normal(size=DIM), quality, faces)


def synthetic(rng, n_sets, prefix, size=4, weight=6.0):
    """Sets whose picks follow softmax(weight · eyes_open_all), nothing else."""
    sets, picks = {}, []
    pca = np.zeros((DIM, 32))
    for i in range(n_sets):
        cands = [cand(f"{prefix}{i}-{j}", rng) for j in range(size)]
        sets[i] = cands
        u = weight * set_matrix(cands, pca)[:, EYES]
        p = np.exp(u - u.max())
        chosen = rng.choice(size, p=p / p.sum())
        picks.append(Pick(cands[chosen].sha256, [c.sha256 for c in cands]))
    return sets, picks


def test_feature_names_fixed():
    assert len(FEATURE_NAMES) == 44
    assert FEATURE_NAMES[:12] == [
        "rule", "sharpness_rel", "brightness_dev", "clipped", "noise_rel", "faces_max",
        "eyes_open_all", "eyes_open_min", "smile_mean", "face_quality_mean", "face_size_max",
        "aesthetic_rel",
    ]
    assert FEATURE_NAMES[12:] == [f"emb_{k}" for k in range(32)]


def test_set_matrix_shape_and_single_candidate():
    rng = np.random.default_rng(1)
    sets = {1: [cand("a", rng), cand("b", rng), cand("c", rng)], 2: [cand("d", rng)]}
    pca = fit_pca(sets)
    assert pca.shape == (DIM, 32)
    # 4 set-centred rows → at most 3 components, the rest zero.
    assert np.all(pca[:, 3:] == 0)
    assert set_matrix(sets[1], pca).shape == (3, 44)
    single = set_matrix(sets[2], pca)
    assert single.shape == (1, 44) and np.all(np.isfinite(single))


def test_no_faces_gives_zero_face_fields():
    rng = np.random.default_rng(2)
    cands = [cand("a", rng, eyes=[]), cand("b", rng, eyes=[0.9])]
    x = set_matrix(cands, np.zeros((DIM, 32)))
    for name in ["eyes_open_all", "eyes_open_min", "smile_mean", "face_quality_mean",
                 "face_size_max"]:
        assert x[0, FEATURE_NAMES.index(name)] == 0
    assert x[0, FEATURE_NAMES.index("faces_max")] == 0
    assert x[1, FEATURE_NAMES.index("faces_max")] == 1


def test_rule_only_prefers_eyes_open_sharper():
    rng = np.random.default_rng(3)
    closed = cand("closed", rng, eyes=[0.1, 0.2], sharpness=100)
    open_ = cand("open", rng, eyes=[0.9, 0.8], sharpness=400)
    for c in (closed, open_):
        c.quality.update(highlights=0.01, shadows=0.01, aesthetic=5.0)
        for f in c.faces:
            f["quality"] = 0.5
    sets = {1: [closed, open_]}
    scores = score(RULE_ONLY(np.zeros((DIM, 32))), sets)
    assert scores["open"] > scores["closed"]


def test_fit_recovers_known_weights():
    rng = np.random.default_rng(4)
    sets, picks = synthetic(rng, 400, "t")
    pca = fit_pca(sets)
    model = fit(sets, picks, pca, 0.1)
    assert int(np.argmax(model.weights)) == EYES

    held, _ = synthetic(rng, 300, "h")
    scores = score(model, held)
    right = total = 0
    for cands in held.values():
        truth = set_matrix(cands, pca)[:, EYES]
        if (truth == truth.max()).sum() > 1:
            continue
        total += 1
        best = max(cands, key=lambda c: scores[c.sha256])
        right += best.sha256 == cands[int(np.argmax(truth))].sha256
    assert total > 100
    assert right / total >= 0.9


def test_fewer_than_20_picks_is_rule_only():
    rng = np.random.default_rng(5)
    sets, picks = synthetic(rng, 19, "f")
    pca = fit_pca(sets)
    model = fit(sets, picks, pca, 0.1)
    assert np.array_equal(model.weights, RULE_ONLY(pca).weights)
    m, cv, promoted = train(sets, picks, None)
    assert np.array_equal(m.weights, RULE_ONLY(pca).weights)
    assert 0 <= cv <= 1 and promoted
    _, _, promoted = train(sets, picks, m)
    assert not promoted


def test_undescribed_picks_are_skipped():
    rng = np.random.default_rng(6)
    sets, picks = synthetic(rng, 30, "s")
    pca = fit_pca(sets)
    stale = [
        Pick("gone", ["gone", "s0-0"]),
        Pick("s0-0", ["s0-0", "gone"]),
        Pick("gone", ["gone", "also-gone"]),
    ]
    a = fit(sets, picks, pca, 0.1)
    b = fit(sets, picks + stale, pca, 0.1)
    assert np.allclose(a.weights, b.weights)
    assert cv_agreement(sets, stale, pca, 0.1) == 0.0
    m, cv, _ = train(sets, stale, None)
    assert cv == 0.0 and np.array_equal(m.weights, RULE_ONLY(pca).weights)


def test_train_not_promoted_below_current_config(monkeypatch):
    rng = np.random.default_rng(7)
    sets, picks = synthetic(rng, 40, "p")
    current = Model(np.zeros(44), 0.5, fit_pca(sets), 1)
    monkeypatch.setattr(ranker, "cv_agreement",
                        lambda s, p, pca, lam, **kw: 0.9 if lam == 0.5 else 0.6)
    _, cv, promoted = train(sets, picks, current)
    assert cv == 0.6 and not promoted
    monkeypatch.setattr(ranker, "cv_agreement",
                        lambda s, p, pca, lam, **kw: 0.6 if lam == 0.5 else 0.7)
    _, cv, promoted = train(sets, picks, current)
    assert cv == 0.7 and promoted


def test_train_picks_lam_and_beats_rule_on_synthetic():
    rng = np.random.default_rng(8)
    sets, picks = synthetic(rng, 120, "q")
    model, cv, promoted = train(sets, picks, RULE_ONLY(fit_pca(sets)))
    assert model.lam in {0.01, 0.1, 1, 10}
    assert int(np.argmax(model.weights)) == EYES
    assert promoted and cv > 0.5


def test_save_load_round_trip(tmp_path):
    rng = np.random.default_rng(9)
    m = Model(rng.normal(size=44), 0.1, rng.normal(size=(DIM, 32)), 1)
    path = tmp_path / "model"
    save(m, path)
    back = load(path)
    assert np.array_equal(back.weights, m.weights)
    assert np.array_equal(back.pca, m.pca)
    assert back.lam == 0.1 and back.feature_version == 1


def test_load_rejects_bad_files(tmp_path):
    bad = tmp_path / "bad.npz"
    with open(bad, "wb") as f:
        np.savez(f, weights=np.zeros(43), lam=0.1, pca=np.zeros((DIM, 32)), feature_version=1)
    with pytest.raises(ValueError):
        load(bad)
    missing = tmp_path / "missing.npz"
    with open(missing, "wb") as f:
        np.savez(f, weights=np.zeros(44), lam=0.1, feature_version=1)
    with pytest.raises(ValueError):
        load(missing)
