import numpy as np
import pytest

from pixelmagic_ai import ranker
from pixelmagic_ai.db import Described, Pick
from pixelmagic_ai.ranker import (
    FEATURE_NAMES,
    RULE_ONLY,
    Model,
    TrainResult,
    credit,
    cv_agreement,
    cv_credits,
    fit,
    fold_ids,
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
        picks.append(Pick(cands[chosen].sha256, [c.sha256 for c in cands], i))
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
    r = train(sets, picks, None)
    assert np.array_equal(r.model.weights, RULE_ONLY(pca).weights)
    assert 0 <= r.cv <= 1 and r.promoted
    assert not train(sets, picks, r.model).promoted


def test_undescribed_picks_are_skipped():
    rng = np.random.default_rng(6)
    sets, picks = synthetic(rng, 30, "s")
    pca = fit_pca(sets)
    stale = [
        Pick("gone", ["gone", "s0-0"], 900),
        Pick("s0-0", ["s0-0", "gone"], 901),
        Pick("gone", ["gone", "also-gone"], 902),
    ]
    a = fit(sets, picks, pca, 0.1)
    b = fit(sets, picks + stale, pca, 0.1)
    assert np.allclose(a.weights, b.weights)
    assert cv_agreement(sets, stale, pca, 0.1) == 0.0
    r = train(sets, stale, None)
    assert r.cv == 0.0 and np.array_equal(r.model.weights, RULE_ONLY(pca).weights)
    assert r.cv_n == 0 and r.chance is None and r.rule_agreement is None


def test_train_not_promoted_below_current_config(monkeypatch):
    rng = np.random.default_rng(7)
    sets, picks = synthetic(rng, 40, "p")
    current = Model(np.zeros(44), 0.5, fit_pca(sets), 1)
    monkeypatch.setattr(ranker, "cv_credits",
                        lambda s, p, pca, lam, **kw: np.full(40, 0.9 if lam == 0.5 else 0.6))
    r = train(sets, picks, current)
    assert np.isclose(r.cv, 0.6) and not r.promoted
    monkeypatch.setattr(ranker, "cv_credits",
                        lambda s, p, pca, lam, **kw: np.full(40, 0.6 if lam == 0.5 else 0.7))
    r = train(sets, picks, current)
    assert np.isclose(r.cv, 0.7) and r.promoted


def test_train_picks_lam_and_beats_rule_on_synthetic():
    rng = np.random.default_rng(8)
    sets, picks = synthetic(rng, 120, "q")
    r = train(sets, picks, RULE_ONLY(fit_pca(sets)))
    assert r.model.lam in {0.01, 0.1, 1, 10}
    assert int(np.argmax(r.model.weights)) == EYES
    assert r.promoted and r.cv > 0.5


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


def test_train_too_few_picks_for_cv_folds_is_rule_only():
    assert ranker.MIN_TRAIN_PICKS == 25
    rng = np.random.default_rng(10)
    sets, picks = synthetic(rng, 22, "u")
    pca = fit_pca(sets)
    r = train(sets, picks, RULE_ONLY(pca))
    assert np.array_equal(r.model.weights, RULE_ONLY(pca).weights)
    assert not r.promoted and 0 <= r.cv <= 1
    # Below MIN_TRAIN_PICKS the rule is the model: its figure is stored, no paired counts.
    assert r.cv == r.rule_agreement and r.cv_n == 22 and r.chance == 0.25
    assert (r.model_wins, r.rule_wins) == (0, 0)


def test_train_with_enough_picks_for_cv_fits():
    rng = np.random.default_rng(11)
    sets, picks = synthetic(rng, 25, "v")
    model = train(sets, picks, None).model
    assert not np.array_equal(model.weights, RULE_ONLY(fit_pca(sets)).weights)
    assert model.lam in {0.01, 0.1, 1, 10}


def test_train_reuses_grid_cv_for_current_lam(monkeypatch):
    rng = np.random.default_rng(12)
    sets, picks = synthetic(rng, 40, "w")
    current = Model(np.zeros(44), 1.0, fit_pca(sets), 1)
    calls = []

    def fake(s, p, pca, lam, **kw):
        calls.append(lam)
        return np.full(40, {0.01: 0.5, 0.1: 0.6, 1.0: 0.7, 10.0: 0.4}[lam])

    monkeypatch.setattr(ranker, "cv_credits", fake)
    r = train(sets, picks, current)
    assert sorted(calls) == [0.01, 0.1, 1.0, 10.0]
    assert r.model.lam == 1.0 and np.isclose(r.cv, 0.7) and r.promoted


# ---- scorecard: tie credit, baselines, folds by set --------------------------------------


def test_credit_is_one_alone_at_the_top_a_share_when_tied_else_zero():
    pca = np.zeros((DIM, 32))
    w = np.zeros(44)
    w[0] = 1.0
    model = Model(w, 0.0, pca, 1)
    x = np.zeros((3, 44))
    x[:, 0] = [0.2, 0.9, 0.5]
    assert credit(model, x, 1) == 1.0
    assert credit(model, x, 0) == 0.0
    x[:, 0] = [0.9, 0.9, 0.5]
    assert credit(model, x, 0) == 0.5 and credit(model, x, 1) == 0.5 and credit(model, x, 2) == 0
    x[:, 0] = [0.9, 0.9 + 1e-12, 0.9]  # within the tolerance: a three-way tie
    assert credit(model, x, 2) == pytest.approx(1 / 3)


def test_train_records_baselines_on_the_held_out_picks():
    rng = np.random.default_rng(20)
    sets, picks = synthetic(rng, 40, "b")
    r = train(sets, picks, None)
    assert isinstance(r, TrainResult)
    assert r.cv_n == 40 and r.chance == 0.25  # four candidates in every set
    assert r.cv == pytest.approx(r.model_credits.mean())
    assert r.rule_agreement == pytest.approx(r.rule_credits.mean())
    assert len(r.model_credits) == len(r.rule_credits) == 40
    differ = int(np.sum(np.abs(r.model_credits - r.rule_credits) > 1e-9))
    assert r.model_wins + r.rule_wins == differ
    assert r.model_wins == int(np.sum(r.model_credits > r.rule_credits + 1e-9))


def test_chance_is_the_mean_of_one_over_candidates():
    rng = np.random.default_rng(21)
    sets, picks = synthetic(rng, 10, "c", size=2)
    more, more_picks = synthetic(rng, 10, "d", size=4)
    sets.update({k + 100: v for k, v in more.items()})
    picks += [Pick(p.chosen, p.shown, p.set_id + 100) for p in more_picks]
    r = train(sets, picks, None)
    assert r.cv_n == 20 and r.chance == pytest.approx((10 * 0.5 + 10 * 0.25) / 20)


def test_rule_ties_are_counted_and_get_half_credit():
    rng = np.random.default_rng(22)
    a = cand("a", rng, eyes=[0.9], sharpness=200)
    b = Described("b", a.embedding.copy(), dict(a.quality), [dict(f) for f in a.faces])
    sets = {1: [a, b]}
    r = train(sets, [Pick("a", ["a", "b"], 1)], None)
    assert r.cv_n == 1 and r.rule_ties == 1
    assert r.rule_agreement == 0.5 and r.cv == 0.5


def test_fold_ids_never_split_a_set():
    folds = fold_ids([5, 5, 7, 7, 7, 9, 11, 11], 5, seed=7)
    assert len(folds) == 8
    assert folds[0] == folds[1] and folds[2] == folds[3] == folds[4] and folds[6] == folds[7]
    assert sorted(set(folds)) == [0, 1, 2, 3]  # four sets, four folds used


def test_fold_ids_with_one_pick_per_set_are_the_per_pick_folds():
    n = 77
    expected = np.empty(n, dtype=int)
    expected[np.random.default_rng(7).permutation(n)] = np.arange(n) % 5
    assert list(fold_ids(list(range(1000, 1000 + n)), 5, seed=7)) == list(expected)


def test_fitted_model_credits_are_strict_but_for_at_most_one_pick():
    rng = np.random.default_rng(23)
    sets, picks = synthetic(rng, 60, "e")
    credits = cv_credits(sets, picks, fit_pca(sets), 0.1)
    assert len(credits) == 60
    assert np.sum((credits != 0) & (credits != 1)) <= 1
    assert cv_agreement(sets, picks, fit_pca(sets), 0.1) == pytest.approx(credits.mean())
