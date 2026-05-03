# 原因
# 今回のエラーは、optimizer.py がまだ「ROI版の指標名」を前提にしているためです。
#
# 具体的には scoring.py を top5命中率専用版へ変えたことで、
# summarize_stability() の返り値が
#   - top5_point_rate
#   - top3_complete_rate
#   - win_in_top5_rate
#   - place_in_top5_rate
# になりました。
#
# でも optimizer.py 側はまだ
#   - top10_share
#   - top5_share
#   - top1_share
#   - hit_payout_cv
#   - hit_pay_median
# を読みに行っているので、
# best_stab['top10_share']
# で KeyError になっています。
#
# つまり今必要なのは scoring.py ではなく optimizer.py の修正です。
#
# 下に、top5命中率専用版に合わせた optimizer.py の完全コードを書きます。
# 必要箇所以外の流れは既存のままです。
#
# これで置き換えてください。


# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import math
import random
from typing import Any, Dict

import pandas as pd

from .common import WeightsMap, _blend_weights, _clip_weight_by_name, _normalize_surface_name
from .config import CONFIG, FEAT_COLS, FEATURE_WEIGHTS_SEED
from .scoring import build_eval_context, better_by_objective, calc_objective_score, eval_success_and_roi


def _stable_label_offset(label: str) -> int:
    digest = hashlib.md5(str(label or "").encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 10000


def _optimizer_seeds() -> list[int]:
    raw = CONFIG.get("OPTIMIZER_SEEDS", None)
    if raw is None:
        return [int(CONFIG["RANDOM_SEED"])]
    if isinstance(raw, str):
        seeds = [int(x.strip()) for x in raw.split(",") if x.strip()]
    else:
        seeds = [int(x) for x in raw]
    return seeds or [int(CONFIG["RANDOM_SEED"])]


def random_neighbor(
    weights: Dict[str, float],
    perturb_p: float,
    logn_sigma: float,
    add_eps_sd: float,
) -> Dict[str, float]:
    w_new = dict(weights)
    for k in FEAT_COLS:
        if random.random() < perturb_p:
            base = float(w_new.get(k, 0.0))
            sign = 1.0 if base >= 0 else -1.0
            mag = abs(base) if base != 0 else 0.1
            mult = math.exp(random.gauss(0, logn_sigma))
            add = random.gauss(0, add_eps_sd)
            v = sign * (mag * mult) + add
            w_new[k] = _clip_weight_by_name(k, v)
        else:
            w_new[k] = _clip_weight_by_name(k, float(w_new.get(k, 0.0)))
    return w_new


def optimize_single_weight_set(
    seed_w: Dict[str, float],
    df_feat: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
    n_iter: int,
    label: str = "",
    optimizer_seed: int | None = None,
) -> tuple[Dict[str, float], Dict[str, Any], Dict[str, Dict[str, Any]]]:
    seeds = [int(optimizer_seed)] if optimizer_seed is not None else _optimizer_seeds()
    if len(seeds) > 1:
        best_result = None
        seed_rows = []
        seed_weight_rows = []
        print(f"[INFO] {label} は複数seedで最適化します: {seeds}")
        for seed in seeds:
            w, summary, det = optimize_single_weight_set(
                seed_w=seed_w,
                df_feat=df_feat,
                df_res_entries=df_res_entries,
                df_res_payout=df_res_payout,
                n_iter=n_iter,
                label=f"{label}#seed{seed}",
                optimizer_seed=seed,
            )
            seed_rows.append({
                "optimizer_seed": seed,
                "best_objective": float(summary.get("best_objective", 0.0)),
                "best_top5_point_rate": float(summary.get("best_top5_point_rate", 0.0)),
                "best_top3_complete_rate": float(summary.get("best_top3_complete_rate", 0.0)),
                "best_win_in_top5_rate": float(summary.get("best_win_in_top5_rate", 0.0)),
                "best_place_in_top5_rate": float(summary.get("best_place_in_top5_rate", 0.0)),
            })
            seed_weight_rows.append(w)
            if best_result is None:
                best_result = (w, summary, det)
                continue
            _, best_summary, _ = best_result
            if better_by_objective(
                float(summary.get("best_objective", 0.0)),
                {
                    "top5_point_rate": float(summary.get("best_top5_point_rate", 0.0)),
                    "top3_complete_rate": float(summary.get("best_top3_complete_rate", 0.0)),
                    "win_in_top5_rate": float(summary.get("best_win_in_top5_rate", 0.0)),
                    "place_in_top5_rate": float(summary.get("best_place_in_top5_rate", 0.0)),
                    "rank1_place_rate": float(summary.get("best_rank1_place_rate", 0.0)),
                    "rank1_win_rate": float(summary.get("best_rank1_win_rate", 0.0)),
                },
                float(best_summary.get("best_objective", 0.0)),
                {
                    "top5_point_rate": float(best_summary.get("best_top5_point_rate", 0.0)),
                    "top3_complete_rate": float(best_summary.get("best_top3_complete_rate", 0.0)),
                    "win_in_top5_rate": float(best_summary.get("best_win_in_top5_rate", 0.0)),
                    "place_in_top5_rate": float(best_summary.get("best_place_in_top5_rate", 0.0)),
                    "rank1_place_rate": float(best_summary.get("best_rank1_place_rate", 0.0)),
                    "rank1_win_rate": float(best_summary.get("best_rank1_win_rate", 0.0)),
                },
            ):
                best_result = (w, summary, det)

        assert best_result is not None
        best_w, best_summary, best_det = best_result
        best_obj = float(best_summary.get("best_objective", 0.0))

        median_w = {
            col: _clip_weight_by_name(
                col,
                float(pd.Series([float(w.get(col, 0.0)) for w in seed_weight_rows]).median()),
            )
            for col in FEAT_COLS
        }
        median_map = {"__default__": median_w}
        median_s, median_t, median_i, median_r, median_det, median_stab = eval_success_and_roi(
            median_map, df_feat, df_res_entries, df_res_payout
        )
        median_obj = calc_objective_score(median_stab)
        seed_rows.append({
            "optimizer_seed": "median",
            "best_objective": float(median_obj),
            "best_top5_point_rate": float(median_stab.get("top5_point_rate", 0.0)),
            "best_top3_complete_rate": float(median_stab.get("top3_complete_rate", 0.0)),
            "best_win_in_top5_rate": float(median_stab.get("win_in_top5_rate", 0.0)),
            "best_place_in_top5_rate": float(median_stab.get("place_in_top5_rate", 0.0)),
        })

        accept_ratio = float(CONFIG.get("MULTISEED_MEDIAN_ACCEPT_RATIO", 0.98) or 0.98)
        if best_obj > 0.0 and median_obj >= best_obj * accept_ratio:
            best_w = median_w
            best_det = median_det
            best_summary = {
                "best_success": median_s,
                "best_total": median_t,
                "best_invest": median_i,
                "best_return": median_r,
                "best_roi": (median_r / median_i) if median_i else 0.0,
                "best_success_rate": median_stab["success_rate"],
                "best_objective": median_obj,
                "best_top5_point_rate": median_stab["top5_point_rate"],
                "best_top3_complete_rate": median_stab["top3_complete_rate"],
                "best_win_in_top5_rate": median_stab["win_in_top5_rate"],
                "best_place_in_top5_rate": median_stab["place_in_top5_rate"],
                "best_rank1_place_rate": median_stab.get("rank1_place_rate", 0.0),
                "best_rank1_win_rate": median_stab.get("rank1_win_rate", 0.0),
                "optimizer_seed": "median",
            }

        best_summary = dict(best_summary)
        best_summary["seed_results_json"] = json.dumps(seed_rows, ensure_ascii=False)
        print(
            f"[INFO] {label} 採用seed={best_summary.get('optimizer_seed')} "
            f"best_obj={best_summary.get('best_objective', 0.0):.4f} "
            f"best_complete={best_summary.get('best_top3_complete_rate', 0.0):.3f}"
        )
        return best_w, best_summary, best_det

    actual_seed = int(seeds[0])
    random.seed(actual_seed + _stable_label_offset(label))
    eval_context = build_eval_context(df_feat, df_res_entries, df_res_payout)

    cur_w = {k: _clip_weight_by_name(k, seed_w.get(k, 0.0)) for k in FEAT_COLS}
    cur_map = {"__default__": cur_w}

    cur_succ, cur_total, cur_invest, cur_return, cur_det, cur_stab = eval_success_and_roi(
        cur_map, df_feat, df_res_entries, df_res_payout, eval_context=eval_context
    )
    cur_obj = calc_objective_score(cur_stab)

    best_w = dict(cur_w)
    best_succ, best_total, best_invest, best_return = cur_succ, cur_total, cur_invest, cur_return
    best_det = dict(cur_det)
    best_stab = dict(cur_stab)
    best_obj = cur_obj

    for it in range(1, int(n_iter) + 1):
        cand_w = random_neighbor(cur_w, CONFIG["PERTURB_P"], CONFIG["LOGN_SIGMA"], CONFIG["ADD_EPS_SD"])
        cand_map = {"__default__": cand_w}
        cand_succ, cand_total, cand_invest, cand_return, cand_det, cand_stab = eval_success_and_roi(
            cand_map, df_feat, df_res_entries, df_res_payout, eval_context=eval_context
        )
        cand_obj = calc_objective_score(cand_stab)

        better = better_by_objective(cand_obj, cand_stab, cur_obj, cur_stab)

        if better:
            cur_w = cand_w
            cur_succ, cur_total, cur_invest, cur_return, cur_det, cur_stab = (
                cand_succ, cand_total, cand_invest, cand_return, cand_det, cand_stab
            )
            cur_obj = cand_obj

            if better_by_objective(cur_obj, cur_stab, best_obj, best_stab):
                best_w = dict(cur_w)
                best_succ, best_total, best_invest, best_return = cur_succ, cur_total, cur_invest, cur_return
                best_det = dict(cur_det)
                best_stab = dict(cur_stab)
                best_obj = cur_obj
        else:
            T = max(1e-6, 1.0 - it / max(int(n_iter), 1))
            delta = cand_obj - cur_obj
            acc_p = 1.0 if delta >= 0 else math.exp(delta / max(T, 1e-6))
            if random.random() < acc_p:
                cur_w = cand_w
                cur_succ, cur_total, cur_invest, cur_return, cur_det, cur_stab = (
                    cand_succ, cand_total, cand_invest, cand_return, cand_det, cand_stab
                )
                cur_obj = cand_obj

        if it % max(1, int(n_iter) // 5) == 0:
            print(
                f"[{label} {it}/{n_iter}] "
                f"cur_obj={cur_obj:.4f} "
                f"cur_point_rate={cur_stab['top5_point_rate']:.3f} "
                f"cur_complete={cur_stab['top3_complete_rate']:.3f} "
                f"cur_win_in5={cur_stab['win_in_top5_rate']:.3f} "
                f"best_obj={best_obj:.4f} "
                f"best_point_rate={best_stab['top5_point_rate']:.3f} "
                f"best_complete={best_stab['top3_complete_rate']:.3f} "
                f"best_win_in5={best_stab['win_in_top5_rate']:.3f}"
            )

    summary = {
        "best_success": best_succ,  # 互換のため名前はそのまま。中身は point_sum
        "best_total": best_total,   # 評価レース数
        "best_invest": best_invest,
        "best_return": best_return,
        "best_roi": (best_return / best_invest) if best_invest else 0.0,
        "best_success_rate": best_stab["success_rate"],  # 互換用
        "best_objective": best_obj,
        "best_top5_point_rate": best_stab["top5_point_rate"],
        "best_top3_complete_rate": best_stab["top3_complete_rate"],
        "best_win_in_top5_rate": best_stab["win_in_top5_rate"],
        "best_place_in_top5_rate": best_stab["place_in_top5_rate"],
        "best_rank1_place_rate": best_stab.get("rank1_place_rate", 0.0),
        "best_rank1_win_rate": best_stab.get("rank1_win_rate", 0.0),
        "optimizer_seed": actual_seed,
    }
    return best_w, summary, best_det


def optimize_placewise_weights(
    df_train: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
) -> tuple[WeightsMap, pd.DataFrame]:
    df_train = df_train.copy()
    if "place_name" not in df_train.columns:
        df_train["place_name"] = ""
    if "surface_name" not in df_train.columns:
        df_train["surface_name"] = ""
    df_train["place_name"] = df_train["place_name"].fillna("").astype(str).str.strip()
    df_train["surface_name"] = df_train["surface_name"].fillna("").map(_normalize_surface_name)

    print("\n[INFO] === 全体共通重みを最適化します ===")
    default_w, default_summary, _ = optimize_single_weight_set(
        seed_w=FEATURE_WEIGHTS_SEED,
        df_feat=df_train,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        n_iter=int(CONFIG["N_ITER_DEFAULT"]),
        label="DEFAULT",
    )

    weights_map: WeightsMap = {"__default__": default_w}
    summary_rows: list[Dict[str, Any]] = [{
        "group_type": "default",
        "group_key": "__default__",
        "place_name": "__default__",
        "surface_name": "",
        "rid_count": int(df_train["rid_str"].astype(str).nunique()),
        "used_model": 1,
        **default_summary,
    }]

    train_place_counts = (
        df_train[["rid_str", "place_name"]]
        .drop_duplicates(subset=["rid_str"])
        .groupby("place_name")["rid_str"]
        .nunique()
        .sort_values(ascending=False)
    )

    for place_name, rid_count in train_place_counts.items():
        place_name = str(place_name or "")
        if not place_name:
            continue

        place_df = df_train[df_train["place_name"].astype(str) == place_name].copy()
        place_rid_count = int(place_df["rid_str"].astype(str).nunique())

        print(f"\n[INFO] === 場所別重み: {place_name} / 学習rid数={place_rid_count} ===")

        if place_rid_count < int(CONFIG["MIN_PLACE_RACES"]):
            print(f"[INFO] {place_name} は学習rid数不足のため default 重みを使用します")
            summary_rows.append({
                "group_type": "place",
                "group_key": place_name,
                "place_name": place_name,
                "surface_name": "",
                "rid_count": place_rid_count,
                "used_model": 0,
                "fallback_target": "__default__",
                "reason": "insufficient_races",
            })
            continue

        place_seed = dict(default_w)
        place_w_raw, place_summary, _ = optimize_single_weight_set(
            seed_w=place_seed,
            df_feat=place_df,
            df_res_entries=df_res_entries,
            df_res_payout=df_res_payout,
            n_iter=int(CONFIG["N_ITER_PLACE"]),
            label=place_name,
        )

        blend_alpha = float(CONFIG["PLACE_BLEND_WITH_DEFAULT"])
        place_w = _blend_weights(default_w, place_w_raw, blend_alpha)

        place_map_eval = {"__default__": default_w, place_name: place_w}
        p_s, p_t, p_i, p_r, _, p_stab = eval_success_and_roi(
            place_map_eval, place_df, df_res_entries, df_res_payout
        )

        if p_t < int(CONFIG["MIN_PLACE_BETS"]):
            print(f"[INFO] {place_name} は評価レース数が少なすぎるため default 重みを使用します")
            summary_rows.append({
                "group_type": "place",
                "group_key": place_name,
                "place_name": place_name,
                "surface_name": "",
                "rid_count": place_rid_count,
                "used_model": 0,
                "fallback_target": "__default__",
                "reason": "insufficient_bets_after_opt",
                "best_total": p_t,
                "best_top5_point_rate": p_stab["top5_point_rate"],
                "best_top3_complete_rate": p_stab["top3_complete_rate"],
                "best_win_in_top5_rate": p_stab["win_in_top5_rate"],
                "best_place_in_top5_rate": p_stab["place_in_top5_rate"],
            })
            continue

        weights_map[place_name] = place_w
        summary_rows.append({
            "group_type": "place",
            "group_key": place_name,
            "place_name": place_name,
            "surface_name": "",
            "rid_count": place_rid_count,
            "used_model": 1,
            "best_success": p_s,
            "best_total": p_t,
            "best_invest": p_i,
            "best_return": p_r,
            "best_roi": p_stab["roi"],
            "best_success_rate": p_stab["success_rate"],
            "best_top5_point_rate": p_stab["top5_point_rate"],
            "best_top3_complete_rate": p_stab["top3_complete_rate"],
            "best_win_in_top5_rate": p_stab["win_in_top5_rate"],
            "best_place_in_top5_rate": p_stab["place_in_top5_rate"],
            "blend_alpha": blend_alpha,
            "optimizer_seed": place_summary.get("optimizer_seed"),
            "raw_best_objective": place_summary.get("best_objective"),
            "seed_results_json": place_summary.get("seed_results_json", ""),
        })

    train_place_surface_counts = (
        df_train[["rid_str", "place_name", "surface_name"]]
        .drop_duplicates(subset=["rid_str"])
        .groupby(["place_name", "surface_name"])["rid_str"]
        .nunique()
        .sort_values(ascending=False)
    )

    for (place_name, surface_name), rid_count in train_place_surface_counts.items():
        place_name = str(place_name or "").strip()
        surface_name = _normalize_surface_name(surface_name)
        if not place_name or not surface_name:
            continue

        place_surface_df = df_train[
            (df_train["place_name"].astype(str) == place_name)
            & (df_train["surface_name"].map(_normalize_surface_name) == surface_name)
        ].copy()
        place_surface_rid_count = int(place_surface_df["rid_str"].astype(str).nunique())

        print(
            f"\n[INFO] === 場所×芝ダ重み: {place_name}_{surface_name} / "
            f"学習rid数={place_surface_rid_count} ==="
        )

        parent_key = place_name if place_name in weights_map else "__default__"
        parent_w = weights_map[parent_key]

        if place_surface_rid_count < int(CONFIG["MIN_PLACE_SURFACE_RACES"]):
            print(f"[INFO] {place_name}_{surface_name} は学習rid数不足のため {parent_key} 重みを使用します")
            summary_rows.append({
                "group_type": "place_surface",
                "group_key": f"{place_name}_{surface_name}",
                "place_name": place_name,
                "surface_name": surface_name,
                "rid_count": place_surface_rid_count,
                "used_model": 0,
                "fallback_target": parent_key,
                "reason": "insufficient_races",
            })
            continue

        place_surface_seed = dict(parent_w)
        place_surface_w_raw, place_surface_summary, _ = optimize_single_weight_set(
            seed_w=place_surface_seed,
            df_feat=place_surface_df,
            df_res_entries=df_res_entries,
            df_res_payout=df_res_payout,
            n_iter=int(CONFIG["N_ITER_PLACE_SURFACE"]),
            label=f"{place_name}_{surface_name}",
        )

        blend_alpha = float(CONFIG["PLACE_SURFACE_BLEND_WITH_PLACE"])
        place_surface_w = _blend_weights(parent_w, place_surface_w_raw, blend_alpha)

        place_surface_key = (place_name, surface_name)
        place_surface_map_eval = dict(weights_map)
        place_surface_map_eval[place_surface_key] = place_surface_w
        ps_s, ps_t, ps_i, ps_r, _, ps_stab = eval_success_and_roi(
            place_surface_map_eval, place_surface_df, df_res_entries, df_res_payout
        )

        if ps_t < int(CONFIG["MIN_PLACE_SURFACE_BETS"]):
            print(f"[INFO] {place_name}_{surface_name} は評価レース数が少なすぎるため {parent_key} 重みを使用します")
            summary_rows.append({
                "group_type": "place_surface",
                "group_key": f"{place_name}_{surface_name}",
                "place_name": place_name,
                "surface_name": surface_name,
                "rid_count": place_surface_rid_count,
                "used_model": 0,
                "fallback_target": parent_key,
                "reason": "insufficient_bets_after_opt",
                "best_total": ps_t,
                "best_top5_point_rate": ps_stab["top5_point_rate"],
                "best_top3_complete_rate": ps_stab["top3_complete_rate"],
                "best_win_in_top5_rate": ps_stab["win_in_top5_rate"],
                "best_place_in_top5_rate": ps_stab["place_in_top5_rate"],
            })
            continue

        weights_map[place_surface_key] = place_surface_w
        summary_rows.append({
            "group_type": "place_surface",
            "group_key": f"{place_name}_{surface_name}",
            "place_name": place_name,
            "surface_name": surface_name,
            "rid_count": place_surface_rid_count,
            "used_model": 1,
            "fallback_target": parent_key,
            "best_success": ps_s,
            "best_total": ps_t,
            "best_invest": ps_i,
            "best_return": ps_r,
            "best_roi": ps_stab["roi"],
            "best_success_rate": ps_stab["success_rate"],
            "best_top5_point_rate": ps_stab["top5_point_rate"],
            "best_top3_complete_rate": ps_stab["top3_complete_rate"],
            "best_win_in_top5_rate": ps_stab["win_in_top5_rate"],
            "best_place_in_top5_rate": ps_stab["place_in_top5_rate"],
            "blend_alpha": blend_alpha,
            "optimizer_seed": place_surface_summary.get("optimizer_seed"),
            "raw_best_objective": place_surface_summary.get("best_objective"),
            "seed_results_json": place_surface_summary.get("seed_results_json", ""),
        })

    place_summary_df = pd.DataFrame(summary_rows)
    return weights_map, place_summary_df
