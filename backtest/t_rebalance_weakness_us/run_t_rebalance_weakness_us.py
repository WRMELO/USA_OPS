"""Backtest T-REBALANCE-WEAKNESS-US: rank-decay + spc_status pre-rebalance.

Paridade metodologica: RENDA_OPS D-082 (Top-20 study) + D-083 (Top-30 extension).
Estudo isolado, sem tocar motor produtivo.
Ref: USA_OPS D-073, D-070, D-066, RENDA_OPS D-082, D-083.
"""
from __future__ import annotations
import json, sys
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from lib.engine import compute_m3_scores, select_top_n
from lib.io import read_json

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_us.parquet"
IN_WINNER    = ROOT / "config" / "winner_us.json"
OUT_DIR      = ROOT / "backtest" / "t_rebalance_weakness_us" / "results"

TRAIN_END       = pd.Timestamp("2022-12-30")
HOLDOUT_START   = pd.Timestamp("2023-01-02")
LOOKBACKS       = [1, 2, 3, 5, 10]
HORIZONS        = [1, 3, 5]
SKIP_INITIAL    = max(LOOKBACKS) * 2
VALID_SIGNALS   = ["SUBINDO", "ESTAVEL", "CAINDO"]
TOP_N_UNIVERSE  = 30

VERDICT_CRITERIA = {
    "_note": "Criterios pre-registrados antes da execucao. NAO ALTERAR apos rodar.",
    "primary_split": "HOLDOUT",
    "primary_lookback_L": 5,
    "primary_horizon": 3,
    "primary_sample_group": "TOPN_ALL",
    "min_n_per_bucket": 50,
    "CONFIRMA_SINAL_US": {
        "description": "INSTAVEL pior que ESTAVEL em HOLDOUT - ambos gates simultaneos",
        "gates": [
            "became_instavel_3_rate(INSTAVEL) > became_instavel_3_rate(ESTAVEL) + 0.05",
            "log_ret_3_mean(INSTAVEL) < log_ret_3_mean(ESTAVEL) - 0.015",
        ],
    },
    "NAO_CONFIRMA_SINAL_US": {
        "description": "Sem diferenca material entre buckets",
        "gates": [
            "became_instavel_3_rate(INSTAVEL) <= became_instavel_3_rate(ESTAVEL)",
            "OU log_ret_3_mean(INSTAVEL) >= log_ret_3_mean(ESTAVEL)",
        ],
    },
    "INCONCLUSIVO": {
        "description": "Um gate confirma e o outro nao; ou n < min_n_per_bucket em algum bucket",
    },
}


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _near_upper(value: float, ucl: float, pct: float = 0.10) -> bool:
    if not (_is_finite(value) and _is_finite(ucl)):
        return False
    if value > ucl:
        return False
    tol = max(abs(ucl) * pct, 1e-9)
    return (ucl - value) <= tol


def _near_lower(value: float, lcl: float, pct: float = 0.10) -> bool:
    if not (_is_finite(value) and _is_finite(lcl)):
        return False
    if value < lcl:
        return False
    tol = max(abs(lcl) * pct, 1e-9)
    return (value - lcl) <= tol


def classify_spc_status(row: pd.Series) -> str:
    required = ["i_value","i_ucl","i_lcl","mr_value","mr_ucl",
                "xbar_value","xbar_ucl","xbar_lcl","r_value","r_ucl"]
    if not all(_is_finite(row.get(c, np.nan)) for c in required):
        return "ESTAVEL"
    i_v,i_u,i_l = float(row["i_value"]),float(row["i_ucl"]),float(row["i_lcl"])
    mr_v,mr_u    = float(row["mr_value"]),float(row["mr_ucl"])
    xb_v,xb_u,xb_l = float(row["xbar_value"]),float(row["xbar_ucl"]),float(row["xbar_lcl"])
    r_v,r_u      = float(row["r_value"]),float(row["r_ucl"])
    if (i_v>i_u or i_v<i_l or mr_v>mr_u or xb_v>xb_u or xb_v<xb_l or r_v>r_u):
        return "INSTAVEL"
    if (_near_upper(i_v,i_u) or _near_lower(i_v,i_l) or _near_upper(mr_v,mr_u)
            or _near_upper(xb_v,xb_u) or _near_lower(xb_v,xb_l) or _near_upper(r_v,r_u)):
        return "ATENCAO"
    return "ESTAVEL"


def build_spc_lookup(df: pd.DataFrame) -> dict:
    cols = ["date","ticker","i_value","i_ucl","i_lcl","mr_value","mr_ucl",
            "xbar_value","xbar_ucl","xbar_lcl","r_value","r_ucl"]
    lookup: dict = {}
    for _, row in df[cols].iterrows():
        d = pd.Timestamp(row["date"]).normalize()
        t = str(row["ticker"]).upper().strip()
        lookup[(d, t)] = classify_spc_status(row)
    return lookup


def build_rank_lookup(scores_by_day: dict) -> dict:
    lookup: dict = {}
    for d, scores in scores_by_day.items():
        d_ts = pd.Timestamp(d).normalize()
        for ticker, row in scores.iterrows():
            t = str(ticker).upper().strip()
            lookup[(d_ts, t)] = {
                "m3_rank": float(row.get("m3_rank", np.nan)),
                "score_m3": float(row.get("score_m3", np.nan)),
            }
    return lookup


def build_mc_lookup(df: pd.DataFrame) -> dict:
    return {
        (pd.Timestamp(row["date"]).normalize(), str(row["ticker"]).upper().strip()): float(row["market_cap"])
        for _, row in df[["date","ticker","market_cap"]].iterrows()
        if _is_finite(float(row["market_cap"]))
    }


def to_split(day: pd.Timestamp) -> str:
    if day <= TRAIN_END:
        return "TRAIN"
    if day >= HOLDOUT_START:
        return "HOLDOUT"
    return "OTHER"


def metric_mean(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce")
    return float(s.mean()) if s.notna().any() else float("nan")


def metric_std(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce")
    return float(s.std(ddof=0)) if s.notna().any() else float("nan")


def aggregate_summary(df: pd.DataFrame) -> pd.DataFrame:
    use = df[df["signal"].isin(VALID_SIGNALS)].copy()
    if use.empty:
        return pd.DataFrame()
    rows = []
    for (sg, sig, lb), g in use.groupby(["sample_group","signal","lookback_L"], dropna=False):
        row: dict = {"sample_group": sg, "signal": sig, "lookback_L": int(lb),
                     "n": int(len(g)), "in_top_n_next_rate": metric_mean(g["in_top_n_next"])}
        for k in HORIZONS:
            row[f"became_instavel_{k}_rate"] = metric_mean(g[f"became_instavel_{k}"])
            row[f"log_ret_{k}_mean"] = metric_mean(g[f"log_ret_{k}"])
            row[f"log_ret_{k}_std"]  = metric_std(g[f"log_ret_{k}"])
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["sample_group","lookback_L","signal"]).reset_index(drop=True)


def build_phase_sweep_stats(df: pd.DataFrame) -> dict:
    metrics = (["in_top_n_next_rate"]
               + [f"became_instavel_{k}_rate" for k in HORIZONS]
               + [f"log_ret_{k}_mean" for k in HORIZONS])
    use = df[df["signal"].isin(VALID_SIGNALS)].copy()
    if use.empty:
        return {"meta": {"notes": "Sem dados."}, "stats": []}
    phase_rows = []
    for (ph,sg,sig,lb), g in use.groupby(["phase","sample_group","signal","lookback_L"], dropna=False):
        row: dict = {"phase": int(ph), "sample_group": sg, "signal": sig,
                     "lookback_L": int(lb), "n": int(len(g)),
                     "in_top_n_next_rate": metric_mean(g["in_top_n_next"])}
        for k in HORIZONS:
            row[f"became_instavel_{k}_rate"] = metric_mean(g[f"became_instavel_{k}"])
            row[f"log_ret_{k}_mean"] = metric_mean(g[f"log_ret_{k}"])
        phase_rows.append(row)
    phase_df = pd.DataFrame(phase_rows)
    stats_rows = []
    for (sg,sig,lb), g in phase_df.groupby(["sample_group","signal","lookback_L"], dropna=False):
        for metric in metrics:
            vals = pd.to_numeric(g[metric], errors="coerce").dropna()
            stats_rows.append({"sample_group": sg, "signal": sig, "lookback_L": int(lb),
                                "metric": metric, "count_phases": int(len(vals)),
                                "mean": float(vals.mean()) if len(vals) else float("nan"),
                                "std":  float(vals.std(ddof=0)) if len(vals) else float("nan"),
                                "min":  float(vals.min()) if len(vals) else float("nan"),
                                "max":  float(vals.max()) if len(vals) else float("nan")})
    return {"meta": {"cadence": 10, "phase_offsets": list(range(10)),
                     "horizons": HORIZONS, "lookbacks": LOOKBACKS, "signals": VALID_SIGNALS},
            "stats": stats_rows}


def compute_verdict(summary_holdout_topall: pd.DataFrame) -> dict:
    vc = VERDICT_CRITERIA
    lb = vc["primary_lookback_L"]; hz = vc["primary_horizon"]
    sg = vc["primary_sample_group"]; min_n = vc["min_n_per_bucket"]
    sub = summary_holdout_topall[
        (summary_holdout_topall["lookback_L"] == lb) &
        (summary_holdout_topall["sample_group"] == sg) &
        summary_holdout_topall["signal"].isin(VALID_SIGNALS)
    ].copy()
    if sub.empty:
        return {"verdict": "INCONCLUSIVO", "reason": "Sem dados apos filtragem.", "criteria": vc}
    def get(sig: str, col: str) -> float:
        r = sub[sub["signal"] == sig]
        return float(r[col].iloc[0]) if not r.empty else float("nan")
    inst_r = get("INSTAVEL", f"became_instavel_{hz}_rate")
    est_r  = get("ESTAVEL",  f"became_instavel_{hz}_rate")
    inst_l = get("INSTAVEL", f"log_ret_{hz}_mean")
    est_l  = get("ESTAVEL",  f"log_ret_{hz}_mean")
    n_inst = get("INSTAVEL", "n")
    n_est  = get("ESTAVEL",  "n")
    gate1  = _is_finite(inst_r) and _is_finite(est_r) and (inst_r > est_r + 0.05)
    gate2  = _is_finite(inst_l) and _is_finite(est_l) and (inst_l < est_l - 0.015)
    n_ok   = (_is_finite(n_inst) and n_inst >= min_n) and (_is_finite(n_est) and n_est >= min_n)
    if not n_ok:
        verdict = "INCONCLUSIVO"
        reason  = f"Amostra insuficiente: n_INSTAVEL={n_inst:.0f}, n_ESTAVEL={n_est:.0f}, min={min_n}"
    elif gate1 and gate2:
        verdict = "CONFIRMA_SINAL_US"
        reason  = "Ambos gates satisfeitos: INSTAVEL pior em became_instavel e log_ret no HOLDOUT."
    elif not gate1 and not gate2:
        verdict = "NAO_CONFIRMA_SINAL_US"
        reason  = "Nenhum gate satisfeito: sem diferenca material entre buckets."
    elif gate1 and not gate2:
        verdict = "INCONCLUSIVO"
        reason  = "Gate1 passou (became_instavel), gate2 falhou (log_ret): resultado ambiguo."
    else:
        verdict = "INCONCLUSIVO"
        reason  = "Gate2 passou (log_ret), gate1 falhou (became_instavel): resultado ambiguo."
    return {"verdict": verdict, "reason": reason,
            "computed": {"instavel_became_instavel_rate": inst_r, "estavel_became_instavel_rate": est_r,
                         "instavel_log_ret_mean": inst_l, "estavel_log_ret_mean": est_l,
                         "n_instavel": n_inst, "n_estavel": n_est,
                         "gate1_passed": gate1, "gate2_passed": gate2, "n_ok": n_ok},
            "criteria": vc}


def run_observations(
    scores_by_day: dict, rank_lookup: dict, spc_lookup: dict, mc_lookup: dict,
    top_n_portfolio: int, top_n_universe: int,
    cadence: int, anchor_idx: int, trading_days: list, day_to_idx: dict,
    px_wide: pd.DataFrame, blacklist: set, min_mc: float,
) -> pd.DataFrame:

    portfolio_cache: dict = {}
    universe_cache: dict  = {}

    def prev_day(day):
        if day is None:
            return None
        idx = day_to_idx.get(day)
        return trading_days[idx - 1] if idx and idx > 0 else None

    def top_n_for_day(day, n, cache):
        if day is None:
            return []
        key = (day, n)
        if key in cache:
            return cache[key]
        scores = scores_by_day.get(day)
        if scores is None:
            cache[key] = []
            return []
        eligible = [
            t for t in scores.index
            if str(t).upper() not in blacklist
            and _is_finite(mc_lookup.get((day, str(t).upper()), float("nan")))
            and mc_lookup.get((day, str(t).upper()), 0.0) >= min_mc
        ]
        filtered = scores.loc[scores.index.isin(eligible)]
        result = select_top_n(filtered, top_n=n, blacklist=set())
        cache[key] = result
        return result

    observations: list[dict] = []

    for phase in range(cadence):
        rebalance_days = [
            day for i, day in enumerate(trading_days)
            if i >= anchor_idx and ((i - anchor_idx) % cadence) == (phase % cadence)
        ]
        for reb_idx, d_reb in enumerate(rebalance_days):
            if reb_idx < SKIP_INITIAL:
                continue
            d_prev = prev_day(d_reb)
            if d_prev is None:
                continue

            top20_prev  = set(top_n_for_day(d_prev, top_n_portfolio, portfolio_cache))
            top30_prev  = top_n_for_day(d_prev, top_n_universe, universe_cache)
            if not top30_prev:
                continue

            d_next_reb      = rebalance_days[reb_idx+1] if reb_idx+1 < len(rebalance_days) else None
            d_prev_next_reb = prev_day(d_next_reb)
            top20_next      = set(top_n_for_day(d_prev_next_reb, top_n_portfolio, portfolio_cache))

            d_prev_reb      = rebalance_days[reb_idx-1] if reb_idx > 0 else None
            d_prev_prev_reb = prev_day(d_prev_reb)
            prev20_prev     = set(top_n_for_day(d_prev_prev_reb, top_n_portfolio, portfolio_cache))
            prev30_prev     = set(top_n_for_day(d_prev_prev_reb, top_n_universe, universe_cache))

            split = to_split(d_reb)
            if split == "OTHER":
                continue

            idx_reb = day_to_idx[d_reb]
            for ticker in top30_prev:
                ticker_u  = str(ticker).upper()
                spc_st    = spc_lookup.get((d_reb, ticker_u), "ESTAVEL")
                in_top_n_next = (
                    float(int(ticker_u in top20_next))
                    if d_prev_next_reb is not None else float("nan")
                )
                base_px = float(px_wide.at[d_prev, ticker_u]) if ticker_u in px_wide.columns else float("nan")

                inst_map: dict = {}; ret_map: dict = {}
                for hz in HORIZONS:
                    if idx_reb + hz > len(trading_days):
                        inst_map[hz] = float("nan"); ret_map[hz] = float("nan")
                        continue
                    future = trading_days[idx_reb: idx_reb+hz]
                    inst_map[hz] = float(int(any(
                        spc_lookup.get((fd, ticker_u), "ESTAVEL") == "INSTAVEL" for fd in future
                    )))
                    end_px = float(px_wide.at[future[-1], ticker_u]) if ticker_u in px_wide.columns else float("nan")
                    if _is_finite(base_px) and _is_finite(end_px) and base_px > 0 and end_px > 0:
                        ret_map[hz] = float(np.log(end_px / base_px))
                    else:
                        ret_map[hz] = float("nan")

                for lb in LOOKBACKS:
                    rn      = rank_lookup.get((d_prev, ticker_u), {}).get("m3_rank", np.nan)
                    rl_ago  = np.nan
                    idx_pv  = day_to_idx.get(d_prev)
                    if idx_pv is not None and idx_pv - lb >= 0:
                        d_l    = trading_days[idx_pv - lb]
                        rl_ago = rank_lookup.get((d_l, ticker_u), {}).get("m3_rank", np.nan)
                    if _is_finite(rn) and _is_finite(rl_ago):
                        delta  = float(rn - rl_ago)
                        signal = "CAINDO" if delta > 1 else ("SUBINDO" if delta < -1 else "ESTAVEL")
                    else:
                        delta = float("nan"); signal = "N/A"

                    in_top20 = ticker_u in top20_prev
                    sgroups  = ["TOPN_30"]
                    if in_top20:
                        sgroups.append("TOPN_ALL")
                        if ticker_u not in prev30_prev:
                            sgroups.append("IGNITION_TRUE")
                        elif ticker_u not in prev20_prev:
                            sgroups.append("LATERAL_STRENGTH")
                        else:
                            sgroups.append("CONSOLIDADO")
                    else:
                        if ticker_u not in prev30_prev:
                            sgroups.append("CANDIDATE_IGNITION")

                    for sg in sgroups:
                        row: dict = {
                            "phase": phase, "d_reb": d_reb.date().isoformat(),
                            "d_prev": d_prev.date().isoformat(),
                            "d_next_reb": d_next_reb.date().isoformat() if d_next_reb else "",
                            "ticker": ticker_u, "sample_group": sg, "lookback_L": lb,
                            "signal": signal, "delta_rank": delta, "spc_status": spc_st,
                            "split": split, "in_top_n_next": in_top_n_next,
                        }
                        for hz in HORIZONS:
                            row[f"became_instavel_{hz}"] = inst_map[hz]
                            row[f"log_ret_{hz}"]         = ret_map[hz]
                        observations.append(row)

    return pd.DataFrame(observations)


def main() -> None:
    cfg          = read_json(IN_WINNER)["winner_config_snapshot"]
    top_n_port   = int(cfg["top_n"])
    cadence      = int(cfg["rebalance_cadence"])
    anchor_date  = pd.Timestamp(cfg["rebalance_anchor_date"]).normalize()
    min_mc       = float(cfg["min_market_cap"])

    print("Carregando canonical_us.parquet...")
    canonical = pd.read_parquet(IN_CANONICAL)
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"]   = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker","date","close_operational"])

    blacklist = set(canonical[canonical["blacklist_level"] == "HARD"]["ticker"].unique())
    canonical = canonical[canonical["blacklist_level"] != "HARD"].copy()
    print(f"Tickers excluidos (HARD blacklist): {len(blacklist)}")

    # Validacao semantica R-026: confirmar presenca e tipo das 10 colunas SPC
    spc_cols = ["i_value","i_ucl","i_lcl","mr_value","mr_ucl",
                "xbar_value","xbar_ucl","xbar_lcl","r_value","r_ucl"]
    missing = [c for c in spc_cols if c not in canonical.columns]
    if missing:
        raise RuntimeError(f"R-026 FAIL: colunas SPC ausentes em canonical_us: {missing}")
    nan_rates = {c: canonical[c].isna().mean() for c in spc_cols}
    print("Validacao R-026 SPC OK. NaN rates:", {k: f"{v*100:.1f}%" for k,v in nan_rates.items()})
    if not all(c in canonical.columns for c in ["market_cap"]):
        raise RuntimeError("R-026 FAIL: coluna market_cap ausente em canonical_us.")

    print("Construindo lookups...")
    spc_lookup  = build_spc_lookup(canonical)
    mc_lookup   = build_mc_lookup(canonical)

    px_wide = (
        canonical.pivot_table(index="date", columns="ticker",
                               values="close_operational", aggfunc="first")
        .sort_index().ffill()
    )
    trading_days = list(px_wide.index)
    day_to_idx   = {d: i for i, d in enumerate(trading_days)}

    if not trading_days:
        raise RuntimeError("Nenhum pregao encontrado.")

    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(np.searchsorted(
            np.array(trading_days, dtype="datetime64[ns]"),
            np.datetime64(anchor_date)
        ))
        anchor_idx = min(pos, len(trading_days) - 1)
        anchor_date = trading_days[anchor_idx]

    min_reb_needed = SKIP_INITIAL + max(LOOKBACKS) + 2
    if (len(trading_days) - anchor_idx) // max(cadence, 1) < min_reb_needed:
        anchor_idx  = 0
        anchor_date = trading_days[0]
        print(f"Aviso: ancora do winner curta para estudo historico; usando {anchor_date.date()}")

    print("Computando scores M3 (pode demorar 5-15 min)...")
    scores_by_day = compute_m3_scores(px_wide)
    print(f"Scores computados para {len(scores_by_day)} pregoes.")

    print("Construindo rank_lookup...")
    rank_lookup = build_rank_lookup(scores_by_day)

    print("Gerando observacoes (phase sweep 10 fases x Top-20 + Top-30)...")
    obs_df = run_observations(
        scores_by_day, rank_lookup, spc_lookup, mc_lookup,
        top_n_port, TOP_N_UNIVERSE, cadence, anchor_idx,
        trading_days, day_to_idx, px_wide, blacklist, min_mc,
    )

    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Estudo Top-20 (paridade T-082 BR) ---
    top20_df = obs_df[obs_df["sample_group"].isin(["TOPN_ALL","CANDIDATE_IGNITION"])].copy()
    top20_df.to_csv(OUT_DIR / "observations_top20.csv", index=False)
    for split_name in ["TRAIN","HOLDOUT","UNION"]:
        sub = top20_df if split_name == "UNION" else top20_df[top20_df["split"] == split_name]
        aggregate_summary(sub).to_csv(OUT_DIR / f"summary_{split_name}_top20.csv", index=False)
    with (OUT_DIR / "phase_sweep_stats_top20.json").open("w", encoding="utf-8") as f:
        json.dump(build_phase_sweep_stats(top20_df), f, ensure_ascii=False, indent=2)

    # --- Extensao Top-30 (paridade T-083 BR) ---
    obs_df.to_csv(OUT_DIR / "observations_top30.csv", index=False)
    for split_name in ["TRAIN","HOLDOUT","UNION"]:
        sub = obs_df if split_name == "UNION" else obs_df[obs_df["split"] == split_name]
        aggregate_summary(sub).to_csv(OUT_DIR / f"summary_{split_name}_top30.csv", index=False)
    with (OUT_DIR / "phase_sweep_stats_top30.json").open("w", encoding="utf-8") as f:
        json.dump(build_phase_sweep_stats(obs_df), f, ensure_ascii=False, indent=2)

    # --- Veredito pre-registrado ---
    holdout_top20_summ = aggregate_summary(top20_df[top20_df["split"] == "HOLDOUT"])
    verdict_data = compute_verdict(holdout_top20_summ)
    with (OUT_DIR / "verdict.json").open("w", encoding="utf-8") as f:
        json.dump(verdict_data, f, ensure_ascii=False, indent=2)

    print(f"\nT-REBALANCE-WEAKNESS-US concluido.")
    print(f"observations_top20={len(top20_df)}, observations_top30={len(obs_df)}")
    print(f"verdict={verdict_data['verdict']}")
    print(f"reason={verdict_data['reason']}")
    print(f"Artefatos em: {OUT_DIR}")


if __name__ == "__main__":
    main()
