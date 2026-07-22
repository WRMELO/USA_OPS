"""DIAGNOSTICO READ-ONLY (nao faz parte do estudo).

Evidencia preservada e referenciada por SALA D-136 / USA D-154 (errata da V1).

Instrumenta o reinvestimento do braco A2 do runner
run_t_defensive_reinvest_policy_us_v1.py para provar, dia a dia, por que o A2
para de recomprar entre 2022 e 2025.

Nao altera o runner e NAO escreve em OUT_DIR (nao sobrescreve artefatos curados).
So imprime evidencia no stdout.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path("/home/wilson/USA_OPS")
sys.path.insert(0, str(ROOT))

import backtest.t_defensive_reinvest_policy_us_v1.run_t_defensive_reinvest_policy_us_v1 as mod  # noqa: E402

rb = mod.rb
prev = mod.prev
prevr037 = mod.prevr037

# ---- coletores de log ----
reinvest_log: list[dict] = []
buy_fail_log: list[dict] = []

_orig_reinvest_a2 = mod._reinvest_a2_policy
_orig_buy = mod._buy_ticker_for_value


def _wrapped_reinvest_a2(**kw):
    prev_scores = kw["prev_scores"]
    blocked = set(kw["blocked"])
    lots = kw["lots"]
    cash_in = float(kw["cash_free"])
    exec_day = kw["exec_day"]
    top_n = int(kw["top_n"])

    held = set(rb.split_lots_by_ticker(lots).keys())
    if prev_scores is None or getattr(prev_scores, "empty", True):
        scores_empty = True
        top20: list[str] = []
    else:
        scores_empty = False
        top20 = list(rb._select_top_n(prev_scores, top_n=top_n, quarantine=set()))

    top20_blocked = len(set(top20) & blocked)
    held_blocked = len(held & blocked)

    out = _orig_reinvest_a2(**kw)
    lots_out, cash_out, gross_buys, trade_cost = out

    reinvest_log.append(
        {
            "date": pd.Timestamp(exec_day).normalize(),
            "scores_empty": bool(scores_empty),
            "cash_in": cash_in,
            "cash_out": float(cash_out),
            "gross_buys": float(gross_buys),
            "n_held": int(len(held)),
            "n_blocked": int(len(blocked)),
            "n_top20": int(len(top20)),
            "top20_in_blocked": int(top20_blocked),
            "held_in_blocked": int(held_blocked),
        }
    )
    return out


def _wrapped_buy(**kw):
    out = _orig_buy(**kw)
    _lots, _cash, _gross, _cost, bought = out
    if int(bought) <= 0:
        px = mod._safe_float(kw["price_row"].get(kw["ticker"], np.nan), np.nan)
        desired = float(kw["desired_value"])
        cash_free = float(kw["cash_free"])
        if not np.isfinite(px) or px <= 0:
            reason = "sem_preco"
        elif desired <= 0 or cash_free <= 0:
            reason = "desired_ou_caixa_zero"
        else:
            max_afford = cash_free / (1.0 + float(kw["friction"]))
            gt = min(desired, max_afford)
            shares = int(gt // px) if px > 0 else 0
            reason = f"shares_zero(desired={desired:.0f},px={px:.2f},shares={shares})"
        buy_fail_log.append(
            {
                "date": pd.Timestamp(kw["exec_day"]).normalize(),
                "ticker": str(kw["ticker"]),
                "reason": reason,
            }
        )
    return out


mod._reinvest_a2_policy = _wrapped_reinvest_a2
mod._buy_ticker_for_value = _wrapped_buy

# ---- replica do setup de main() (read-only) ----
holdout_end, manifest = prev._load_holdout_end_from_manifest(mod.IN_MANIFEST)
cfg = mod._load_winner_snapshot_full(mod.IN_WINNER)
top_n = int(cfg["top_n"])
min_market_cap = float(cfg["min_market_cap"])

required_cols = [
    "ticker", "date", "close_operational", "market_cap",
    "i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl",
    "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl",
]
canonical = pd.read_parquet(mod.IN_CANONICAL, columns=required_cols).copy()
canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
canonical = canonical.dropna(subset=["ticker", "date", "close_operational"]).copy()
canonical = canonical[canonical["date"] <= holdout_end].copy()

blacklist = prev._load_blacklist(mod.IN_BLACKLIST)
if blacklist:
    canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()

spc_blocked_by_day = prev._build_spc_blocked_by_day(canonical)
canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
mc_eligible_by_day = {}
for _dt, _grp in canonical.groupby("date"):
    mc_eligible_by_day[_dt] = set(_grp.loc[_grp["market_cap"] >= min_market_cap, "ticker"].dropna())

px_exec_wide = (
    canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
    .sort_index()
    .ffill()
)
trading_days = list(px_exec_wide.index)
day_to_idx = {d: i for i, d in enumerate(trading_days)}
scores_by_day = prev._compute_scores_by_day(px_exec_wide, holdout_end=holdout_end)

macro = pd.read_parquet(mod.DATASET_DIR / "macro_us.parquet").copy()
macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
macro = macro.dropna(subset=["date", "fed_funds_rate"]).sort_values("date")
cash_log_daily = rb.build_cash_log_daily(macro)

print(f"dias_com_scores(total)={len(scores_by_day)}")
sd = sorted(scores_by_day.keys())
print(f"scores primeira data={pd.Timestamp(sd[0]).date()} ultima={pd.Timestamp(sd[-1]).date()}")

# ---- roda somente o A2 ----
_curve = mod._simulate_arm(
    arm_name="A2",
    trading_days=trading_days,
    day_to_idx=day_to_idx,
    px_exec_wide=px_exec_wide,
    cash_log_daily=cash_log_daily,
    scores_by_day=scores_by_day,
    mc_eligible_by_day=mc_eligible_by_day,
    spc_blocked_by_day=spc_blocked_by_day,
    cfg=cfg,
    blacklist=blacklist,
    holdout_end=holdout_end,
)

log = pd.DataFrame(reinvest_log)
log["ano"] = log["date"].dt.year

print("\n== reinvest A2: por ano ==")
g = log.groupby("ano").agg(
    dias=("date", "count"),
    dias_scores_vazio=("scores_empty", "sum"),
    gross_buys=("gross_buys", "sum"),
    n_blocked_med=("n_blocked", "mean"),
    n_top20_med=("n_top20", "mean"),
    top20_in_blocked_med=("top20_in_blocked", "mean"),
    n_held_med=("n_held", "mean"),
)
print(g.round(2).to_string())

print("\n== amostra de dias 2022-2025 (1 por trimestre) ==")
mid = log[(log.ano >= 2022) & (log.ano <= 2025)].copy()
sample = mid.iloc[::63]
cols = ["date", "scores_empty", "cash_in", "gross_buys", "n_held", "n_blocked", "n_top20", "top20_in_blocked", "held_in_blocked"]
print(sample[cols].to_string(index=False))

if buy_fail_log:
    bf = pd.DataFrame(buy_fail_log)
    bf["ano"] = bf["date"].dt.year
    print("\n== motivos de compra falha (contagem por motivo x ano) ==")
    bf["reason_kind"] = bf["reason"].str.split("(").str[0]
    print(bf.groupby(["ano", "reason_kind"]).size().to_string())
else:
    print("\n== nenhuma chamada de compra retornou bought=0 (buy nunca foi tentado ou sempre comprou) ==")
