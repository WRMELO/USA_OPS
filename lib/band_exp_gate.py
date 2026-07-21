"""BandExp ∩ ret_62 gate helpers for operational decision."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

REQUIRED_CANONICAL_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "i_value",
    "i_ucl",
    "i_lcl",
    "mr_value",
    "mr_ucl",
    "xbar_value",
    "xbar_ucl",
    "xbar_lcl",
    "r_value",
    "r_ucl",
)
REQUIRED_SCORE_COLUMNS: tuple[str, ...] = ("date", "ticker", "ret_62")
_FEATURE_CACHE: dict[tuple[int, int, int], pd.DataFrame] = {}


def _normalize_ts(v: Any) -> pd.Timestamp:
    ts = pd.Timestamp(v)
    return ts.normalize()


def _normalize_ticker_col(values: Any) -> pd.Series:
    if isinstance(values, pd.Series):
        return values.astype(str).str.upper().str.strip()
    return pd.Series(values).astype(str).str.upper().str.strip()


def _empty_output(index: pd.Index) -> pd.DataFrame:
    out = pd.DataFrame(index=index.copy())
    out.index.name = "ticker"
    out["flag_bandexp"] = False
    out["band_exp20"] = pd.NA
    out["mono20"] = pd.NA
    out["ret_62"] = pd.NA
    out["gate_bandexp_ret62"] = False
    return out


def _build_bandexp_features(canonical: pd.DataFrame) -> pd.DataFrame:
    work = canonical[list(REQUIRED_CANONICAL_COLUMNS)].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["ticker"] = _normalize_ticker_col(work["ticker"])
    for col in [
        "i_value",
        "i_ucl",
        "i_lcl",
        "mr_value",
        "mr_ucl",
        "xbar_value",
        "xbar_ucl",
        "xbar_lcl",
        "r_value",
        "r_ucl",
    ]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["date", "ticker"]).copy()
    if work.empty:
        return pd.DataFrame(columns=["date", "ticker", "flag_bandexp", "band_exp20", "mono20"])

    work = work.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = work.groupby("ticker", sort=False)

    work["width"] = work["i_ucl"] - work["i_lcl"]
    work["width_20ago"] = g["width"].shift(20)
    work["band_exp20"] = work["width"] / work["width_20ago"] - 1.0
    work["w_diff_pos"] = (g["width"].diff() > 0).astype(float)
    work["mono20"] = g["w_diff_pos"].transform(lambda s: s.rolling(20, min_periods=20).mean())

    work["w_pct_dia"] = work.groupby("date", sort=False)["width"].rank(pct=True)
    work["w_ter"] = pd.cut(
        work["w_pct_dia"],
        [0.0, 1.0 / 3.0, 2.0 / 3.0, 1.0],
        labels=["ESTREITA", "MEDIA", "LARGA"],
        include_lowest=True,
    )
    work["exp_pct_dia"] = work.groupby("date", sort=False)["band_exp20"].rank(pct=True)

    work["viol_any"] = (
        (work["i_value"] > work["i_ucl"])
        | (work["i_value"] < work["i_lcl"])
        | (work["mr_value"] > work["mr_ucl"])
        | (work["r_value"] > work["r_ucl"])
        | (work["xbar_value"] > work["xbar_ucl"])
        | (work["xbar_value"] < work["xbar_lcl"])
    ).fillna(False)

    work["flag_bandexp"] = (
        (work["w_ter"] == "LARGA")
        & (pd.to_numeric(work["exp_pct_dia"], errors="coerce") >= 0.80)
        & (pd.to_numeric(work["mono20"], errors="coerce") >= 0.65)
        & (~work["viol_any"])
    ).fillna(False)

    return work[["date", "ticker", "flag_bandexp", "band_exp20", "mono20"]].copy()


def _get_bandexp_features(canonical: pd.DataFrame) -> pd.DataFrame:
    key = (id(canonical), int(canonical.shape[0]), int(canonical.shape[1]))
    cached = _FEATURE_CACHE.get(key)
    if cached is not None:
        return cached
    out = _build_bandexp_features(canonical)
    _FEATURE_CACHE[key] = out
    return out


def compute_bandexp_ret62_gate(
    canonical: pd.DataFrame,
    scores: pd.DataFrame,
    as_of_date: date | datetime | pd.Timestamp | str,
) -> pd.DataFrame:
    """Compute operational gate for BandExp ∩ ret_62>=1.00 at as_of_date.

    Fail-open by design: insufficient/missing SPC history never becomes a veto.
    """
    as_of_ts = _normalize_ts(as_of_date)

    if scores is None or not {"date", "ticker"}.issubset(set(scores.columns)):
        return _empty_output(pd.Index([], dtype="object"))

    scores_work = scores.copy()
    scores_work["date"] = pd.to_datetime(scores_work["date"], errors="coerce").dt.normalize()
    scores_work["ticker"] = _normalize_ticker_col(scores_work["ticker"])
    if "ret_62" in scores_work.columns:
        scores_work["ret_62"] = pd.to_numeric(scores_work["ret_62"], errors="coerce")
    else:
        scores_work["ret_62"] = pd.NA
    scores_work = scores_work.dropna(subset=["date", "ticker"]).copy()
    scores_work = scores_work[scores_work["date"] <= as_of_ts].copy()

    scores_day = scores_work[scores_work["date"] == as_of_ts][["ticker", "ret_62"]].drop_duplicates(
        subset=["ticker"], keep="last"
    )
    tickers = pd.Index(sorted(scores_day["ticker"].astype(str).tolist()), dtype="object")
    out = _empty_output(tickers)

    if out.empty:
        return out

    out["ret_62"] = scores_day.set_index("ticker")["ret_62"].reindex(out.index)

    if canonical is None:
        out["gate_bandexp_ret62"] = (
            pd.to_numeric(out["ret_62"], errors="coerce") >= 1.0
        ) & out["flag_bandexp"].fillna(False)
        return out

    missing_cols = sorted(set(REQUIRED_CANONICAL_COLUMNS) - set(canonical.columns))
    if missing_cols:
        out["gate_bandexp_ret62"] = (
            pd.to_numeric(out["ret_62"], errors="coerce") >= 1.0
        ) & out["flag_bandexp"].fillna(False)
        return out

    work = _get_bandexp_features(canonical)
    work = work[work["date"] <= as_of_ts].copy()
    if work.empty:
        out["gate_bandexp_ret62"] = (
            pd.to_numeric(out["ret_62"], errors="coerce") >= 1.0
        ) & out["flag_bandexp"].fillna(False)
        return out

    day_rows = work[work["date"] == as_of_ts][["ticker", "flag_bandexp", "band_exp20", "mono20"]].copy()
    day_rows = day_rows.drop_duplicates(subset=["ticker"], keep="last").set_index("ticker")

    out["flag_bandexp"] = day_rows["flag_bandexp"].reindex(out.index).fillna(False).astype(bool)
    out["band_exp20"] = pd.to_numeric(day_rows["band_exp20"].reindex(out.index), errors="coerce")
    out["mono20"] = pd.to_numeric(day_rows["mono20"].reindex(out.index), errors="coerce")
    out["ret_62"] = pd.to_numeric(out["ret_62"], errors="coerce")
    out["gate_bandexp_ret62"] = ((out["ret_62"] >= 1.0) & out["flag_bandexp"]).fillna(False).astype(bool)
    return out
