"""Checagem de valor incremental do gate BandExp vs R-037/volatilidade.

Saida principal:
- incremental_value_band_exp_entry_us_v1.csv
"""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
OBS_PATH = ROOT / "backtest" / "t_band_exp_entry_us_v1" / "results" / "observations_band_exp_entry_us_v1.csv"
OUT_PATH = ROOT / "backtest" / "t_band_exp_entry_us_v1" / "results" / "incremental_value_band_exp_entry_us_v1.csv"


def _ticker_list(raw: str) -> list[str]:
    if not isinstance(raw, str) or not raw.strip():
        return []
    return [x.strip().upper() for x in raw.split(";") if x.strip()]


def _safe_ret(px: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, ticker: str) -> float:
    if start not in px.index or end not in px.index:
        return float("nan")
    if ticker not in px.columns:
        return float("nan")
    p0 = px.at[start, ticker]
    p1 = px.at[end, ticker]
    if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0 or p1 <= 0:
        return float("nan")
    return float(np.log(p1 / p0))


def main() -> None:
    if not OBS_PATH.exists():
        raise RuntimeError(f"Observations nao encontrado: {OBS_PATH}")

    obs = pd.read_csv(OBS_PATH)
    obs = obs[(obs["arm"] == "Arm_BandExp") & (obs["is_holdout"] == 1)].copy()
    if obs.empty:
        raise RuntimeError("Sem observacoes HOLDOUT para Arm_BandExp.")

    obs["d_prev"] = pd.to_datetime(obs["d_prev"], errors="coerce").dt.normalize()
    obs["d_prev_next_reb"] = pd.to_datetime(obs["d_prev_next_reb"], errors="coerce").dt.normalize()
    obs = obs.dropna(subset=["d_prev", "d_prev_next_reb"]).copy()

    canonical = pd.read_parquet(DATASET_DIR / "canonical_us.parquet", columns=["date", "ticker", "close_operational"]).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    px = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index()
        .ffill()
    )

    scores = pd.read_parquet(DATASET_DIR / "scores_m3_us.parquet", columns=["date", "ticker", "ret_62", "vol_62"]).copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()
    scores["ticker"] = scores["ticker"].astype(str).str.upper().str.strip()
    scores["ret_62"] = pd.to_numeric(scores["ret_62"], errors="coerce")
    scores["vol_62"] = pd.to_numeric(scores["vol_62"], errors="coerce")
    scores["vol_pct_dia"] = scores.groupby("date", sort=False)["vol_62"].rank(pct=True)
    scores["vol_top_tercile"] = scores["vol_pct_dia"] >= (2.0 / 3.0)
    scores["r037_severo"] = (scores["ret_62"] >= 1.0) & scores["vol_top_tercile"]
    severe_map = {
        (pd.Timestamp(r.date).normalize(), str(r.ticker).upper().strip()): bool(r.r037_severo)
        for r in scores.itertuples()
    }

    rows: list[dict[str, object]] = []
    for r in obs.itertuples():
        d_prev = pd.Timestamp(r.d_prev).normalize()
        d_end = pd.Timestamp(r.d_prev_next_reb).normalize()
        baseline = _ticker_list(getattr(r, "tickers_baseline", ""))
        flagged = set(_ticker_list(getattr(r, "tickers_vetados", "")))

        for tk in baseline:
            flag_bandexp = tk in flagged
            severe = severe_map.get((d_prev, tk), False)
            ret = _safe_ret(px, start=d_prev, end=d_end, ticker=tk)
            if not np.isfinite(ret):
                continue

            if flag_bandexp and severe:
                grp = "Flag_BandExp+R-037_severo"
            elif flag_bandexp and (not severe):
                grp = "Flag_BandExp_so"
            elif severe and (not flag_bandexp):
                grp = "R-037_severo_so_sem_Flag_BandExp"
            else:
                grp = "Nenhum_dos_dois"

            rows.append(
                {
                    "group": grp,
                    "ticker": tk,
                    "d_prev": d_prev.date().isoformat(),
                    "d_end": d_end.date().isoformat(),
                    "ret_holding_log": ret,
                }
            )

    if not rows:
        raise RuntimeError("Sem linhas validas para incremental_value.")

    df = pd.DataFrame(rows)
    summary = (
        df.groupby("group", as_index=False)
        .agg(
            n=("ret_holding_log", "size"),
            ret_medio_holding_log=("ret_holding_log", "mean"),
            ret_mediano_holding_log=("ret_holding_log", "median"),
            ret_p10_holding_log=("ret_holding_log", lambda s: s.quantile(0.10)),
        )
        .sort_values("group")
    )
    summary.to_csv(OUT_PATH, index=False)
    print("incremental_value concluido.")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:+.6f}"))


if __name__ == "__main__":
    main()
