from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.band_exp_gate import compute_bandexp_ret62_gate  # noqa: E402
from lib.engine import compute_m3_scores  # noqa: E402

TASK_ID = "T-SDC-MOTOR-BANDEXP-RET62-VETO-US-V1"
DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
OBS_PATH = ROOT / "backtest" / "t_bandexp_ret62_entry_us_v1" / "results" / "observations_bandexp_ret62_entry_us_v1.csv"
OUT_DIR = ROOT / "backtest" / "t_motor_bandexp_ret62_veto_us_v1" / "results"
OUT_PATH = OUT_DIR / "parity_report_v1.json"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_set(raw: object) -> set[str]:
    if raw is None:
        return set()
    text = str(raw).strip()
    if not text or text.lower() == "nan":
        return set()
    return {tok.strip().upper() for tok in text.split(";") if tok.strip()}


def _build_scores_from_canonical(canonical: pd.DataFrame) -> pd.DataFrame:
    px_wide = (
        canonical.pivot_table(
            index="date",
            columns="ticker",
            values="close_operational",
            aggfunc="first",
        )
        .sort_index()
        .ffill()
    )
    scores_by_day = compute_m3_scores(px_wide)
    rows: list[pd.DataFrame] = []
    for d, day_df in scores_by_day.items():
        if day_df.empty:
            continue
        chunk = day_df.reset_index()[["ticker", "ret_62"]].copy()
        chunk["date"] = pd.Timestamp(d).normalize()
        rows.append(chunk)
    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "ret_62"])
    out = pd.concat(rows, ignore_index=True)
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["ret_62"] = pd.to_numeric(out["ret_62"], errors="coerce")
    out = out.dropna(subset=["date", "ticker"]).drop_duplicates(subset=["date", "ticker"], keep="last")
    return out


def main() -> int:
    manifest_path = DATASET_DIR / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifesto ausente: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    files_meta = manifest.get("files", {})
    hash_checks: dict[str, dict[str, object]] = {}
    hashes_ok = True
    for rel_name, meta in files_meta.items():
        p = DATASET_DIR / rel_name
        expected = str((meta or {}).get("sha256", "")).strip().lower()
        actual = _sha256(p).lower() if p.exists() else ""
        ok = bool(expected) and p.exists() and (actual == expected)
        hash_checks[rel_name] = {
            "exists": p.exists(),
            "expected_sha256": expected,
            "actual_sha256": actual,
            "ok": ok,
        }
        hashes_ok = hashes_ok and ok

    canonical_path = DATASET_DIR / "canonical_us.parquet"
    scores_path = DATASET_DIR / "scores_m3_us.parquet"
    if not canonical_path.exists() or not scores_path.exists():
        raise FileNotFoundError("Dataset research_dataset_us_v2 incompleto (canonical/scores ausentes).")

    canonical = pd.read_parquet(
        canonical_path,
        columns=[
            "date",
            "ticker",
            "close_operational",
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
        ],
    ).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical = canonical.dropna(subset=["date", "ticker"]).copy()

    official_scores = pd.read_parquet(scores_path, columns=["date", "ticker", "ret_62"]).copy()
    official_scores["date"] = pd.to_datetime(official_scores["date"], errors="coerce").dt.normalize()
    official_scores["ticker"] = official_scores["ticker"].astype(str).str.upper().str.strip()
    official_scores["ret_62"] = pd.to_numeric(official_scores["ret_62"], errors="coerce")
    official_scores = official_scores.dropna(subset=["date", "ticker"]).drop_duplicates(
        subset=["date", "ticker"], keep="last"
    )

    obs = pd.read_csv(OBS_PATH)
    obs = obs[obs["arm"] == "Arm_BandExpRet62"].copy()
    obs["d_prev"] = pd.to_datetime(obs["d_prev"], errors="coerce").dt.normalize()
    obs = obs.dropna(subset=["d_prev"]).copy()
    obs["tickers_vetados"] = obs["tickers_vetados"].fillna("").astype(str)
    obs_veto = obs[obs["tickers_vetados"].str.strip().ne("")].copy()
    if obs_veto.empty:
        raise RuntimeError("Sem linhas com veto em observations_bandexp_ret62_entry_us_v1.csv")

    required_dates = set(obs_veto["d_prev"].tolist())
    official_dates = set(official_scores["date"].tolist())
    if required_dates.issubset(official_dates):
        scores_source = official_scores.copy()
        scores_source_name = "scores_m3_us.parquet"
    else:
        scores_source = _build_scores_from_canonical(canonical)
        scores_source_name = "recomputed_from_canonical_via_compute_m3_scores"

    scores_day_map: dict[pd.Timestamp, pd.DataFrame] = {}
    for d, grp in scores_source.groupby("date", sort=False):
        scores_day_map[pd.Timestamp(d).normalize()] = grp[["date", "ticker", "ret_62"]].copy()

    mismatches: list[dict[str, object]] = []
    rows_checked = 0
    rows_match = 0

    for _, row in obs_veto.iterrows():
        d_prev = pd.Timestamp(row["d_prev"]).normalize()
        baseline_set = _parse_set(row.get("tickers_baseline"))
        expected_veto_set = _parse_set(row.get("tickers_vetados"))
        scores_day = scores_day_map.get(d_prev, pd.DataFrame(columns=["date", "ticker", "ret_62"]))

        gate_df = compute_bandexp_ret62_gate(
            canonical=canonical,
            scores=scores_day,
            as_of_date=d_prev,
        )
        gate_true = set(
            gate_df.index[gate_df["gate_bandexp_ret62"].fillna(False).astype(bool)].astype(str).str.upper().tolist()
        )
        actual_veto_set = gate_true.intersection(baseline_set)

        rows_checked += 1
        if actual_veto_set == expected_veto_set:
            rows_match += 1
            continue

        mismatches.append(
            {
                "d_prev": d_prev.date().isoformat(),
                "expected": sorted(expected_veto_set),
                "actual": sorted(actual_veto_set),
                "missing_in_actual": sorted(expected_veto_set - actual_veto_set),
                "unexpected_in_actual": sorted(actual_veto_set - expected_veto_set),
            }
        )

    report = {
        "task_id": TASK_ID,
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "dataset_manifest": str(manifest_path.relative_to(ROOT)),
        "hash_checks": hash_checks,
        "hashes_ok": hashes_ok,
        "observations_path": str(OBS_PATH.relative_to(ROOT)),
        "arm": "Arm_BandExpRet62",
        "score_source": scores_source_name,
        "rows_with_veto": int(len(obs_veto)),
        "rows_checked": int(rows_checked),
        "rows_match": int(rows_match),
        "rows_mismatch": int(len(mismatches)),
        "mismatches": mismatches,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Paridade concluída. rows_checked={rows_checked} rows_match={rows_match} mismatches={len(mismatches)}")
    print(f"Relatório: {OUT_PATH}")

    return 0 if (hashes_ok and not mismatches) else 1


if __name__ == "__main__":
    raise SystemExit(main())
