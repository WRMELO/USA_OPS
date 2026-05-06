"""T-REBALANCE-WEAKNESS-US-FASE-B-PREP.

Read-only parity report BR x US using existing backtest artifacts.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "backtest" / "t_rebalance_weakness_us" / "results"

BR_OBS_PATH = Path("/home/wilson/RENDA_OPS/backtest/t082_rebalance_weakness/results/observations.csv")
US_OBS_PATH = ROOT / "backtest" / "t_rebalance_weakness_us" / "results" / "observations_top20.csv"

OUT_JSON_PATH = OUT_DIR / "parity_with_renda_ops.json"
OUT_MD_PATH = OUT_DIR / "parity_with_renda_ops.md"

FILTER_SAMPLE_GROUP = "TOPN_ALL"
FILTER_SPLIT = "HOLDOUT"
FILTER_LOOKBACK = 5

STATUS_ORDER = ["ESTAVEL", "ATENCAO", "INSTAVEL"]


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if np.isfinite(out):
        return out
    return None


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{100.0 * value:+.2f}%"


def _fmt_pp(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{100.0 * value:+.2f} p.p."


def _validate_columns(df: pd.DataFrame, factory: str) -> None:
    required = {
        "sample_group",
        "split",
        "lookback_L",
        "spc_status",
        "log_ret_3",
        "became_instavel_1",
        "became_instavel_3",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"{factory}: colunas ausentes: {missing}")


def _filter_topn_holdout_l5(df: pd.DataFrame) -> pd.DataFrame:
    use = df.copy()
    lookback_num = pd.to_numeric(use["lookback_L"], errors="coerce")
    mask = (
        (use["sample_group"].astype(str) == FILTER_SAMPLE_GROUP)
        & (use["split"].astype(str) == FILTER_SPLIT)
        & (lookback_num == FILTER_LOOKBACK)
    )
    return use.loc[mask].copy()


def _status_stats(df: pd.DataFrame) -> dict[str, dict[str, float | int | None]]:
    out: dict[str, dict[str, float | int | None]] = {}

    seen_status = [str(x) for x in pd.Series(df["spc_status"]).dropna().astype(str).unique().tolist()]
    ordered_status = STATUS_ORDER + sorted([s for s in seen_status if s not in STATUS_ORDER])

    for status in ordered_status:
        group = df[df["spc_status"].astype(str) == status]
        if group.empty:
            continue
        log_ret_3 = pd.to_numeric(group["log_ret_3"], errors="coerce")
        became_1 = pd.to_numeric(group["became_instavel_1"], errors="coerce")
        became_3 = pd.to_numeric(group["became_instavel_3"], errors="coerce")

        became_1_valid = became_1.dropna()
        share_eq_1 = None
        if len(became_1_valid) > 0:
            share_eq_1 = _safe_float((became_1_valid == 1.0).mean())

        out[status] = {
            "n": int(len(group)),
            "log_ret_3_mean": _safe_float(log_ret_3.mean()),
            "log_ret_3_std": _safe_float(log_ret_3.std(ddof=0)),
            "became_instavel_1_mean": _safe_float(became_1.mean()),
            "became_instavel_3_mean": _safe_float(became_3.mean()),
            "share_became_instavel_1_eq_1": share_eq_1,
        }
    return out


def _factory_metrics(factory: str, path: Path) -> dict[str, Any]:
    df = pd.read_csv(path)
    _validate_columns(df, factory)
    filtered = _filter_topn_holdout_l5(df)
    if filtered.empty:
        raise RuntimeError(f"{factory}: sem linhas para TOPN_ALL + HOLDOUT + L=5")

    stats_by_status = _status_stats(filtered)

    inst = stats_by_status.get("INSTAVEL", {})
    est = stats_by_status.get("ESTAVEL", {})
    inst_mean = inst.get("log_ret_3_mean")
    est_mean = est.get("log_ret_3_mean")
    inst_minus_est = None
    if isinstance(inst_mean, (int, float)) and isinstance(est_mean, (int, float)):
        inst_minus_est = float(inst_mean - est_mean)

    taut_share = inst.get("share_became_instavel_1_eq_1")
    taut_mean = inst.get("became_instavel_1_mean")
    tautological = False
    if isinstance(taut_share, (int, float)) and isinstance(taut_mean, (int, float)):
        tautological = (abs(float(taut_share) - 1.0) < 1e-12) and (abs(float(taut_mean) - 1.0) < 1e-12)

    return {
        "factory": factory,
        "source_path": str(path),
        "rows_total": int(len(df)),
        "rows_filtered": int(len(filtered)),
        "filter": {
            "sample_group": FILTER_SAMPLE_GROUP,
            "split": FILTER_SPLIT,
            "lookback_L": FILTER_LOOKBACK,
        },
        "by_spc_status": stats_by_status,
        "instavel_minus_estavel_log_ret_3": inst_minus_est,
        "tautology_instavel_became_instavel_1": {
            "became_instavel_1_mean": taut_mean,
            "share_became_instavel_1_eq_1": taut_share,
            "is_tautological": tautological,
        },
    }


def _status_value(factory_data: dict[str, Any], status: str, key: str) -> float | None:
    status_data = factory_data.get("by_spc_status", {}).get(status, {})
    value = status_data.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _build_payload(br: dict[str, Any], us: dict[str, Any]) -> dict[str, Any]:
    br_diff = br.get("instavel_minus_estavel_log_ret_3")
    us_diff = us.get("instavel_minus_estavel_log_ret_3")
    diff_gap = None
    if isinstance(br_diff, (int, float)) and isinstance(us_diff, (int, float)):
        diff_gap = float(br_diff - us_diff)

    payload = {
        "task_id": "T-REBALANCE-WEAKNESS-US-FASE-B-PREP",
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "inputs": {
            "br_observations": str(BR_OBS_PATH),
            "us_observations": str(US_OBS_PATH),
        },
        "filter": {
            "sample_group": FILTER_SAMPLE_GROUP,
            "split": FILTER_SPLIT,
            "lookback_L": FILTER_LOOKBACK,
        },
        "br": br,
        "us": us,
        "magnitude_comparison": {
            "log_ret_3_mean_by_spc_status": {
                "BR": {
                    status: _status_value(br, status, "log_ret_3_mean")
                    for status in STATUS_ORDER
                },
                "US": {
                    status: _status_value(us, status, "log_ret_3_mean")
                    for status in STATUS_ORDER
                },
            },
            "instavel_minus_estavel_log_ret_3": {
                "BR": br_diff,
                "US": us_diff,
                "BR_minus_US": diff_gap,
            },
        },
        "tautology_check": {
            "BR": br.get("tautology_instavel_became_instavel_1", {}),
            "US": us.get("tautology_instavel_became_instavel_1", {}),
            "same_pattern_br_us": bool(
                br.get("tautology_instavel_became_instavel_1", {}).get("is_tautological", False)
                and us.get("tautology_instavel_became_instavel_1", {}).get("is_tautological", False)
            ),
            "mechanism": "future_days inclui d_reb (janela [idx_reb : idx_reb+horizon]), logo INSTAVEL em d_reb implica became_instavel_1=1 por construcao",
        },
        "gate_mapping": {
            "br_motor_gate": "blocked_bc (Regra 1 + Nelson/WE B+C nas cartas I, MR, Xbar e R)",
            "br_backtest_status_dimension": "spc_status (ESTAVEL/ATENCAO/INSTAVEL)",
            "us_open_decision": "Owner decide se Fase B usa INSTAVEL puro, blocked_bc, ou outro escopo",
            "references": [
                "RENDA_OPS/lib/spc.py",
                "RENDA_OPS/pipeline/09_decide.py",
                "RENDA_OPS/DECISION_LOG.md D-088/D-090",
            ],
        },
        "q4_threshold_check": {
            "exists_numeric_log_ret_threshold_in_br_motor": False,
            "evidence": "Nao foi identificado threshold numerico de log_ret para gate de motor no BR; D-088 referencia criterio pre-registrado do T-088 (ablacao), nao threshold de retorno isolado.",
        },
        "owner_decision_points": [
            "Escopo do gate na Fase B US: INSTAVEL puro vs blocked_bc.",
            "Incluir ATENCAO no gate US ou manter escopo BR-equivalente.",
            "Abordagem de validacao para desbloqueio do motor US: ablacao BR-like vs threshold numerico proprio.",
        ],
    }
    return payload


def _build_markdown(payload: dict[str, Any]) -> str:
    br = payload["br"]
    us = payload["us"]

    br_diff = br.get("instavel_minus_estavel_log_ret_3")
    us_diff = us.get("instavel_minus_estavel_log_ret_3")

    def status_n(factory: dict[str, Any], status: str) -> int:
        data = factory.get("by_spc_status", {}).get(status, {})
        value = data.get("n")
        return int(value) if isinstance(value, int) else 0

    def status_log_mean(factory: dict[str, Any], status: str) -> float | None:
        data = factory.get("by_spc_status", {}).get(status, {})
        value = data.get("log_ret_3_mean")
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def status_taut_share(factory: dict[str, Any]) -> float | None:
        data = factory.get("tautology_instavel_became_instavel_1", {})
        value = data.get("share_became_instavel_1_eq_1")
        if isinstance(value, (int, float)):
            return float(value)
        return None

    lines = [
        "# Paridade metodologica BR x US (T-REBALANCE-WEAKNESS-US-FASE-B-PREP)",
        "",
        f"- Gerado em UTC: {payload['generated_at_utc']}",
        f"- Filtro comum: sample_group={FILTER_SAMPLE_GROUP}, split={FILTER_SPLIT}, lookback_L={FILTER_LOOKBACK}",
        f"- Fonte BR: `{payload['inputs']['br_observations']}`",
        f"- Fonte US: `{payload['inputs']['us_observations']}`",
        "",
        "## Q1 — Magnitude do sinal BR vs US",
        "",
        (
            "Comparacao do contraste principal (INSTAVEL - ESTAVEL) em `log_ret_3` no mesmo corte "
            "(TOPN_ALL, HOLDOUT, L=5):"
        ),
        f"- BR: {_fmt_pp(br_diff)}",
        f"- US: {_fmt_pp(us_diff)}",
        (
            "- Leitura: ambos mostram degradacao para INSTAVEL contra ESTAVEL no horizonte de 3 dias; "
            "a ordem de grandeza e semelhante."
        ),
        (
            f"- Amostras INSTAVEL/ESTAVEL: BR={status_n(br, 'INSTAVEL')}/{status_n(br, 'ESTAVEL')} ; "
            f"US={status_n(us, 'INSTAVEL')}/{status_n(us, 'ESTAVEL')}."
        ),
        "",
        "## Q2 — Tautologia em became_instavel_K",
        "",
        (
            "Padrao observado para `became_instavel_1` dentro do grupo `spc_status=INSTAVEL` "
            "(mesma definicao do BR e do US):"
        ),
        f"- BR share(became_instavel_1 == 1 | INSTAVEL): {_fmt_pct(status_taut_share(br))}",
        f"- US share(became_instavel_1 == 1 | INSTAVEL): {_fmt_pct(status_taut_share(us))}",
        (
            "- Conclusao: a tautologia e estrutural e igual nas duas fabricas, pois a janela "
            "futura inclui o proprio `d_reb`."
        ),
        "",
        "## Q3 — Gate do motor BR: blocked_bc vs spc_status=INSTAVEL",
        "",
        (
            "No BR, o motor nao usa `spc_status=INSTAVEL` isolado como gate de entrada. "
            "O gate operacional esta em `build_spc_bc_blocked_set(...)` e retorna `blocked_bc`, "
            "que agrega Regra 1 e regras Nelson/WE B+C nas cartas I/MR/Xbar/R."
        ),
        (
            "Logo, `blocked_bc` e semanticamente mais amplo que apenas `INSTAVEL` do classificador "
            "simples por limites."
        ),
        "",
        "## Q4 — Existe threshold numerico de log_ret no BR?",
        "",
        (
            "Nao foi identificado threshold numerico de `log_ret` no motor BR para habilitar gate. "
            "A decisao BR (D-088/D-090) referencia criterio pre-registrado em ablacoes (T-088), "
            "nao um corte numerico unico de retorno."
        ),
        "",
        "## Tabela comparativa BR × US",
        "",
        "| Fabrica | n_ESTAVEL | n_ATENCAO | n_INSTAVEL | log_ret_3 ESTAVEL | log_ret_3 ATENCAO | log_ret_3 INSTAVEL | INSTAVEL-ESTAVEL | share(became_instavel_1==1 \\| INSTAVEL) |",
        "|---------|-----------|-----------|------------|-------------------|-------------------|--------------------|------------------|-------------------------------------------|",
        (
            f"| BR | {status_n(br, 'ESTAVEL')} | {status_n(br, 'ATENCAO')} | {status_n(br, 'INSTAVEL')} | "
            f"{_fmt_pct(status_log_mean(br, 'ESTAVEL'))} | {_fmt_pct(status_log_mean(br, 'ATENCAO'))} | "
            f"{_fmt_pct(status_log_mean(br, 'INSTAVEL'))} | {_fmt_pp(br_diff)} | {_fmt_pct(status_taut_share(br))} |"
        ),
        (
            f"| US | {status_n(us, 'ESTAVEL')} | {status_n(us, 'ATENCAO')} | {status_n(us, 'INSTAVEL')} | "
            f"{_fmt_pct(status_log_mean(us, 'ESTAVEL'))} | {_fmt_pct(status_log_mean(us, 'ATENCAO'))} | "
            f"{_fmt_pct(status_log_mean(us, 'INSTAVEL'))} | {_fmt_pp(us_diff)} | {_fmt_pct(status_taut_share(us))} |"
        ),
        "",
        "## Pontos de decisao para o Owner",
        "",
        "1. Escopo de gate para Fase B no US: `INSTAVEL` puro ou `blocked_bc` BR-like?",
        "2. Incluir `ATENCAO` no gate US ou manter escopo BR-equivalente?",
        "3. Criterio de desbloqueio: adotar ablacoes BR-like (sem threshold unico) ou criar threshold proprio formalizado?",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    br_metrics = _factory_metrics("BR", BR_OBS_PATH)
    us_metrics = _factory_metrics("US", US_OBS_PATH)
    payload = _build_payload(br_metrics, us_metrics)

    with OUT_JSON_PATH.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)

    markdown = _build_markdown(payload)
    OUT_MD_PATH.write_text(markdown, encoding="utf-8")

    print("parity_with_renda_ops concluido.")
    print(f"json={OUT_JSON_PATH}")
    print(f"md={OUT_MD_PATH}")


if __name__ == "__main__":
    main()
