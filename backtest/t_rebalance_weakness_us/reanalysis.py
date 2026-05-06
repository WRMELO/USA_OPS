"""Reanalise Fase A de T-REBALANCE-WEAKNESS-US.

Le os artifacts existentes em results/ (sem rerun do backtest),
reconstroi matriz honesta por spc_status x signal, evidencia bug semantico
do veredito e escreve:
  - reanalysis_report.json
  - reanalysis_report.md
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "backtest" / "t_rebalance_weakness_us" / "results"
IN_TOP20 = RESULTS_DIR / "observations_top20.csv"
IN_TOP30 = RESULTS_DIR / "observations_top30.csv"
OUT_JSON = RESULTS_DIR / "reanalysis_report.json"
OUT_MD = RESULTS_DIR / "reanalysis_report.md"
LOOKBACKS = [1, 2, 3, 5, 10]


def _to_native(v: Any) -> Any:
    if isinstance(v, (np.floating, np.integer)):
        return v.item()
    if isinstance(v, float):
        if np.isnan(v):
            return None
        return v
    return v


def _safe_mean(s: pd.Series) -> float:
    s_num = pd.to_numeric(s, errors="coerce")
    return float(s_num.mean()) if s_num.notna().any() else float("nan")


def _safe_std(s: pd.Series) -> float:
    s_num = pd.to_numeric(s, errors="coerce")
    return float(s_num.std(ddof=0)) if s_num.notna().any() else float("nan")


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    vals = pd.to_numeric(values, errors="coerce")
    wts = pd.to_numeric(weights, errors="coerce")
    mask = vals.notna() & wts.notna()
    if not mask.any():
        return float("nan")
    ww = wts[mask]
    if float(ww.sum()) == 0.0:
        return float("nan")
    return float(np.average(vals[mask], weights=ww))


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        rows.append({k: _to_native(v) for k, v in rec.items()})
    return rows


def cross_tab(
    df: pd.DataFrame,
    sample_group_filter: str | None = None,
    split_filter: str | None = None,
    lookback_L_filter: int | None = None,
) -> pd.DataFrame:
    use = df.copy()
    if sample_group_filter is not None:
        use = use[use["sample_group"] == sample_group_filter]
    if split_filter is not None:
        use = use[use["split"] == split_filter]
    if lookback_L_filter is not None:
        use = use[use["lookback_L"] == int(lookback_L_filter)]

    if use.empty:
        return pd.DataFrame(
            columns=[
                "spc_status",
                "signal",
                "n",
                "in_top_n_next_rate",
                "became_instavel_1_rate",
                "became_instavel_3_rate",
                "became_instavel_5_rate",
                "log_ret_1_mean",
                "log_ret_3_mean",
                "log_ret_5_mean",
                "log_ret_3_std",
            ]
        )

    grouped = (
        use.groupby(["spc_status", "signal"], dropna=False)
        .agg(
            n=("ticker", "count"),
            in_top_n_next_rate=("in_top_n_next", _safe_mean),
            became_instavel_1_rate=("became_instavel_1", _safe_mean),
            became_instavel_3_rate=("became_instavel_3", _safe_mean),
            became_instavel_5_rate=("became_instavel_5", _safe_mean),
            log_ret_1_mean=("log_ret_1", _safe_mean),
            log_ret_3_mean=("log_ret_3", _safe_mean),
            log_ret_5_mean=("log_ret_5", _safe_mean),
            log_ret_3_std=("log_ret_3", _safe_std),
        )
        .reset_index()
    )
    status_order = {"INSTAVEL": 0, "ATENCAO": 1, "ESTAVEL": 2}
    signal_order = {"CAINDO": 0, "ESTAVEL": 1, "SUBINDO": 2}
    grouped["_status_ord"] = grouped["spc_status"].map(status_order).fillna(9)
    grouped["_signal_ord"] = grouped["signal"].map(signal_order).fillna(9)
    grouped = grouped.sort_values(["_status_ord", "_signal_ord"]).drop(
        columns=["_status_ord", "_signal_ord"]
    )
    return grouped.reset_index(drop=True)


def tautology_check(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "spc_status",
                "n_total",
                "mean_became_instavel_1",
                "mean_became_instavel_3",
                "share_became_instavel_1_eq_1",
                "share_became_instavel_3_eq_1",
                "log_ret_3_mean",
                "log_ret_3_std",
            ]
        )

    rows: list[dict[str, Any]] = []
    for spc, grp in df.groupby("spc_status", dropna=False):
        b1 = pd.to_numeric(grp["became_instavel_1"], errors="coerce")
        b3 = pd.to_numeric(grp["became_instavel_3"], errors="coerce")
        rows.append(
            {
                "spc_status": spc,
                "n_total": int(len(grp)),
                "mean_became_instavel_1": _safe_mean(b1),
                "mean_became_instavel_3": _safe_mean(b3),
                "share_became_instavel_1_eq_1": float((b1 == 1.0).mean())
                if b1.notna().any()
                else float("nan"),
                "share_became_instavel_3_eq_1": float((b3 == 1.0).mean())
                if b3.notna().any()
                else float("nan"),
                "log_ret_3_mean": _safe_mean(grp["log_ret_3"]),
                "log_ret_3_std": _safe_std(grp["log_ret_3"]),
            }
        )
    out = pd.DataFrame(rows)
    ord_map = {"INSTAVEL": 0, "ATENCAO": 1, "ESTAVEL": 2}
    out["_ord"] = out["spc_status"].map(ord_map).fillna(9)
    out = out.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)
    return out


def apply_gates(df_cross: pd.DataFrame) -> dict[str, Any]:
    needed = ["INSTAVEL", "ESTAVEL"]
    collapsed: dict[str, dict[str, float]] = {}
    for spc in needed:
        sub = df_cross[df_cross["spc_status"] == spc].copy()
        if sub.empty:
            collapsed[spc] = {
                "n": float("nan"),
                "became_instavel_3_rate": float("nan"),
                "log_ret_3_mean": float("nan"),
            }
            continue
        n_sum = float(sub["n"].sum())
        collapsed[spc] = {
            "n": n_sum,
            "became_instavel_3_rate": _weighted_mean(
                sub["became_instavel_3_rate"], sub["n"]
            ),
            "log_ret_3_mean": _weighted_mean(sub["log_ret_3_mean"], sub["n"]),
        }

    inst = collapsed["INSTAVEL"]
    est = collapsed["ESTAVEL"]
    gate1 = (
        np.isfinite(inst["became_instavel_3_rate"])
        and np.isfinite(est["became_instavel_3_rate"])
        and inst["became_instavel_3_rate"] > est["became_instavel_3_rate"] + 0.05
    )
    gate2 = (
        np.isfinite(inst["log_ret_3_mean"])
        and np.isfinite(est["log_ret_3_mean"])
        and inst["log_ret_3_mean"] < est["log_ret_3_mean"] - 0.015
    )
    diff_log_ret_3 = (
        inst["log_ret_3_mean"] - est["log_ret_3_mean"]
        if np.isfinite(inst["log_ret_3_mean"]) and np.isfinite(est["log_ret_3_mean"])
        else float("nan")
    )
    return {
        "criteria": {
            "gate1": "became_instavel_3_rate(INSTAVEL) > became_instavel_3_rate(ESTAVEL) + 0.05",
            "gate2": "log_ret_3_mean(INSTAVEL) < log_ret_3_mean(ESTAVEL) - 0.015",
        },
        "instavel": inst,
        "estavel": est,
        "diff_log_ret_3_instavel_minus_estavel": diff_log_ret_3,
        "gate1_passed": bool(gate1),
        "gate2_passed": bool(gate2),
        "gate1_tautologico": True,
        "gate1_tautologico_reason": (
            "became_instavel_K considera janela [d_reb, d_reb+K) incluindo d_reb; "
            "para tickers ja INSTAVEL em d_reb, o indicador tende a 1.0 por construcao."
        ),
    }


def _format_pct(v: Any) -> str:
    if v is None:
        return "N/A"
    try:
        x = float(v)
    except Exception:
        return "N/A"
    if np.isnan(x):
        return "N/A"
    return f"{x * 100:.2f}%"


def _render_table(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "_(sem dados)_"
    keep = [c for c in cols if c in df.columns]
    if not keep:
        return "_(sem colunas para render)_"
    head = "| " + " | ".join(keep) + " |\n"
    sep = "| " + " | ".join(["---"] * len(keep)) + " |\n"
    lines = [head, sep]
    for _, row in df[keep].iterrows():
        vals: list[str] = []
        for c in keep:
            v = row[c]
            if isinstance(v, float):
                if np.isnan(v):
                    vals.append("N/A")
                elif "rate" in c or c.endswith("_mean") or c.endswith("_std"):
                    vals.append(f"{v:.6f}")
                else:
                    vals.append(f"{v:.2f}")
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |\n")
    return "".join(lines)


def build_report() -> dict[str, Any]:
    top20 = pd.read_csv(IN_TOP20)
    top30 = pd.read_csv(IN_TOP30)

    matrices: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for name, df in {"top20": top20, "top30": top30}.items():
        matrices[name] = {}
        for split in ["TRAIN", "HOLDOUT", "UNION"]:
            matrices[name][split] = {}
            for lb in LOOKBACKS:
                split_value = None if split == "UNION" else split
                tab = cross_tab(
                    df,
                    sample_group_filter="TOPN_ALL",
                    split_filter=split_value,
                    lookback_L_filter=lb,
                )
                matrices[name][split][f"L{lb}"] = _records(tab)

    primary_holdout_l5_top20 = cross_tab(
        top20,
        sample_group_filter="TOPN_ALL",
        split_filter="HOLDOUT",
        lookback_L_filter=5,
    )
    primary_holdout_l5_top30 = cross_tab(
        top30,
        sample_group_filter="TOPN_ALL",
        split_filter="HOLDOUT",
        lookback_L_filter=5,
    )

    gate_top20 = apply_gates(primary_holdout_l5_top20)
    gate_top30 = apply_gates(primary_holdout_l5_top30)

    taut_top20 = tautology_check(
        top20[
            (top20["sample_group"] == "TOPN_ALL")
            & (top20["split"] == "HOLDOUT")
            & (top20["lookback_L"] == 5)
        ]
    )
    taut_top30 = tautology_check(
        top30[
            (top30["sample_group"] == "TOPN_ALL")
            & (top30["split"] == "HOLDOUT")
            & (top30["lookback_L"] == 5)
        ]
    )

    subgroup_1 = (
        top20[
            (top20["sample_group"] == "TOPN_ALL")
            & (top20["split"] == "HOLDOUT")
            & (top20["signal"] == "ESTAVEL")
            & (top20["spc_status"] == "INSTAVEL")
        ]
        .groupby("lookback_L", dropna=False)
        .agg(
            n=("ticker", "count"),
            became_instavel_3_rate=("became_instavel_3", _safe_mean),
            log_ret_3_mean=("log_ret_3", _safe_mean),
            log_ret_3_std=("log_ret_3", _safe_std),
        )
        .reset_index()
        .sort_values("lookback_L")
    )

    subgroup_2 = (
        top20[
            (top20["sample_group"] == "TOPN_ALL")
            & (top20["split"] == "HOLDOUT")
            & (top20["lookback_L"] == 5)
            & (top20["spc_status"] == "ATENCAO")
        ]
        .groupby("signal", dropna=False)
        .agg(
            n=("ticker", "count"),
            became_instavel_3_rate=("became_instavel_3", _safe_mean),
            log_ret_3_mean=("log_ret_3", _safe_mean),
            log_ret_3_std=("log_ret_3", _safe_std),
        )
        .reset_index()
    )

    subgroup_3 = (
        top30[
            (top30["sample_group"] == "IGNITION_TRUE")
            & (top30["split"] == "HOLDOUT")
            & (top30["spc_status"] == "INSTAVEL")
        ]
        .groupby("lookback_L", dropna=False)
        .agg(
            n=("ticker", "count"),
            became_instavel_3_rate=("became_instavel_3", _safe_mean),
            log_ret_3_mean=("log_ret_3", _safe_mean),
            log_ret_3_std=("log_ret_3", _safe_std),
        )
        .reset_index()
        .sort_values("lookback_L")
    )

    report: dict[str, Any] = {
        "meta": {
            "task_id": "T-REBALANCE-WEAKNESS-US-FASE-A",
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "source_files": [str(IN_TOP20), str(IN_TOP30)],
        },
        "findings": {
            "semantic_bug": {
                "description": (
                    "compute_verdict usa signal=='INSTAVEL' em summaries de rank_trend "
                    "(signal=SUBINDO/ESTAVEL/CAINDO), produzindo n_INSTAVEL=NaN."
                ),
                "evidence_file": "run_t_rebalance_weakness_us.py",
            },
            "tautology_warning": {
                "description": (
                    "became_instavel_K considera janela iniciando em d_reb; para tickers "
                    "ja INSTAVEL em d_reb, a metrica tende a 1.0 por construcao."
                ),
                "top20_holdout_topn_all_l5": _records(taut_top20),
                "top30_holdout_topn_all_l5": _records(taut_top30),
            },
        },
        "matrices": matrices,
        "primary_matrix": {
            "top20_holdout_topn_all_l5": _records(primary_holdout_l5_top20),
            "top30_holdout_topn_all_l5": _records(primary_holdout_l5_top30),
        },
        "gates": {
            "top20_holdout_topn_all_l5": gate_top20,
            "top30_holdout_topn_all_l5": gate_top30,
        },
        "subgroups": {
            "signal_ESTAVEL_x_spc_INSTAVEL_top20_holdout_all_lookbacks": _records(
                subgroup_1
            ),
            "spc_ATENCAO_top20_holdout_l5_all_signals": _records(subgroup_2),
            "sample_group_IGNITION_TRUE_x_spc_INSTAVEL_top30_holdout_all_lookbacks": _records(
                subgroup_3
            ),
        },
        "owner_decision_prompt": (
            "O sinal observado justifica executar a Fase B "
            "(patch + rerun + auditoria dupla) ou a frente deve ser arquivada?"
        ),
    }
    return report


def write_markdown(report: dict[str, Any]) -> None:
    top20_primary = pd.DataFrame(report["primary_matrix"]["top20_holdout_topn_all_l5"])
    top30_primary = pd.DataFrame(report["primary_matrix"]["top30_holdout_topn_all_l5"])
    taut20 = pd.DataFrame(
        report["findings"]["tautology_warning"]["top20_holdout_topn_all_l5"]
    )
    sg1 = pd.DataFrame(
        report["subgroups"]["signal_ESTAVEL_x_spc_INSTAVEL_top20_holdout_all_lookbacks"]
    )
    sg2 = pd.DataFrame(
        report["subgroups"]["spc_ATENCAO_top20_holdout_l5_all_signals"]
    )
    sg3 = pd.DataFrame(
        report["subgroups"][
            "sample_group_IGNITION_TRUE_x_spc_INSTAVEL_top30_holdout_all_lookbacks"
        ]
    )

    gates20 = report["gates"]["top20_holdout_topn_all_l5"]
    lines: list[str] = []
    lines.append("# Reanalise Fase A — T-REBALANCE-WEAKNESS-US\n\n")
    lines.append("## Contexto\n\n")
    lines.append(
        "Reanalise sobre os dados ja gerados (`observations_top20.csv` e "
        "`observations_top30.csv`), sem rerun do backtest.\n\n"
    )

    lines.append("## Bug semantico identificado\n\n")
    lines.append(
        "- O `compute_verdict` procurou `signal == \"INSTAVEL\"` nos summaries.\n"
    )
    lines.append(
        "- Nos summaries, `signal` representa `rank_trend` "
        "(`SUBINDO/ESTAVEL/CAINDO`) e nao `spc_status`.\n"
    )
    lines.append(
        "- Consequencia: `n_INSTAVEL = NaN` e veredito `INCONCLUSIVO` por construcao.\n\n"
    )

    lines.append("## Tautologia em `became_instavel_K`\n\n")
    lines.append(
        "A janela usada para `became_instavel_K` inclui `d_reb`; quando o ticker ja "
        "esta `INSTAVEL` em `d_reb`, a metrica tende a 1.0 por construcao.\n\n"
    )
    lines.append(
        _render_table(
            taut20,
            [
                "spc_status",
                "n_total",
                "mean_became_instavel_1",
                "mean_became_instavel_3",
                "share_became_instavel_1_eq_1",
                "share_became_instavel_3_eq_1",
                "log_ret_3_mean",
            ],
        )
    )
    lines.append("\n")

    lines.append("## Matriz honesta HOLDOUT TOPN_ALL L=5 (primaria: Top-20)\n\n")
    lines.append(
        _render_table(
            top20_primary,
            [
                "spc_status",
                "signal",
                "n",
                "became_instavel_3_rate",
                "log_ret_3_mean",
                "log_ret_3_std",
            ],
        )
    )
    lines.append("\n\n")
    lines.append("### Comparativo Top-30 (mesma janela)\n\n")
    lines.append(
        _render_table(
            top30_primary,
            [
                "spc_status",
                "signal",
                "n",
                "became_instavel_3_rate",
                "log_ret_3_mean",
                "log_ret_3_std",
            ],
        )
    )
    lines.append("\n")

    lines.append("## Aplicacao dos gates pre-registrados (Top-20, HOLDOUT, L=5)\n\n")
    lines.append(
        f"- Gate 1 (`became_instavel_3`): "
        f"{_format_pct(gates20['instavel']['became_instavel_3_rate'])} vs "
        f"{_format_pct(gates20['estavel']['became_instavel_3_rate'])} + 5 p.p. "
        f"=> **{'PASS' if gates20['gate1_passed'] else 'FAIL'}**\n"
    )
    lines.append(
        "- **FLAG TAUTOLOGICO**: Gate 1 nao deve ser tratado como evidencia primaria.\n"
    )
    lines.append(
        f"- Gate 2 (`log_ret_3_mean`): "
        f"{_format_pct(gates20['instavel']['log_ret_3_mean'])} vs "
        f"{_format_pct(gates20['estavel']['log_ret_3_mean'])} - 1.5 p.p. "
        f"=> **{'PASS' if gates20['gate2_passed'] else 'FAIL'}**\n"
    )
    diff = gates20["diff_log_ret_3_instavel_minus_estavel"]
    lines.append(f"- Diferenca observada INSTAVEL-ESTAVEL em `log_ret_3`: {_format_pct(diff)}\n\n")

    lines.append("## Subgrupos obrigatorios\n\n")
    lines.append("### 1) `signal=ESTAVEL x spc_status=INSTAVEL` (Top-20, HOLDOUT)\n\n")
    lines.append(
        _render_table(
            sg1, ["lookback_L", "n", "became_instavel_3_rate", "log_ret_3_mean", "log_ret_3_std"]
        )
    )
    lines.append("\n\n")
    lines.append("### 2) `spc_status=ATENCAO` (Top-20, HOLDOUT, L=5)\n\n")
    lines.append(
        _render_table(
            sg2, ["signal", "n", "became_instavel_3_rate", "log_ret_3_mean", "log_ret_3_std"]
        )
    )
    lines.append("\n\n")
    lines.append("### 3) `sample_group=IGNITION_TRUE x spc_status=INSTAVEL` (Top-30, HOLDOUT)\n\n")
    lines.append(
        _render_table(
            sg3, ["lookback_L", "n", "became_instavel_3_rate", "log_ret_3_mean", "log_ret_3_std"]
        )
    )
    lines.append("\n\n")

    lines.append("## Ponto de Decisao do Owner\n\n")
    lines.append(
        "O sinal observado justifica executar a **Fase B** "
        "(patch + rerun + auditoria dupla) ou a frente deve ser **arquivada**?\n"
    )

    OUT_MD.write_text("".join(lines), encoding="utf-8")


def main() -> None:
    if not IN_TOP20.exists() or not IN_TOP30.exists():
        raise FileNotFoundError(
            "Arquivos de observacao nao encontrados em results/. "
            "Esperado: observations_top20.csv e observations_top30.csv"
        )

    report = build_report()
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(report)
    print(f"[OK] JSON: {OUT_JSON}")
    print(f"[OK] MD:   {OUT_MD}")


if __name__ == "__main__":
    main()
