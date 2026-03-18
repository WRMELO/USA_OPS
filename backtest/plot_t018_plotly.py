"""Plots Plotly da ablação T-018 (C2 vs C4)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "backtest" / "results"


def _load_summary() -> pd.DataFrame:
    path = RESULTS / "t018_ablation_summary.csv"
    if not path.exists():
        raise RuntimeError(f"Arquivo ausente: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise RuntimeError("t018_ablation_summary.csv vazio")
    df["top_n"] = pd.to_numeric(df.get("top_n"), errors="coerce")
    df["rebalance_cadence"] = pd.to_numeric(df.get("rebalance_cadence"), errors="coerce")
    df["buffer_k"] = pd.to_numeric(df.get("buffer_k"), errors="coerce")
    df["k_damp"] = pd.to_numeric(df.get("k_damp"), errors="coerce")
    df["max_weight_cap"] = pd.to_numeric(df.get("max_weight_cap"), errors="coerce")
    return df


def main() -> None:
    df = _load_summary()
    holdout = df[df["split"] == "HOLDOUT"].copy()
    if holdout.empty:
        raise RuntimeError("Sem linhas HOLDOUT no summary T-018.")

    holdout["cfg"] = holdout.apply(
        lambda r: (
            f"{r['variant']} N={int(r['top_n'])} Cad={int(r['rebalance_cadence'])} "
            f"K={int(r['buffer_k'])} kd={r['k_damp']:.2f} cap={r['max_weight_cap']:.2f}"
        ),
        axis=1,
    )

    # 1) Frontier CAGR x MDD por configuração.
    fig1 = px.scatter(
        holdout,
        x="mdd",
        y="cagr",
        color="variant",
        hover_name="cfg",
        size="avg_tickers",
        title="T-018 HOLDOUT - Frontier CAGR x MDD (C2 vs C4)",
        labels={"mdd": "MDD (%)", "cagr": "CAGR (%)", "avg_tickers": "Avg tickers"},
    )
    fig1.add_vline(x=-50, line_dash="dash", line_color="firebrick")
    out1 = RESULTS / "plot_t018_frontier_cagr_mdd.html"
    fig1.write_html(str(out1), include_plotlyjs="cdn")

    # 2) Boxplots de concentração por variante.
    fig2 = px.box(
        holdout,
        x="variant",
        y="max_concentration_pct",
        points="all",
        color="variant",
        hover_data=["top_n", "rebalance_cadence", "buffer_k", "k_damp", "max_weight_cap", "cagr", "mdd"],
        title="T-018 HOLDOUT - Distribuição de Concentração Máxima (%)",
        labels={"max_concentration_pct": "Concentração Máxima (%)"},
    )
    out2 = RESULTS / "plot_t018_box_concentration.html"
    fig2.write_html(str(out2), include_plotlyjs="cdn")

    # 3) Comparação C2 vs C4 na melhor configuração com restrição de MDD <= -50%.
    filt = holdout[holdout["mdd"] >= -50.0].copy()
    if filt.empty:
        filt = holdout.copy()
    c2_best = filt[filt["variant"] == "C2"].sort_values("cagr", ascending=False).head(1)
    c4_best = filt[filt["variant"] == "C4"].sort_values("cagr", ascending=False).head(1)
    pair = pd.concat([c2_best, c4_best], ignore_index=True)
    if pair.empty:
        pair = holdout.sort_values("cagr", ascending=False).head(2)

    fig3 = go.Figure()
    fig3.add_trace(
        go.Bar(
            x=pair["variant"],
            y=pair["cagr"],
            name="CAGR (%)",
            marker_color="#2563eb",
            yaxis="y",
            customdata=pair[["mdd", "max_concentration_pct", "top_n", "rebalance_cadence", "buffer_k", "k_damp", "max_weight_cap"]].values,
            hovertemplate=(
                "Variante=%{x}<br>CAGR=%{y:.2f}%<br>MDD=%{customdata[0]:.2f}%<br>"
                "Concentração=%{customdata[1]:.2f}%<br>TopN=%{customdata[2]} Cad=%{customdata[3]} "
                "K=%{customdata[4]} kd=%{customdata[5]:.2f} cap=%{customdata[6]:.2f}<extra></extra>"
            ),
        )
    )
    fig3.add_trace(
        go.Bar(
            x=pair["variant"],
            y=pair["max_concentration_pct"],
            name="Concentração Máxima (%)",
            marker_color="#d97706",
            yaxis="y2",
            customdata=pair[["mdd"]].values,
            hovertemplate="Variante=%{x}<br>Concentração=%{y:.2f}%<br>MDD=%{customdata[0]:.2f}%<extra></extra>",
        )
    )
    fig3.update_layout(
        title="T-018 HOLDOUT - Melhor C2 vs Melhor C4 (com filtro MDD >= -50%)",
        yaxis={"title": "CAGR (%)"},
        yaxis2={"title": "Concentração (%)", "overlaying": "y", "side": "right"},
        barmode="group",
    )
    out3 = RESULTS / "plot_t018_c2_vs_c4_best.html"
    fig3.write_html(str(out3), include_plotlyjs="cdn")

    # 4) Heatmap C4 (CAGR) por k_damp x cap (agregado por melhor TopN/Cad/K).
    c4 = holdout[holdout["variant"] == "C4"].copy()
    if not c4.empty:
        best = (
            c4.sort_values("cagr", ascending=False)
            .groupby(["k_damp", "max_weight_cap"], as_index=False)
            .first()
        )
        pivot = best.pivot(index="k_damp", columns="max_weight_cap", values="cagr")
        fig4 = px.imshow(
            pivot,
            text_auto=".2f",
            aspect="auto",
            color_continuous_scale="Viridis",
            title="T-018 HOLDOUT - Heatmap CAGR C4 (melhor TopN/Cad/K por kdamp x cap)",
            labels={"x": "max_weight_cap", "y": "k_damp", "color": "CAGR (%)"},
        )
        out4 = RESULTS / "plot_t018_heatmap_c4_cagr.html"
        fig4.write_html(str(out4), include_plotlyjs="cdn")

    print(f"T-018 plot generated: {out1}")
    print(f"T-018 plot generated: {out2}")
    print(f"T-018 plot generated: {out3}")
    if not c4.empty:
        print(f"T-018 plot generated: {out4}")


if __name__ == "__main__":
    main()
