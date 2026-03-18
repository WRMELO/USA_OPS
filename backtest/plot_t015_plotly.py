"""Plots do T-015/T-016 (framework + defensiva)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "backtest" / "results"
TRAIN_END = pd.Timestamp("2022-12-30")


def _load_curve(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        raise RuntimeError(f"Curva vazia: {path}")
    base = float(df["equity"].iloc[0]) if float(df["equity"].iloc[0]) > 0 else 1.0
    df["equity_base100"] = (df["equity"] / base) * 100.0
    df["label"] = label
    return df


def _defensive_windows(base_df: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if "regime_defensive_used" not in base_df.columns:
        return []
    s = base_df[["date", "regime_defensive_used"]].copy()
    s["flag"] = s["regime_defensive_used"].fillna(0).astype(int)
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start = None
    for _, row in s.iterrows():
        if row["flag"] == 1 and start is None:
            start = row["date"]
        elif row["flag"] == 0 and start is not None:
            windows.append((start, row["date"]))
            start = None
    if start is not None:
        windows.append((start, s["date"].iloc[-1]))
    return windows


def main() -> None:
    c1 = _load_curve(RESULTS / "curve_C1.csv", "C1")
    c2 = _load_curve(RESULTS / "curve_C2_K15.csv", "C2 K=15")
    c3 = _load_curve(RESULTS / "curve_C3.csv", "C3")

    fig = go.Figure()
    for df, color in ((c1, "#1f77b4"), (c2, "#ff7f0e"), (c3, "#2ca02c")):
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["equity_base100"],
                mode="lines",
                name=df["label"].iloc[0],
                line={"width": 2.3, "color": color},
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra>%{fullData.name}</extra>",
            )
        )

    for x0, x1 in _defensive_windows(c2):
        fig.add_vrect(x0=x0, x1=x1, fillcolor="#ef4444", opacity=0.10, line_width=0)
    fig.add_vline(x=TRAIN_END, line_width=1.5, line_dash="dash", line_color="#64748b")
    fig.update_layout(
        title="T-016 - Equity Base 100 (C1/C2/C3) + Regime Defensivo",
        template="plotly_white",
        hovermode="x unified",
        height=560,
        legend={"orientation": "h", "y": -0.18},
        margin={"l": 50, "r": 30, "t": 80, "b": 80},
        xaxis_title="Data",
        yaxis_title="Equity (base 100)",
    )

    out = RESULTS / "plot_t015_equity_comparison.html"
    fig.write_html(str(out), include_plotlyjs="cdn")
    print(f"T-016 plot generated: {out}")

    events_path = RESULTS / "events_defensive_sells.csv"
    if events_path.exists():
        ev = pd.read_csv(events_path)
        if not ev.empty and "event" in ev.columns:
            ev = ev[ev["event"] == "defensive_sell"].copy()
            if not ev.empty:
                ev["date"] = pd.to_datetime(ev["date"], errors="coerce")
                ev = ev.dropna(subset=["date"])
                fig2 = go.Figure()
                colors = {4: "#f59e0b", 5: "#f97316", 6: "#dc2626"}
                ev["score"] = pd.to_numeric(ev["score"], errors="coerce")
                for score in [4, 5, 6]:
                    sub = ev[ev["score"] == score].copy()
                    if sub.empty:
                        continue
                    fig2.add_trace(
                        go.Scatter(
                            x=sub["date"],
                            y=sub["variant"],
                            mode="markers",
                            name=f"Score {score}",
                            marker={"size": 8, "color": colors[score], "opacity": 0.85},
                            customdata=sub[["ticker", "sell_pct", "sold_shares", "trade_cost"]].values,
                            hovertemplate=(
                                "%{x|%Y-%m-%d}<br>Variante=%{y}<br>Ticker=%{customdata[0]}"
                                "<br>Sell%%=%{customdata[1]:.2f}<br>Shares=%{customdata[2]}"
                                "<br>Custo=%{customdata[3]:.2f}<extra>%{fullData.name}</extra>"
                            ),
                        )
                    )
                fig2.add_vline(x=TRAIN_END, line_width=1.5, line_dash="dash", line_color="#64748b")
                fig2.update_layout(
                    title="T-016 - Timeline de Vendas Defensivas (Score 4/5/6)",
                    template="plotly_white",
                    hovermode="closest",
                    height=520,
                    legend={"orientation": "h", "y": -0.20},
                    margin={"l": 50, "r": 30, "t": 80, "b": 90},
                    xaxis_title="Data",
                    yaxis_title="Variante",
                )
                out2 = RESULTS / "plot_t016_defensive_sells.html"
                fig2.write_html(str(out2), include_plotlyjs="cdn")
                print(f"T-016 defensive timeline generated: {out2}")


if __name__ == "__main__":
    main()
