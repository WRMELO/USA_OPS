"""Frente 6: Universo e seleção."""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path("/home/wilson/USA_OPS")

def check_f6():
    findings = []
    
    print("=== FRENTE 6: UNIVERSO E SELEÇÃO ===")
    
    import json
    
    # Carregar canonical e scores
    canonical = pd.read_parquet(ROOT / "data/ssot/canonical_us.parquet", columns=["date", "ticker", "market_cap"])
    scores = pd.read_parquet(ROOT / "data/features/scores_m3_us.parquet", columns=["date", "ticker", "m3_rank"])
    
    # Carregar blacklist como dict/list
    with open(ROOT / "config/blacklist_us.json") as f:
        blacklist_data = json.load(f)
    
    canonical["date"] = pd.to_datetime(canonical["date"]).dt.normalize()
    scores["date"] = pd.to_datetime(scores["date"]).dt.normalize()
    
    # Verificar evolução do universo ao longo do tempo
    print("\nEvolução do universo:")
    yearly = scores.groupby(scores["date"].dt.year)["ticker"].nunique()
    for year, count in yearly.items():
        print(f"  {year}: {count} tickers com score")
    
    # Verificar se há churn excessivo
    dates = sorted(scores["date"].unique())
    sample_dates = [dates[0], dates[len(dates)//2], dates[-1]]
    
    print("\nChurn de tickers (sample):")
    for i, d in enumerate(sample_dates):
        day_tickers = set(scores[scores["date"] == d]["ticker"])
        print(f"  {pd.Timestamp(d).date()}: {len(day_tickers)} tickers")
        if i > 0:
            prev_tickers = set(scores[scores["date"] == sample_dates[i-1]]["ticker"])
            common = len(day_tickers.intersection(prev_tickers))
            print(f"    Continuidade com anterior: {common}/{len(day_tickers)} ({common/len(day_tickers)*100:.1f}%)")
    
    # Verificar filtro de market_cap
    merged = scores.merge(canonical[["date", "ticker", "market_cap"]], on=["date", "ticker"], how="left")
    merged["passes_filter"] = merged["market_cap"] >= 300_000_000
    
    filter_rate = merged["passes_filter"].mean()
    print(f"\nFiltro market_cap >= $300M:")
    print(f"  Taxa de passagem: {filter_rate*100:.1f}%")
    
    if filter_rate < 0.5:
        findings.append(("MEDIO", f"Filtro market_cap muito restritivo: apenas {filter_rate*100:.1f}% passam"))
    
    # Verificar blacklist
    black_tickers = set()
    if isinstance(blacklist_data, dict):
        for key, val in blacklist_data.items():
            if isinstance(val, list):
                black_tickers.update([str(v).upper() for v in val])
    elif isinstance(blacklist_data, list):
        black_tickers = set([str(v).upper() for v in blacklist_data])
    
    print(f"\nBlacklist: {len(black_tickers)} tickers")
    
    # Verificar se tickers da blacklist aparecem nos scores
    all_scored = set(scores["ticker"].str.upper())
    overlap = black_tickers.intersection(all_scored)
    if overlap:
        findings.append(("BAIXO", f"{len(overlap)} tickers da blacklist aparecem nos scores (esperado: 0)"))
    
    return findings

if __name__ == "__main__":
    findings = check_f6()
    if findings:
        for sev, msg in findings:
            print(f"[{sev}] {msg}")
    else:
        print("\n[LIMPO] Universo saudável, sem vieses de seleção")
    print(f"\nTotal findings: {len(findings)}")
