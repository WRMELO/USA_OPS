"""Frente 1: Consistência numérica cruzada entre reports."""
import json
import pandas as pd
from pathlib import Path

ROOT = Path("/home/wilson/USA_OPS")

def check_f1():
    # Carregar todos os JSONs de métricas
    winner = json.loads((ROOT / "config" / "winner_us.json").read_text())
    t016 = json.loads((ROOT / "backtest/results/t016_backtest_report.json").read_text())
    t018 = json.loads((ROOT / "backtest/results/t018_ablation_summary.json").read_text())
    
    findings = []
    
    # Extrair métricas holdout C4 do winner
    win_c4 = winner["holdout_metrics"]
    win_cagr = win_c4["cagr_pct"]
    win_mdd = win_c4["mdd_pct"]
    
    # Verificar contra t018_ablation_summary (deve ter C4_N20_Cad10_K10_kd0.0_cap0.06)
    c4_entries = [x for x in t018 if x["variant"] == "C4" and x["split"] == "HOLDOUT" 
                  and x["top_n"] == 20 and x["rebalance_cadence"] == 10 
                  and x["buffer_k"] == 10 and x["k_damp"] == 0.0 
                  and x["max_weight_cap"] == 0.06]
    
    if not c4_entries:
        findings.append(("CRITICO", "Winner C4 não encontrado em t018_ablation_summary"))
    else:
        c4 = c4_entries[0]
        # Tolerância de 0.1 p.p. para diferenças de arredondamento
        if abs(c4["cagr"] - win_cagr) > 0.2:
            findings.append(("ALTO", f"CAGR mismatch: winner={win_cagr}, t018={c4['cagr']}"))
        if abs(c4["mdd"] - win_mdd) > 0.2:
            findings.append(("ALTO", f"MDD mismatch: winner={win_mdd}, t018={c4['mdd']}"))
    
    # Verificar contra t016 metrics (C4)
    t016_c4 = t016.get("metrics", {}).get("C4", {})
    if t016_c4:
        t016_cagr_full = t016_c4.get("cagr_full", 0) * 100
        t016_mdd_full = t016_c4.get("mdd_full", 0) * 100
        if abs(t016_cagr_full - win_cagr) > 0.2:
            findings.append(("MEDIO", f"CAGR t016 vs winner: {t016_cagr_full} vs {win_cagr}"))
        if abs(t016_mdd_full - win_mdd) > 0.2:
            findings.append(("MEDIO", f"MDD t016 vs winner: {t016_mdd_full} vs {win_mdd}"))
    
    # Verificar curva C4_K10.csv
    curve = pd.read_csv(ROOT / "backtest/results/curve_C4_K10.csv")
    curve["date"] = pd.to_datetime(curve["date"])
    holdout_start = pd.Timestamp("2023-01-02")
    holdout_end = pd.Timestamp("2026-03-16")
    curve_holdout = curve[(curve["date"] >= holdout_start) & (curve["date"] <= holdout_end)]
    
    if len(curve_holdout) < 2:
        findings.append(("CRITICO", "Curva C4 sem dados suficientes no holdout"))
    else:
        eq = curve_holdout["equity"].values
        n_years = max(len(eq) / 252.0, 1/252)
        curve_cagr = (eq[-1] / eq[0]) ** (1.0 / n_years) - 1.0
        running_max = pd.Series(eq).cummax()
        curve_mdd = ((eq / running_max) - 1.0).min()
        
        if abs(curve_cagr * 100 - win_cagr) > 0.2:
            findings.append(("ALTO", f"Curva C4 CAGR {curve_cagr*100:.4f} vs winner {win_cagr}"))
        if abs(curve_mdd * 100 - win_mdd) > 0.2:
            findings.append(("ALTO", f"Curva C4 MDD {curve_mdd*100:.4f} vs winner {win_mdd}"))
    
    # Verificar SHA256 no winner vs MANIFEST
    manifest = json.loads((ROOT / "MANIFESTO_ORIGEM.json").read_text())
    winner_file = next((f for f in manifest["files"] if f["path"] == "config/winner_us.json"), None)
    if winner_file:
        import hashlib
        actual_sha = hashlib.sha256((ROOT / "config/winner_us.json").read_bytes()).hexdigest()
        if actual_sha != winner_file["local_sha256"]:
            findings.append(("MEDIO", f"SHA256 winner mismatch: manifest={winner_file['local_sha256'][:16]}..., actual={actual_sha[:16]}..."))
    
    return findings

if __name__ == "__main__":
    findings = check_f1()
    print("=== FRENTE 1: CONSISTÊNCIA NUMÉRICA ===")
    if findings:
        for sev, msg in findings:
            print(f"[{sev}] {msg}")
    else:
        print("[LIMPO] Todas as métricas cruzadas consistentes")
    print(f"\nTotal findings: {len(findings)}")
