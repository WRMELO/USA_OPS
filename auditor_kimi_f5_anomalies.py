"""Frente 5: Distribuição e anomalias."""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path("/home/wilson/USA_OPS")

def check_f5():
    findings = []
    
    print("=== FRENTE 5: DISTRIBUIÇÃO E ANOMALIAS ===")
    
    # Carregar curva C4
    c4 = pd.read_csv(ROOT / "backtest/results/curve_C4_K10.csv")
    c4["date"] = pd.to_datetime(c4["date"])
    
    # Split TRAIN/HOLDOUT
    train = c4[c4["date"] <= "2022-12-30"]
    holdout = c4[c4["date"] > "2022-12-30"]
    
    # Calcular retornos diários
    train["ret"] = train["equity"].pct_change()
    holdout["ret"] = holdout["equity"].pct_change()
    
    # Estatísticas de retorno
    print("\nEstatísticas de retorno diário:")
    print(f"  TRAIN: mean={train['ret'].mean()*100:.4f}%, std={train['ret'].std()*100:.4f}%")
    print(f"  HOLDOUT: mean={holdout['ret'].mean()*100:.4f}%, std={holdout['ret'].std()*100}%")
    
    # Verificar autocorrelação dos retornos
    train_autocorr = train["ret"].dropna().autocorr(lag=1)
    holdout_autocorr = holdout["ret"].dropna().autocorr(lag=1)
    print(f"\nAutocorrelação lag-1:")
    print(f"  TRAIN: {train_autocorr:.4f}")
    print(f"  HOLDOUT: {holdout_autocorr:.4f}")
    
    if abs(train_autocorr) > 0.1 or abs(holdout_autocorr) > 0.1:
        findings.append(("MEDIO", f"Autocorrelação alta detectada: train={train_autocorr:.3f}, holdout={holdout_autocorr:.3f}"))
    
    # Verificar saltos > 15% (anomalias)
    train_jumps = train[abs(train["ret"]) > 0.15]
    holdout_jumps = holdout[abs(holdout["ret"]) > 0.15]
    
    print(f"\nSaltos > 15%:")
    print(f"  TRAIN: {len(train_jumps)} dias")
    print(f"  HOLDOUT: {len(holdout_jumps)} dias")
    
    if len(holdout_jumps) > 5:
        findings.append(("BAIXO", f"{len(holdout_jumps)} saltos >15% no holdout (volatilidade normal para small-caps)"))
    
    # Verificar concentração
    print(f"\nConcentração máxima:")
    print(f"  TRAIN max: {train['max_concentration'].max()*100:.2f}%")
    print(f"  HOLDOUT max: {holdout['max_concentration'].max()*100:.2f}%")
    
    # Verificar dias em regime defensivo
    print(f"\nRegime defensivo:")
    print(f"  TRAIN: {train['regime_defensive_used'].mean()*100:.1f}%")
    print(f"  HOLDOUT: {holdout['regime_defensive_used'].mean()*100:.1f}%")
    
    return findings

if __name__ == "__main__":
    findings = check_f5()
    if findings:
        for sev, msg in findings:
            print(f"[{sev}] {msg}")
    else:
        print("\n[LIMPO] Distribuição normal, sem anomalias críticas")
    print(f"\nTotal findings: {len(findings)}")
