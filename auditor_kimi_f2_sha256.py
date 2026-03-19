"""Frente 2: Integridade SHA256 dos artefatos."""
import json
import hashlib
from pathlib import Path

ROOT = Path("/home/wilson/USA_OPS")

def sha256_file(path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()

def check_f2():
    findings = []
    
    # Carregar manifesto
    manifest = json.loads((ROOT / "MANIFESTO_ORIGEM.json").read_text())
    
    print("=== FRENTE 2: INTEGRIDADE SHA256 ===")
    
    # Verificar arquivos do manifesto
    checked = 0
    mismatches = 0
    missing = 0
    
    for entry in manifest["files"]:
        path = ROOT / entry["path"]
        expected = entry.get("local_sha256")
        
        if not expected:
            continue
            
        actual = sha256_file(path)
        checked += 1
        
        if actual is None:
            missing += 1
            findings.append(("ALTO", f"Arquivo ausente: {entry['path']}"))
        elif actual != expected:
            mismatches += 1
            # Calcular diff parcial para report
            findings.append(("MEDIO", f"SHA256 mismatch: {entry['path'][:50]}... (expected: {expected[:16]}..., got: {actual[:16]}...)"))
    
    # Verificar arquivos críticos que devem existir
    critical_files = [
        "config/winner_us.json",
        "backtest/results/curve_C4_K10.csv",
        "backtest/results/t018_ablation_summary.json",
        "data/ssot/canonical_us.parquet",
        "data/features/scores_m3_us.parquet",
    ]
    
    for cf in critical_files:
        path = ROOT / cf
        if not path.exists():
            findings.append(("CRITICO", f"Arquivo crítico ausente: {cf}"))
    
    print(f"Arquivos verificados: {checked}")
    print(f"SHA256 mismatches: {mismatches}")
    print(f"Arquivos ausentes: {missing}")
    
    return findings

if __name__ == "__main__":
    findings = check_f2()
    if findings:
        for sev, msg in findings[:20]:  # Limitar output
            print(f"[{sev}] {msg}")
    else:
        print("[LIMPO] Todos os SHA256 verificados com sucesso")
    print(f"\nTotal findings: {len(findings)}")
