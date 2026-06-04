# Resultados - T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1

- Data de execucao: 2026-06-04
- Task ID: `T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1`
- Pre-registro de referencia: `preregistro.md`
- Confirmacao de integridade: thresholds e split temporal foram mantidos exatamente como pre-registrados, sem ajuste pos-hoc.

## Cobertura dos dados

- `operational_window`: 2024-05-30 a 2026-06-03
- `scores_m3_us`: 2024-08-28 a 2026-06-03
- Universo (Top-20 diario): 8.840 linhas
- Split:
  - Calibracao: 4.620 linhas
  - Validacao: 4.220 linhas

## Veredito por hipotese

| Hipotese | n_sig_cal | media_sig_cal | media_ctrl_cal | p_cal | n_sig_val | media_sig_val | media_ctrl_val | p_val | Veredito |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| H1 Exaustao parabolica sem persistencia | 1519 | -0.039486 | -0.013801 | 0.0025 | 1395 | -0.039285 | -0.012469 | 0.0005 | PASS |
| H2 Run downside carta-I (run >= 6) | 144 | -0.007009 | -0.022736 | 0.7726 | 152 | -0.008159 | -0.022062 | 0.7671 | FAIL |
| H3 Drawdown desde pico local 10d | 1354 | -0.033493 | -0.017583 | 0.0345 | 840 | -0.018048 | -0.022445 | 0.7171 | INCONCLUSIVO |
| H4 Amplitude LARGA + retorno negativo | 239 | 0.006319 | -0.023804 | 0.9530 | 269 | -0.036848 | -0.020480 | 0.1204 | INCONCLUSIVO |

## Casos de motivacao (SATL / FCEL)

### SATL e H2 (run downside)

- `run_length_on_2026_06_03 = 7` pontos consecutivos abaixo da linha central.
- `trigger_before_2026_06_03 = true`.
- Observacao: no historico amplo, a primeira ocorrencia de `run >= 6` para SATL apareceu antes (`2025-01-23`), o que reforca que o gatilho H2 isolado tende a gerar sinal com baixa discriminacao de queda futura no universo testado.

### FCEL e H1 em 2026-06-02

- `ret_62_log = 1.0561` (acima do corte 0.80).
- `amplitude = 0.4087`.
- `amplitude_larga = false` no dia 2026-06-02 pelo corte dinamico do tercil superior.
- Resultado: `signal_h1 = false` nesse dia, apesar do `ret_62` alto.

## Limitacoes

- O estudo mede poder preditivo de queda em `forward_5d_ret`, nao impacto direto no P&L de operacoes executadas com friccao real.
- O recorte depende da cobertura de `scores_m3_us` (inicio efetivo em 2024-08-28), nao da serie completa do `operational_window`.
- H2 baseado apenas em run da carta-I mostrou baixa separacao estatistica nesta configuracao.
- O check de SATL em H2 confirma o padrao visual (run=7), mas isoladamente nao valida valor operacional do gatilho.

## Decisao final

**PASS parcial da linha de investigacao**, com um unico candidato robusto para proxima etapa:

- **Aprovado para follow-up:** H1 (exaustao parabolica sem persistencia) - sinal consistente em calibracao e validacao.
- **Nao aprovado para endurecimento imediato:** H2 (FAIL), H3/H4 (INCONCLUSIVO).

Recomendacao de proxima task (se Owner aprovar):
1. Atualizar skill `analista-usa` para alerta consultivo baseado em H1.
2. Registrar governanca correspondente (DECISION_LOG + eventual update em REGRAS_OPERACIONAIS/CORPUS).
3. Manter H2/H3/H4 como observabilidade ate nova calibracao.
