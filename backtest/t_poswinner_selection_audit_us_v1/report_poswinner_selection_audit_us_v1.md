# T-SDC-POSWINNER-SELECTION-AUDIT-US-V1

## Escopo

Estudo read-only pre-registrado para auditar criterios pos-winner do Forno US em duas frentes:

1. Horizonte do score (M3 atual vs variantes de selecao/score curto).
2. Piso de tamanho (min_market_cap 300M vs 150M vs 75M).

Nenhum arquivo blindado foi alterado e nenhuma mudanca de motor foi aplicada.

## Dataset e janelas

- Dataset: `research_dataset_us` v1 (`freeze_asof=2026-06-09`), validado por `verify_dataset.py` com status `OK`.
- Janela:
  - TRAIN ate `2022-12-30`
  - HOLDOUT `2023-01-02` a `2026-06-09`
  - SW1 `2023-01-02` a `2024-06-30`
  - SW2 `2024-07-01` a `2026-06-09`
- Cadencia: 10 pregões; `skip_initial_rebalances=20`.
- Observacao operacional: a ancora efetiva usada no estudo foi `2021-01-04` (script emitiu aviso por curta cobertura historica da ancora original para o volume de ciclos exigido).
- Casos REPL/SMWB de julho/2026 ficam fora da amostra quantitativa por estarem apos o freeze; entram apenas como motivacao qualitativa.

## Diagnostico de redundancia z_m0 vs z_ret

Arquivo: `results/diagnostic_z_m0_z_ret_correlation.json`

- Dias avaliados: `442` (de `2024-09-04` a `2026-06-09`).
- Correlacao media cross-sectional `corr(z_m0, z_ret)`: `1.0000`.
- Mediana: `1.0000`; p10: `~1.0000`; p90: `1.0000`.
- Fracao de dias com corr >= 0.95: `1.0000`.

Leitura tecnica: o diagnostico confirma redundancia quase perfeita entre `z_m0` e `z_ret` no dataset congelado.

## Frente 1 - Horizonte do score

Baseline de comparacao: `Baseline_300M`.

### Arm_RecentSlopeVeto

- Veredito: `INCONCLUSIVO`.
- HOLDOUT:
  - Delta Sharpe: `-0.0144` (IC95 `[-0.3233, 0.2942]`)
  - Delta CVaR5: `+0.0085` (IC95 `[0.0011, 0.0162]`)
  - Delta CAGR proxy: `-0.0017` (IC95 `[-0.0386, 0.0355]`)
  - Gate operacional: `veto_rate=0.1226` (limite `0.35`, gate OK)
- Subjanelas:
  - SW1 com sinal misto/negativo em Sharpe e CAGR.
  - SW2 com sinal positivo nas tres metricas.

Leitura: melhora de cauda em holdout, mas sem dominancia nas tres metricas e sem estabilidade uniforme por subjanela.

### Arm_ShortTermScore

- Veredito: `INCONCLUSIVO`.
- HOLDOUT:
  - Delta Sharpe: `+0.1001` (IC95 `[-0.4590, 0.6528]`)
  - Delta CVaR5: `+0.0108` (IC95 `[-0.0030, 0.0238]`)
  - Delta CAGR proxy: `+0.0121` (IC95 `[-0.0545, 0.0795]`)
  - Gate operacional: `turnover_rate=0.5151` (limite `0.35`, gate NAO OK)
- Subjanelas:
  - SW1 muito favoravel.
  - SW2 com reversao (Sharpe e CAGR negativos vs baseline).

Leitura: apesar de sinais positivos no agregado, o arm falha no gate operacional de rotacao e nao estabiliza entre SW1/SW2.

## Frente 2 - Piso de tamanho

Baseline de comparacao: `Baseline_300M`.

### Arm_150M

- Veredito: `FAVORECIDO_BASELINE`.
- HOLDOUT:
  - Delta Sharpe: `-1.1936` (IC95 `[-1.5880, -0.8261]`)
  - Delta CVaR5: `-0.0053` (IC95 `[-0.0133, 0.0026]`)
  - Delta CAGR proxy: `-0.1492` (IC95 `[-0.1938, -0.1053]`)
  - Gate operacional: `turnover_rate=0.1884` (OK)

### Arm_75M

- Veredito: `FAVORECIDO_BASELINE`.
- HOLDOUT:
  - Delta Sharpe: `-2.0609` (IC95 `[-2.6498, -1.5174]`)
  - Delta CVaR5: `-0.0127` (IC95 `[-0.0247, -0.0002]`)
  - Delta CAGR proxy: `-0.2769` (IC95 `[-0.3446, -0.2093]`)
  - Gate operacional: `turnover_rate=0.2968` (OK)

Leitura: reduzir o piso de tamanho para 150M ou 75M piora materialmente performance ajustada a risco no holdout.

## Diagnostico late_rocket (ticker-level no Top-20 baseline)

- Veredito: `INCONCLUSIVO`.
- Bootstrap (785 ciclos com pares late/non-late):
  - `mean_diff = mean(late) - mean(non_late) = +0.0086`
  - IC95: `[-0.0005, 0.0182]`
  - massa(diff < 0): `0.034`
  - massa(diff > 0): `0.966`
  - SW1 mean diff: `-0.0156`
  - SW2 mean diff: `+0.0302`

Leitura: sem evidencia robusta de dominancia no sentido pre-registrado; sinais mudam entre SW1 e SW2.

## Limitacoes declaradas

1. REPL/SMWB (julho/2026) nao entram na amostra quantitativa por limite do freeze.
2. Comparacao entre fases compartilha historico de precos e nao e totalmente independente.
3. Dataset congelado nao possui coluna de volume/ADV para teste de liquidez na Frente 2.

## Conclusao operacional

- Resultado global:
  - Frente 1: `INCONCLUSIVO` para ambos os arms.
  - Frente 2: `FAVORECIDO_BASELINE` para 150M e 75M.
  - Diagnostico late_rocket: `INCONCLUSIVO`.
- Recomendacao desta etapa (read-only): manter motor inalterado nesta task.
- Qualquer promocao de regra para o motor exige novo ciclo formal com evidencias no padrao de governanca aplicavel.
