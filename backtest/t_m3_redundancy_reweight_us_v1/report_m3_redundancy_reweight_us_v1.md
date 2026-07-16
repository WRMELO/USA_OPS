# T-SDC-M3-REDUNDANCY-REWEIGHT-US-V1

## Escopo

Estudo read-only pre-registrado para avaliar se a desduplicacao/repeso do score M3 melhora risco-retorno versus o baseline C4, sem alterar motor.

Arms avaliados:

1. `Baseline_C4`: `score_m3 = z_m0 + z_ret - z_vol` (formula atual).
2. `Arm_Dedup1x1`: `score_m3 = z_ret - z_vol`.
3. `Arm_DedupVolTilt`: `score_m3 = z_ret - 1.5*z_vol`.

Nenhum arquivo blindado foi alterado e nenhuma mudanca de motor foi aplicada.

## Dataset e janelas

- Dataset: `research_dataset_us_v2` (`freeze_asof=2026-07-15`), validado por `verify_dataset_v2.py` com status `OK`.
- Janela:
  - TRAIN ate `2022-12-30`
  - HOLDOUT `2023-01-02` a `2026-07-15`
  - SW1 `2023-01-02` a `2024-06-30`
  - SW2 `2024-07-01` a `2026-07-15`
- Cadencia: 10 pregoes; `skip_initial_rebalances=20`.
- Ancora efetiva usada no estudo: `2021-01-04` (fallback automatico do script por cobertura historica da ancora original para o numero de ciclos requerido).

## Diagnostico de redundancia z_m0 vs z_ret (v2)

Arquivo: `results/diagnostic_z_m0_z_ret_correlation_v2.json`

- Dias avaliados: `442` (de `2024-10-08` a `2026-07-15`).
- Correlacao media cross-sectional `corr(z_m0, z_ret)`: `1.0000`.
- Mediana: `1.0000`; p10: `~1.0000`; p90: `1.0000`.
- Fracao de dias com corr >= 0.95: `1.0000`.

Leitura tecnica: a redundancia `z_m0 ~ z_ret` permanece praticamente perfeita no dataset v2.

## Resultado dos arms (score horizon dedup)

Baseline de comparacao: `Baseline_C4`.

### Arm_Dedup1x1

- Veredito: `INCONCLUSIVO`.
- HOLDOUT:
  - Delta Sharpe: `+0.1813` (IC95 `[-0.1407, 0.5038]`)
  - Delta CVaR5: `+0.0202` (IC95 `[0.0136, 0.0267]`)
  - Delta CAGR proxy: `+0.0257` (IC95 `[-0.0144, 0.0653]`)
  - Gate operacional: `turnover_rate=0.2492` (limite `0.35`, gate OK)
- Subjanelas:
  - SW1: tres metricas positivas.
  - SW2: tres metricas positivas.

Leitura: melhora clara de cauda e sinais positivos em SW1/SW2, mas sem IC95 totalmente favoravel em Sharpe/CAGR e sem massa bootstrap >=90% em duas metricas; criterio R-048 para favorecimento nao foi atingido.

### Arm_DedupVolTilt

- Veredito: `INCONCLUSIVO`.
- HOLDOUT:
  - Delta Sharpe: `-0.0280` (IC95 `[-0.5520, 0.4900]`)
  - Delta CVaR5: `+0.0455` (IC95 `[0.0345, 0.0559]`)
  - Delta CAGR proxy: `+0.0105` (IC95 `[-0.0532, 0.0732]`)
  - Gate operacional: `turnover_rate=0.4809` (limite `0.35`, gate NAO OK)
- Subjanelas:
  - SW1: Sharpe negativo.
  - SW2: Sharpe positivo.

Leitura: apesar de melhora forte de cauda, o arm falha no gate operacional de rotacao e nao fecha criterio de favorecimento.

## Diagnostico late_rocket (ticker-level no Top-20 Baseline_C4)

- Veredito: `INCONCLUSIVO`.
- Bootstrap (807 ciclos com pares late/non-late):
  - `mean_diff = mean(late) - mean(non_late) = +0.0078`
  - IC95: `[-0.0010, 0.0174]`
  - massa(diff < 0): `0.0414`
  - massa(diff > 0): `0.9586`
  - SW1 mean diff: `-0.0162`
  - SW2 mean diff: `+0.0282`

Leitura: sem dominancia robusta no sentido pre-registrado; sinal muda entre SW1 e SW2.

## Casos REPL e SMWB

Arquivo dedicado: `results/case_trace_repl_smwb_us_v1.csv`.

### REPL

- Cobertura no trace: `1178` ciclos (`875` em holdout).
- Pico de `ret_62`: `1.0971` em `2025-12-18`.
  - Nesse ciclo: `raw_m3_rank=7`, elegivel `300M=1`, presente no Top-20 em baseline/dedup/tilt (`1/1/1`).
  - `forward_logret` no ciclo: `-0.0819`.
- Presenca no Top-20 ao longo do trace:
  - Baseline: `29` ciclos
  - Dedup1x1: `14` ciclos
  - DedupVolTilt: `3` ciclos
- Ultimo ciclo do trace (`2026-06-30`): `raw_m3_rank=1061`, fora do Top-20, market cap `~US$705.17M`.

Leitura: os arms de deduplicacao reduziram materialmente a frequencia de entrada de REPL no Top-20 (principalmente no arm com tilt de volatilidade), mas sem veredito estatistico conclusivo no agregado.

### SMWB

- Cobertura no trace: `1178` ciclos (`875` em holdout).
- Em `2026-06-30`: `raw_m3_rank=35`, `market_cap~US$237.42M`, `eligible_300=0`.
- Presenca no Top-20 (baseline/dedup/tilt): `0/0/0` em todos os ciclos do trace.
- Pico de `ret_62`: `0.9131` em `2026-06-30`, com `forward_logret=+0.0658`.

Leitura: a exclusao de SMWB permanece explicada por piso de tamanho (`<300M`), nao pela formula de score.

### Nota temporal dos casos

O `freeze_asof` inclui julho/2026, mas o `case_trace` por ciclos de rebalance com `d_prev_next_reb` encerra em `2026-06-30` (ultimo ciclo completo disponivel na grade de 10 pregoes no momento do freeze).

## Limitacoes declaradas

1. O trace de casos nomeados e por ciclos completos de rebalance; nao representa todos os pregoes corridos de julho/2026.
2. Comparacao entre fases compartilha historico de precos e nao e totalmente independente.
3. Esta task nao reabre a frente de piso de tamanho; interpreta SMWB sob o gate de 300M vigente.

## Conclusao operacional

- Resultado global:
  - `Arm_Dedup1x1`: `INCONCLUSIVO`.
  - `Arm_DedupVolTilt`: `INCONCLUSIVO`.
  - Diagnostico late_rocket: `INCONCLUSIVO`.
- Recomendacao desta etapa (read-only): manter motor inalterado nesta task.
- Qualquer promocao de regra para o motor exige novo ciclo formal com evidencias no padrao de governanca aplicavel.
