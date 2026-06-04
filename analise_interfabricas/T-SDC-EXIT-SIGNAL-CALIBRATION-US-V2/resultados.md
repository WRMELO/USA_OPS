# Resultados - T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2

Data de execucao: 2026-06-04

## Confirmacao de pre-registro

- Thresholds usados na execucao sao exatamente os pre-registrados no `preregistro.md`.
- Nao houve alteracao pos-hoc da grade:
  - `DRAWDOWN_THRESHOLDS = [-0.10, -0.125, -0.15]`
  - `SLOPE_THRESHOLDS = [-0.08, -0.10, -0.12]`
  - `RANK_DELTA_THRESHOLDS = [10, 15]`
  - `R035_THRESHOLD = -0.1285`
  - `MIN_HOLDING_DAYS = 5`
  - `CAL_END = 2025-07-31`
  - `VAL_START = 2025-08-01`

## Tabela de resultados por configuracao (18 combinacoes)

| config_id | melhoria_preco_cal | falso_positivo_cal | cobertura_cal | melhoria_preco_val | falso_positivo_val | cobertura_val | VEREDITO |
|---|---:|---:|---:|---:|---:|---:|---|
| DD0.100_SL0.080_RD10 | 0.9591 | 0.6667 | 0.0083 | 0.9130 | 1.0000 | 0.0089 | FAIL |
| DD0.100_SL0.080_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.100_SL0.100_RD10 | 0.9591 | 0.6667 | 0.0083 | 0.9130 | 1.0000 | 0.0089 | FAIL |
| DD0.100_SL0.100_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.100_SL0.120_RD10 | 0.9591 | 0.6667 | 0.0083 | nan | 1.0000 | 0.0044 | FAIL |
| DD0.100_SL0.120_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.125_SL0.080_RD10 | 0.9591 | 0.6667 | 0.0083 | 0.9130 | 1.0000 | 0.0089 | FAIL |
| DD0.125_SL0.080_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.125_SL0.100_RD10 | 0.9591 | 0.6667 | 0.0083 | 0.9130 | 1.0000 | 0.0089 | FAIL |
| DD0.125_SL0.100_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.125_SL0.120_RD10 | 0.9591 | 0.6667 | 0.0083 | nan | 1.0000 | 0.0044 | FAIL |
| DD0.125_SL0.120_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.150_SL0.080_RD10 | 0.9591 | 0.6667 | 0.0083 | nan | 1.0000 | 0.0044 | FAIL |
| DD0.150_SL0.080_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.150_SL0.100_RD10 | 0.9591 | 0.6667 | 0.0083 | nan | 1.0000 | 0.0044 | FAIL |
| DD0.150_SL0.100_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |
| DD0.150_SL0.120_RD10 | 0.9591 | 0.6667 | 0.0083 | nan | 1.0000 | 0.0044 | FAIL |
| DD0.150_SL0.120_RD15 | nan | nan | 0.0000 | nan | nan | 0.0000 | INCONCLUSIVO |

## Comparacao direta com a melhor configuracao da V1

- Melhor configuracao da V1 (referencia): `DD0.200_SL0.120`.
  - `melhoria_preco_val = 1.0314`
  - `falso_positivo_val = 0.6207`
  - `veredito = FAIL`
- Melhor configuracao da V2 em validacao (mesmo sem PASS): `DD0.100_SL0.080_RD10`.
  - `melhoria_preco_val = 0.9130`
  - `falso_positivo_val = 1.0000`
  - `veredito = FAIL`
- Leitura objetiva: a V2 condicional reduziu cobertura para quase zero e nao melhorou falso positivo frente a V1.

## Melhor configuracao V2 destacada

- Nao houve configuracao com `PASS`.
- Total: `0 PASS`, `9 FAIL`, `9 INCONCLUSIVO`.

## Sanity-check SATL (entry 2026-05-18, custo 9.7982)

- Baseline R-035 (retorno <= -12.85%) ocorre em `2026-06-03` a `7.84`.
- Na V2, nenhuma configuracao disparou para SATL (`0/18` hits).
- Portanto, o filtro condicional (forma + WE downside + rank deteriorando) foi estrito demais para capturar o caso SATL em `2026-06-01` a `8.67`.

## Limitacoes

- Cobertura extremamente baixa (`<= 0.89%` em validacao), com muitos cenarios sem disparo (`nan` nas metricas de melhoria/falso positivo).
- Definicao de `rank_delta_5d` dentro do holding Top-20 pode eliminar sinais quando a permanencia no Top-20 e curta.
- O filtro composto melhora seletividade teorica, mas nesta grade sacrificou sensibilidade em excesso.
- Resultado negativo/INCONCLUSIVO e valido e encerra esta grade sem fishing pos-hoc.
