# Resultados - T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1

Data de execucao: 2026-06-04

## Confirmacao de pre-registro

- Thresholds usados na execucao sao exatamente os pre-registrados no `preregistro.md`.
- Nao houve alteracao pos-hoc da grade:
  - `drawdown_thresholds = [-0.10, -0.125, -0.15, -0.175, -0.20]`
  - `slope_thresholds = [-0.08, -0.10, -0.12]`
  - `R035_THRESHOLD = -0.1285`
  - `MIN_HOLDING_DAYS = 5`
  - `CAL_END = 2025-07-31`
  - `VAL_START = 2025-08-01`

## Tabela de resultados por configuracao

| config_id | melhoria_preco_cal | falso_positivo_cal | cobertura_cal | melhoria_preco_val | falso_positivo_val | cobertura_val | VEREDITO |
|---|---:|---:|---:|---:|---:|---:|---|
| DD0.100_SL0.080 | 1.0263 | 0.6757 | 0.4208 | 0.9988 | 0.7821 | 0.3200 | FAIL |
| DD0.100_SL0.100 | 1.0272 | 0.6602 | 0.3875 | 1.0048 | 0.7671 | 0.2978 | FAIL |
| DD0.100_SL0.120 | 1.0309 | 0.6222 | 0.3292 | 0.9989 | 0.7460 | 0.2533 | FAIL |
| DD0.125_SL0.080 | 1.0239 | 0.6400 | 0.3750 | 1.0123 | 0.7500 | 0.2756 | FAIL |
| DD0.125_SL0.100 | 1.0294 | 0.6277 | 0.3500 | 1.0083 | 0.7500 | 0.2756 | FAIL |
| DD0.125_SL0.120 | 1.0331 | 0.6180 | 0.3250 | 1.0082 | 0.7460 | 0.2533 | FAIL |
| DD0.150_SL0.080 | 1.0360 | 0.6265 | 0.3208 | 1.0073 | 0.7679 | 0.2311 | FAIL |
| DD0.150_SL0.100 | 1.0324 | 0.6203 | 0.3042 | 1.0049 | 0.7679 | 0.2311 | FAIL |
| DD0.150_SL0.120 | 1.0322 | 0.6053 | 0.2875 | 1.0068 | 0.7593 | 0.2222 | FAIL |
| DD0.175_SL0.080 | 1.0289 | 0.6232 | 0.2625 | 0.9979 | 0.7381 | 0.1689 | FAIL |
| DD0.175_SL0.100 | 1.0266 | 0.6232 | 0.2625 | 0.9943 | 0.7317 | 0.1644 | FAIL |
| DD0.175_SL0.120 | 1.0267 | 0.6119 | 0.2542 | 1.0144 | 0.7179 | 0.1556 | FAIL |
| DD0.200_SL0.080 | 1.0342 | 0.6406 | 0.2333 | 1.0254 | 0.6452 | 0.1156 | FAIL |
| DD0.200_SL0.100 | 1.0320 | 0.6406 | 0.2333 | 1.0254 | 0.6333 | 0.1111 | FAIL |
| DD0.200_SL0.120 | 1.0451 | 0.6349 | 0.2292 | 1.0314 | 0.6207 | 0.1067 | FAIL |

## Melhor configuracao destacada

- Nao houve configuracao com `PASS` (campo `best_pass_config = null` em `resultados_raw.json`).
- Todas as 15 configuracoes foram classificadas como `FAIL`, dominadas por `falso_positivo_pct` acima de 0.50 em calibracao e validacao.

## Sanity-check SATL (entry 2026-05-18, custo 9.7982)

- Baseline R-035 (retorno <= -12.85%) ocorre em `2026-06-03` a `7.84`.
- Configuracoes que teriam disparado em `2026-06-01` a `8.67` (saida melhor e antecipada):
  - `DD0.100_SL0.080`, `DD0.100_SL0.100`, `DD0.100_SL0.120`
  - `DD0.125_SL0.080`, `DD0.125_SL0.100`, `DD0.125_SL0.120`
  - `DD0.150_SL0.080`, `DD0.150_SL0.100`, `DD0.150_SL0.120`
  - `DD0.175_SL0.080`, `DD0.175_SL0.100`, `DD0.175_SL0.120`
- Configuracoes com disparo apenas no mesmo dia do baseline (`2026-06-03`, `7.84`):
  - `DD0.200_SL0.080`, `DD0.200_SL0.100`, `DD0.200_SL0.120`

## Limitacoes

- Universo restrito a holdings Top-20 com no minimo 5 pregoes consecutivos; pode excluir padroes de permanencia curta.
- Criterio de falso positivo e estrito: qualquer disparo sem atingimento posterior do baseline R-035 e contabilizado como falso positivo.
- Nao foi aplicado filtro adicional de regime (macro/volatilidade) nesta rodada.
- Resultado negativo global nao invalida o principio de saida por forma da curva; indica que esta grade de thresholds nao atende aos criterios de robustez definidos.
