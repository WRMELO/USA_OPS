# Pre-Registration — T-SDC-TOPN-CAP-SWEEP-US-V2

## Extensao pre-execucao (antes de qualquer metrica V2)

- O V1 ficou restrito por cobertura curta de `macro_us.parquet` no freeze v2 original (`2024-07-11..2026-07-15`), pois o harness simula na intersecao `px_exec_wide ∩ cash_log_daily`.
- Nesta V2, o dataset de pesquisa recebe **addendum aditivo** com:
  - `scores_m3_us_fullhistory.parquet` (gerado por `t012` no canonical congelado);
  - `macro_us_fullhistory.parquet` (gerado por `t011` no calendario do canonical congelado);
  - manifest atualizado em `fullhistory_addendum`.
- O V1 permanece intocado como evidencia historica.
- Esta extensao preserva motor, gates e tiers; altera apenas cobertura temporal dos insumos de pesquisa.

## Meta

- **task_id**: `T-SDC-TOPN-CAP-SWEEP-US-V2`
- **decision_ref**: `PENDING-DECISION-LOG`
- **extends_task_id**: `T-SDC-TOPN-CAP-SWEEP-US-V1`
- **status**: `registered_before_execution=true`
- **registered_at**: `2026-07-24T12:43:47-03:00`
- **study_type**: read-only (sem alteracao de motor)

## Hipotese

Existe configuracao de `top_n` diferente de 20 que apresenta evidencia de melhora de risco-retorno frente ao baseline do winner, e essa evidencia deve ser avaliada em dois cenarios:

1. **ISOLADO**: variar apenas `top_n` com `max_weight_cap` fixo em 6% (diagnostico de confundimento N x cap).
2. **COVARIADO**: variar `top_n` e co-variar `max_weight_cap` por formula coerente com o winner.

## Baseline e parametros fixos

- Baseline estrutural: **C4 + R-060** (veto operacional `BandExp ∩ ret_62>=1.00`) aplicado por pre-filtragem do pool de scores antes da selecao.
- Parametros fixos:
  - `rebalance_cadence=10`
  - `buffer_k=10`
  - `k_damp=0.0`
  - `min_market_cap=300000000.0`
  - `friction_one_way_bps=2.5`
  - `settlement_days=1`
  - `base_capital=100000.0`
- Proibido herdar `rebalance_anchor_date` de circuito LIVE (R-061).

## Grid experimental

- `top_n_grid = [10, 12, 15, 20, 25, 30]`
- `baseline_top_n = 20`
- Tracks:
  - `ISOLADO`: `max_weight_cap = 0.06`
  - `COVARIADO`: `max_weight_cap(top_n) = round(1.2/top_n, 4)`

## Dataset e integridade (R-041)

- Dataset base congelado: `backtest/research_dataset_us_v2/`
- `freeze_asof = 2026-07-15`
- Addendum fullhistory no mesmo dataset (manifest `fullhistory_addendum`):
  - `scores_m3_us_fullhistory.parquet`
  - `macro_us_fullhistory.parquet`
- Verificacao obrigatoria de hash antes de qualquer metrica:
  - arquivos originais do freeze (`canonical_us.parquet`, `macro_us.parquet`, `scores_m3_us.parquet`);
  - arquivos do addendum (`macro_us_fullhistory.parquet`, `scores_m3_us_fullhistory.parquet`).

## Janelas (derivadas empiricamente antes da execucao)

- `macro_date_min = 2021-01-04`
- `scores_date_min = 2021-04-05`
- `effective_start = max(macro_date_min, scores_date_min) = 2021-04-05`
- `HOLDOUT = 2021-04-05 .. 2026-07-15`
- `SW1 = 2021-04-05 .. 2023-11-17`
- `SW2 = 2023-11-20 .. 2026-07-15`

## Inferencia e tiers (R-048)

- Bootstrap: `cluster por dia`
- `n_resamples = 2000`
- `seed = 42`
- Metricas:
  - `delta_cvar5`
  - `delta_sharpe_cost_adj`
- Materialidade em HOLDOUT:
  - `abs(delta_sharpe_cost_adj)>=0.30` OU `abs(delta_cvar5)>=0.02`
- Tiers:
  1. `DOMINA_FORTE`: IC95 totalmente favoravel em HOLDOUT, SW1 e SW2 para ambas metricas.
  2. `FAVORECIDO_<lado>`: sem DOMINA_FORTE, massa bootstrap >=90% em HOLDOUT para ambas metricas, concordancia direcional em HOLDOUT/SW1/SW2 e materialidade acima do limiar.
  3. `INCONCLUSIVO`: qualquer caso fora dos anteriores.

## Sanity gates pre-metricas (R-061)

- G1: calendario de rebalance sem ancora LIVE.
- G2: caixa ocioso por configuracao (hard + soft).
- G3: equity finito e >0 em 100% das linhas.
- G4: razao `mean_tickers_rebalance/top_n` com hard `0.60<=razao<=1.05` e soft `razao>=0.75`.
- G5: hashes validos para freeze original + addendum fullhistory.
- G6: taxa media diaria de veto R-060 >0 e <=30%.
- G7: diagnostico de concentracao/cap binding (nao abortante).

**Regra de abort**: qualquer FAIL hard nos gates aborta a rodada antes de bootstrap e tiers.

## Fora de escopo

- Alterar `winner_us.json`, `pipeline/09_decide.py` ou qualquer blindado do motor.
- Promover automaticamente novo `top_n` ao motor.
- Usar SSOT vivo como baseline de pesquisa.
- Alterar V1 retroativamente.

## Proibicao de pos-hoc

Os criterios de dataset, gates, bootstrap, tiers e materialidade estao congelados neste pre-registro e nao podem ser reinterpretados apos observar os resultados.
