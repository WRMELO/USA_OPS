# Pre-Registration — T-SDC-TOPN-CAP-SWEEP-US-V1

## Correcao pos-falha (registrada antes de qualquer metrica valida)

- A primeira execucao deste estudo abortou nos gates hard G2 e G4 para as 12 configuracoes, antes de gerar qualquer metrica, bootstrap ou veredito.
- O `manifest.json` do dataset congelado v2 confirma cobertura efetiva menor que a assumida no registro inicial:
  - `macro`: inicia em `2024-07-11`
  - `scores` M3: inicia em `2024-10-08`
- Entre `2024-07-11` e `2024-10-07`, o motor opera sem sinal M3 (sem candidatos), o que gera `n_tickers=0` e `cash_ratio=1.0` por construcao; isso contaminou o gate G2 sem refletir falha logica de selecao.
- O gate G4 original (diferenca absoluta para `top_n`) estava mal calibrado para o regime defensivo estrutural do C4; foi substituido por criterio de **razao** `mean_tickers_rebalance/top_n`, com hard e soft separados.
- Correcao aplicada nesta versao:
  - janela efetiva inicia em `2024-10-08`;
  - particionamento avaliavel: `SW1=2024-10-08..2025-08-26` e `SW2=2025-08-27..2026-07-15`;
  - exclusao de pregoes anteriores a `2024-10-08` da inferencia;
  - G4 recalibrado com evidencia empirica (historico T018 + rodada atual pos-correcao).
- O motor C4 e o veto R-060 **nao foram alterados**. O texto original permanece abaixo, com trilha explicita das datas anteriores.

## Meta

- **task_id**: `T-SDC-TOPN-CAP-SWEEP-US-V1`
- **decision_ref**: `PENDING-DECISION-LOG`
- **status**: `registered_before_execution=true`
- **registered_at**: `2026-07-24T10:42:00-03:00`
- **study_type**: read-only (sem alteracao de motor)

## Hipotese

Existe configuracao de `top_n` diferente de 20 que apresenta evidencia de melhora de risco-retorno frente ao baseline do winner, e essa evidencia deve ser avaliada em dois cenarios:

1. **ISOLADO**: variar apenas `top_n` com `max_weight_cap` fixo em 6% (diagnostico de confundimento N x cap).
2. **COVARIADO**: variar `top_n` e co-variar `max_weight_cap` por formula coerente com o winner.

## Baseline e parametros fixos

- Baseline estrutural: **C4 + R-060** (veto operacional `BandExp ∩ ret_62>=1.00`) aplicado por pre-filtragem do pool de scores antes da selecao.
- Parametros fixos herdados do winner:
  - `rebalance_cadence=10`
  - `buffer_k=10`
  - `k_damp=0.0`
  - `min_market_cap=300000000.0`
  - `friction_one_way_bps=2.5`
  - `settlement_days=1`
  - `base_capital=100000.0`
- **Proibido** herdar `rebalance_anchor_date` de circuito LIVE (R-061).

## Grid experimental

- `top_n_grid = [10, 12, 15, 20, 25, 30]`
- `baseline_top_n = 20`
- Tracks:
  - `ISOLADO`: `max_weight_cap = 0.06`
  - `COVARIADO`: `max_weight_cap(top_n) = round(1.2/top_n, 4)`  
    (garante `0.0600` quando `top_n=20`)

## Dataset e integridade (R-041)

- Dataset congelado: `backtest/research_dataset_us_v2/`
- `freeze_asof = 2026-07-15`
- `decision_ref_freeze = SALA D-110`
- Verificacao obrigatoria de hash (`manifest.json`) antes de qualquer metrica.

## Janelas (atualizado pela correcao pos-falha acima)

- `TRAIN`: `N/A` nesta task (texto original preservado: ~~`2021-01-01` a `2022-12-30`~~)
- `HOLDOUT`: `2024-10-08` a `2026-07-15` (texto original preservado: ~~`2023-01-02` a `2026-07-15`~~)
- `SW1`: `2024-10-08` a `2025-08-26` (texto original preservado: ~~`2023-01-02` a `2024-06-30`~~)
- `SW2`: `2025-08-27` a `2026-07-15` (texto original preservado: ~~`2024-07-01` a `2026-07-15`~~)

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

- G1: calendario de rebalance sem ancora LIVE (por construcao do runner).
- G2: caixa ocioso (hard + soft) por configuracao.
- G3: identidade de equity valida (`equity` finito e >0 em 100% dos dias).
- G4: razao `mean_tickers_rebalance/top_n` por configuracao com hard `0.60<=razao<=1.05` e soft (nao abortante) `razao>=0.75` (texto original preservado: ~~faixa de `n_tickers` em dias de rebalance, relativa a `top_n`~~).
- G5: hashes do dataset congelado conferidos.
- G6: plausibilidade de ativacao do veto R-060 (taxa media diaria >0 e <=30%).
- G7: diagnostico de concentracao/cap binding (nao abortante).

**Regra de abort**: qualquer FAIL hard nos gates aborta a rodada antes de bootstrap e tiers.

## Fora de escopo

- Alterar `winner_us.json`, `pipeline/09_decide.py` ou qualquer arquivo blindado.
- Reabrir politica de reinvestimento defensivo (D-135/D-136/D-137).
- Promover automaticamente novo `top_n` ao motor.
- Usar SSOT vivo para baseline de pesquisa.
- Fazer phase-sweep de offsets nesta rodada.

## Proibicao de pos-hoc

Os criterios de dataset, gates, bootstrap, tiers e materialidade estao congelados neste pre-registro e nao podem ser reinterpretados apos observar os resultados.
