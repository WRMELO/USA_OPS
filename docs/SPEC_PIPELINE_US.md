# SPEC — Pipeline US

## Objetivo

Definir o contrato tecnico da Phase 1 para produzir o SSOT canônico US (~1.100 tickers) com rastreabilidade, idempotencia e blindagem contra survivorship bias e lookahead bias.

O resultado desta SPEC deve destravar a implementacao das tasks T-006 a T-011 com entradas/saidas e criterios objetivos de aceite.

## Fontes de dados

- Polygon.io (obrigatorio): composicao historica de indices, OHLCV diario, dividendos, splits.
- FRED (obrigatorio para macro): VIX, DXY, Treasuries, Fed Funds, HY OAS, IG OAS.

Variaveis de ambiente:

- `POLYGON_API_KEY` (obrigatoria)
- `FRED_API_KEY` (opcional; varias series sao publicas)

Politica de resiliencia:

- Retry exponencial com cap (`2^attempt`, max 60s).
- Fallback quando aplicavel (fonte secundaria ou reutilizacao de ultimo snapshot valido).
- Fail-fast com log estruturado quando nao houver fallback seguro.

## Anti-survivorship

Regras obrigatorias:

1. Universo US e composicoes devem ser obtidos por data efetiva (2018-01-01 ate 2026-12-31), nunca por foto atual.
2. Constituinte entra/sai do universo conforme efetividade historica, sem preencher retrospectivamente.
3. Toda agregacao por ticker deve respeitar o universo valido na data da observacao.

## Anti-lookahead

Regras obrigatorias:

1. Nenhuma feature executavel no dia D pode usar informacao de fechamento do proprio D.
2. Toda feature de decisao deve ser `shift(1)` no minimo.
3. Para features cross-market/macro com latencia ou fechamento em horario diferente, usar janela de seguranca D+1 quando aplicavel.
4. Validacao de vazamento deve produzir evidencia objetiva (ex.: checks de alinhamento temporal e amostras de linha).

## Steps

### T-006v2 — Composicao historica via Polygon `/v3/reference/tickers`

- Input: Polygon `/v3/reference/tickers` com datas ancora anuais de 2018 a 2026.
- Output: `data/ssot/index_compositions.parquet`, `data/ssot/index_compositions_coverage.json`.
- Regras: filtrar `market=stocks`, `type=CS`, `primary_exchange in (XNYS, XNAS, XASE)`, paginar por `next_url`, retry exponencial e registrar metricas de requests/retries.
- Colunas minimas: `date`, `ticker`, `is_member`, `effective_from`, `effective_to`, `primary_exchange`, `source`.
- Gate obrigatorio: total de tickers em `2018-01-02` >= 4000; abaixo disso = FAIL logico.

### T-007v2 — Ingestao massiva OHLCV US

- Input: universo bruto (~1.600 tickers pre-exclusao), Polygon aggs/dividends/splits.
- Output: `data/ssot/us_market_data_raw.parquet`.
- Regras: retry exponencial, normalizacao de timezone/data, deduplicacao por (date,ticker), ingestao obrigatoria com `adjusted=False`.
- Idempotencia: reprocessamento nao gera duplicata.

### T-008av2 — Reference data US por ticker

- Input: `data/ssot/index_compositions.parquet`.
- Output: `data/ssot/ticker_reference_us.parquet`, `data/ssot/t008a_reference_report.json`.
- Regras: consultar Polygon Ticker Details (v3) + Ticker Events (vX), retry exponencial, 1 linha por ticker, output deterministico ordenado por ticker.
- Campos minimos: `ticker`, `asof_date`, `active`, `list_date`, `delisted_utc`, `primary_exchange`, `type`, `market_cap`, `ticker_changes_json`, `source`, `ingested_at`.
- Uso downstream: T-008 consome este artefato para classificar blacklist com causa (deslistado=HARD, historico curto por IPO recente=SOFT).

### T-008v2 — Qualidade SPC + blacklist (Shewhart completo)

- Input: `data/ssot/us_market_data_raw.parquet`, `data/ssot/ticker_reference_us.parquet`.
- Output: `data/ssot/us_universe_operational.parquet`, `config/blacklist_us.json`.
- Regras: SPC por ticker idêntico ao RENDA_OPS, classificacao HARD/SOFT e flags de cobertura/outlier com causas de reference data.
- SPC obrigatorio:
  - `log_ret_nominal = np.log(close_operational / close_operational.shift(1))`
  - `X_real = log_ret_nominal - fed_funds_log_daily`
  - `i_value = X_real`
  - `mr_value = abs(i_value - i_value.shift(1))`
  - `SUBGROUP_N=4`, `REF_WINDOW_K=60`
  - constantes Shewhart: `A2=0.729`, `D4=2.282`, `E2=2.66`, `D4_IMR=3.267`
  - `center_line = rolling_mean(i_value, REF_WINDOW_K).shift(1)`
  - `mr_bar = rolling_mean(mr_value, REF_WINDOW_K).shift(1)`
  - `i_ucl = center_line + E2 * mr_bar`, `i_lcl = center_line - E2 * mr_bar`
  - `mr_ucl = D4_IMR * mr_bar`
  - `xbar_value = rolling_mean(i_value, SUBGROUP_N)`
  - `r_value = rolling_max(i_value, SUBGROUP_N) - rolling_min(i_value, SUBGROUP_N)`
  - `r_bar = rolling_mean(r_value, REF_WINDOW_K).shift(1)`
  - `xbar_ucl = center_line + A2 * r_bar`, `xbar_lcl = center_line - A2 * r_bar`
  - `r_ucl = D4 * r_bar`
- Referencia obrigatoria: `RENDA_OPS/pipeline/04_build_canonical.py` (trecho de Shewhart completo).
- Blacklist HARD: ticker ausente no raw, estrutura OHLCV invalida, `active=false`, `delisted_utc` preenchido, ou `fetch_status=FAIL` no reference.
- Blacklist SOFT: historico insuficiente (`history_days < 252`) apenas.

### T-009v2 — Exclusao de BDRs

- Input: universo operacional + `RENDA_OPS/data/ssot/bdr_universe.parquet`.
- Output: `data/ssot/bdr_exclusion_list.json`.
- Regras: matching por ticker US normalizado (`upper/strip`) entre `us_universe_operational.is_operational=true` e `bdr_universe.ticker`.
- Evidencias obrigatorias: `operational_total`, `bdr_underlyings_total`, `excluded_count`, `remaining_count`, amostras de match/miss e `sha256_inputs`.
- Gate de coerencia: `remaining_count = operational_total - excluded_count`.

### T-010v2 — SSOT canônico US

- Input: universo operacional filtrado + eventos corporativos.
- Output: `data/ssot/canonical_us.parquet`.
- Regras: consolidar `us_market_data_raw` + `us_universe_operational` + `ticker_reference_us` + `bdr_exclusion_list` + `blacklist_us`.
- Universo final: tickers com `is_operational=true` (T-008) menos `excluded_tickers` da T-009 (anti-sobreposição BDR).
- `close_raw` deve ser preservado do raw (`adjusted=False`), e `close_operational` deve ser calculado dinamicamente por fator cumulativo de splits.
- Campos minimos operacionais no canônico: `date`, `ticker`, `close_raw`, `close_operational`, `split_factor`, `dividend_rate`, `log_ret_nominal`, `X_real`, `i_value`, `i_ucl`, `i_lcl`, `mr_value`, `mr_ucl`, `xbar_value`, `xbar_ucl`, `xbar_lcl`, `r_value`, `r_ucl`, `center_line`, `mr_bar`, `r_bar`, `universe_tag`.
- Gate de qualidade: zero duplicatas por `(date,ticker)` e contagem de tickers igual ao `remaining_count` da T-009.

### T-011v2 — Macro expandido US

- Input: series FRED.
- Output: `data/ssot/macro_us.parquet`, `data/features/macro_features_us.parquet`.
- Regras: `outer merge` entre calendario de mercado e series FRED, `ffill` (sem backfill), e somente depois filtrar para calendario de pregoes, com evidencias de missing antes/depois.
- Series base (FredAdapter): `vix_close`, `usd_index_broad`, `ust_10y_yield`, `ust_2y_yield`, `fed_funds_rate`, `hy_oas`, `ig_oas`.
- Features macro: para cada série gerar `feature_<alias>_level`, `feature_<alias>_diff_1d`, `feature_<alias>_pct_1d`.
- Anti-lookahead obrigatório: todas as `feature_*` com `shift(1)` e coluna `feature_timestamp_cutoff` no output de features.

## Schemas

### `data/ssot/index_compositions.parquet`

Colunas minimas:

- `date` (date)
- `index_id` (string) — ex.: `R1000`, `SP600`
- `ticker` (string)
- `is_member` (bool)
- `effective_from` (date)
- `effective_to` (date nullable)

### `data/ssot/us_market_data_raw.parquet`

Colunas minimas:

- `date` (date)
- `ticker` (string)
- `open` (float)
- `high` (float)
- `low` (float)
- `close` (float)
- `volume` (float/int)
- `dividend_rate` (float nullable)
- `split_from` (float nullable)
- `split_to` (float nullable)
- `source` (string)
- `ingested_at` (timestamp)

### `data/ssot/us_universe_operational.parquet`

Colunas minimas:

- `date` (date)
- `ticker` (string)
- `is_operational` (bool)
- `i_value` (float)
- `i_ucl` (float)
- `i_lcl` (float)
- `mr_value` (float)
- `mr_ucl` (float)
- `xbar_value` (float)
- `xbar_ucl` (float)
- `xbar_lcl` (float)
- `r_value` (float)
- `r_ucl` (float)
- `quality_flag` (string)
- `blacklist_level` (string nullable: HARD/SOFT/NULL)
- `blacklist_reason` (string nullable)

### `data/ssot/canonical_us.parquet`

Colunas minimas:

- `date` (date)
- `ticker` (string)
- `close_raw` (float)
- `close_operational` (float)
- `split_factor` (float)
- `dividend_rate` (float nullable)
- `log_ret_nominal` (float nullable)
- `X_real` (float nullable)
- `i_value` (float nullable)
- `i_ucl` (float nullable)
- `i_lcl` (float nullable)
- `mr_value` (float nullable)
- `mr_ucl` (float nullable)
- `xbar_value` (float nullable)
- `xbar_ucl` (float nullable)
- `xbar_lcl` (float nullable)
- `r_value` (float nullable)
- `r_ucl` (float nullable)
- `center_line` (float nullable)
- `mr_bar` (float nullable)
- `r_bar` (float nullable)
- `universe_tag` (string)

### `data/ssot/macro_us.parquet`

Colunas minimas:

- `date` (date)
- `vix_close` (float nullable)
- `usd_index_broad` (float nullable)
- `ust_10y_yield` (float nullable)
- `ust_2y_yield` (float nullable)
- `fed_funds_rate` (float nullable)
- `hy_oas` (float nullable)
- `ig_oas` (float nullable)

### `data/features/macro_features_us.parquet`

Colunas minimas:

- `date` (date)
- `feature_*` (float) — apenas features elegiveis para decisao, com shift temporal aplicado
- `feature_timestamp_cutoff` (timestamp) — evidencia de corte temporal

## Gates

Gate de saida da Phase 1:

1. `data/ssot/canonical_us.parquet` com universo final ~1.100 tickers.
2. `data/features/macro_features_us.parquet` presente e valido.
3. Zero survivorship bias: composicao historica por data efetiva comprovada.
4. SPC Shewhart completo por ticker (I-MR e Xbar-R) e blacklist HARD/SOFT documentada.
5. Resiliencia API ativa (retry exponencial + fallback) com logs.
6. Evidencia anti-lookahead registrada (incluindo regra D+1 para cross-market quando aplicavel).

## Riscos e mitigacoes

- Risco: erro de cobertura historica no endpoint de tickers em datas antigas.
  - Mitigacao: gate obrigatorio em 2018-01-02 (>= 4000 tickers) e evidencias detalhadas no coverage report.
- Risco: lacunas de cobertura em alguns tickers.
  - Mitigacao: classificar lacuna, aplicar blacklist SOFT/HARD, nao forcar imputacao opaca.
- Risco: latencia/rate limit no Polygon.
  - Mitigacao: batching, retry exponencial, checkpoint incremental.
- Risco: drift de schema entre steps.
  - Mitigacao: contratos de schema fixos e validacao antes de escrever parquet final.
