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

1. Universo US e composicoes devem ser obtidos por data efetiva (2018-01-01 ate 2026-12-31), nunca por foto atual, exceto na T-006 em modo snapshot aprovado em D-005.
2. Constituinte entra/sai do universo conforme efetividade historica, sem preencher retrospectivamente.
3. Toda agregacao por ticker deve respeitar o universo valido na data da observacao.

## Anti-lookahead

Regras obrigatorias:

1. Nenhuma feature executavel no dia D pode usar informacao de fechamento do proprio D.
2. Toda feature de decisao deve ser `shift(1)` no minimo.
3. Para features cross-market/macro com latencia ou fechamento em horario diferente, usar janela de seguranca D+1 quando aplicavel.
4. Validacao de vazamento deve produzir evidencia objetiva (ex.: checks de alinhamento temporal e amostras de linha).

## Steps

### T-006 — Composicao do universo via snapshot iShares

- Input: CSV publico iShares de holdings (IWB para R1000, IJR para SP600).
- Output: `data/ssot/index_compositions.parquet`, `data/ssot/index_compositions_coverage.json`.
- Regras: filtrar apenas `Asset Class == Equity`, registrar `snapshot_date`, `source_url`, `raw_sha256`, e modo `coverage_mode=snapshot`.
- Limitacao aceita (D-005): snapshot atual nao representa composicao historica por data efetiva.
- Idempotencia: mesma data de snapshot + mesmas fontes => mesmo parquet.

### T-007 — Ingestao massiva OHLCV US

- Input: universo bruto (~1.600 tickers pre-exclusao), Polygon aggs/dividends/splits.
- Output: `data/ssot/us_market_data_raw.parquet`.
- Regras: retry exponencial, normalizacao de timezone/data, deduplicacao por (date,ticker).
- Idempotencia: reprocessamento nao gera duplicata.

### T-008 — Qualidade SPC + blacklist

- Input: `data/ssot/us_market_data_raw.parquet`.
- Output: `data/ssot/us_universe_operational.parquet`, `config/blacklist_us.json`.
- Regras: SPC por ticker (xbar/ucl/lcl), classificacao HARD/SOFT, flags de cobertura/outlier.

### T-009 — Exclusao de BDRs

- Input: universo operacional + `RENDA_OPS/data/ssot/bdr_universe.parquet`.
- Output: `data/ssot/bdr_exclusion_list.json`.
- Regras: remover tickers com sobreposicao BDR-B3; log de contagem e diffs.

### T-010 — SSOT canônico US

- Input: universo operacional filtrado + eventos corporativos.
- Output: `data/ssot/canonical_us.parquet`.
- Regras: incluir campos operacionais (`close_operational`, `split_factor`, `dividend_rate`, SPC metrics).

### T-011 — Macro expandido US

- Input: series FRED.
- Output: `data/ssot/macro_us.parquet`, `data/features/macro_features_us.parquet`.
- Regras: sincronizacao temporal, politicas de fill documentadas, sem lookahead.

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
- `spc_xbar` (float)
- `spc_ucl` (float)
- `spc_lcl` (float)
- `quality_flag` (string)
- `blacklist_level` (string nullable: HARD/SOFT/NULL)
- `blacklist_reason` (string nullable)

### `data/ssot/canonical_us.parquet`

Colunas minimas:

- `date` (date)
- `ticker` (string)
- `close_operational` (float)
- `split_factor` (float)
- `dividend_rate` (float nullable)
- `spc_xbar` (float)
- `spc_ucl` (float)
- `spc_lcl` (float)
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
4. SPC completo por ticker (xbar/ucl/lcl) e blacklist HARD/SOFT documentada.
5. Resiliencia API ativa (retry exponencial + fallback) com logs.
6. Evidencia anti-lookahead registrada (incluindo regra D+1 para cross-market quando aplicavel).

## Riscos e mitigacoes

- Risco: composicao em modo snapshot (sem historico por effective_date na T-006).
  - Mitigacao: risco aceito em D-005; registrar limitacao no coverage.json e manter trilha para evolucao futura.
- Risco: lacunas de cobertura em alguns tickers.
  - Mitigacao: classificar lacuna, aplicar blacklist SOFT/HARD, nao forcar imputacao opaca.
- Risco: latencia/rate limit no Polygon.
  - Mitigacao: batching, retry exponencial, checkpoint incremental.
- Risco: drift de schema entre steps.
  - Mitigacao: contratos de schema fixos e validacao antes de escrever parquet final.
