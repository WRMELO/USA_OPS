# CHANGELOG — USA_OPS

## 2026-03-07

- chore: initial commit — estrutura do repo operacional US (Russell 1000 + SmallCap 600)
- docs: criar trinca de governança (GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md)
- docs: criar PLANO_USA_OPS.md (plano de execução completo)
- docs: copiar CORPUS_FABRICA_BR.md como referência

## 2026-03-16

- chore: T-001 setup do repositório (venv, requirements.txt fixo, skeleton lib/ e pipeline/)
- ref: D-001
- feat: T-002 — portar engine/metrics/io + criar adapters US (PolygonAdapter + FredAdapter). Artefatos: lib/engine.py, lib/metrics.py, lib/io.py, lib/adapters.py (ref: D-001, D-003)
- docs: T-003 — criar MANIFESTO_ORIGEM.json (proveniência + SHA256). Artefatos: MANIFESTO_ORIGEM.json (ref: D-001)
- docs: T-004 — confirmar corpus BR como referência (cópia + SHA256 + manifesto). Artefatos: docs/CORPUS_FABRICA_BR.md, MANIFESTO_ORIGEM.json (ref: D-001)
- docs: T-005 — criar SPEC do pipeline US (schemas, fontes, riscos, anti-lookahead/anti-survivorship). Artefatos: docs/SPEC_PIPELINE_US.md (ref: D-003)
- feat: T-006 — gerar composição histórica (proxy ETF) R1000/IWB e SP600/IJR com gate rígido de cobertura + evidências. Artefatos: config/index_proxies_us.json, scripts/t006_build_index_compositions.py (ref: D-004)
- fix: T-006 — reescrever composição do universo via CSV público iShares (IWB/IJR) em modo snapshot + evidências. Artefatos: config/index_proxies_us.json, scripts/t006_build_index_compositions.py, docs/SPEC_PIPELINE_US.md (ref: D-005)
- fix: T-006 — filtrar tickers inválidos (ex.: '-') do snapshot iShares + evidenciar descartes/pesos nulos + corrigir SHA256 do DECISION_LOG no MANIFESTO_ORIGEM. Artefatos: scripts/t006_build_index_compositions.py, MANIFESTO_ORIGEM.json (ref: D-005)
- feat: T-007 — ingestão massiva OHLCV + dividendos + splits (Polygon) para universo do snapshot iShares (T-006), com dedupe (date,ticker) + evidências de cobertura. Artefatos: scripts/t007_ingest_us_market_data_raw.py, data/ssot/us_market_data_raw.parquet (2.87M rows), data/ssot/t007_ingestion_report.json, data/ssot/t007_failures.json (ref: D-003, D-005)
- feat: T-008a — Reference data US por ticker (active/delisted, list_date, ticker changes) via Polygon/Massive + evidências. Artefatos: scripts/t008a_ingest_ticker_reference_us.py, data/ssot/ticker_reference_us.parquet, data/ssot/t008a_reference_report.json (ref: D-006)
- feat: T-008 — Qualidade SPC por ticker + blacklist HARD/SOFT + universo operacional. Artefatos: scripts/t008_quality_spc_and_blacklist.py, config/blacklist_us.json, data/ssot/us_universe_operational.parquet, data/ssot/t008_quality_report.json (ref: D-002)
- feat: T-009 — Exclusão de tickers com BDR na B3 (anti-sobreposição) + evidências. Artefatos: scripts/t009_exclude_bdrs.py, data/ssot/bdr_exclusion_list.json, docs/SPEC_PIPELINE_US.md (ref: D-001)
- feat: T-010 — SSOT canônico US (canonical_us.parquet) consolidando raw+SPC+reference+exclusão BDR + evidências. Artefatos: scripts/t010_build_canonical_us.py, data/ssot/canonical_us.parquet, data/ssot/t010_canonical_report.json, docs/SPEC_PIPELINE_US.md (ref: D-001)
- feat: T-011 — Macro expandido US (FRED) + features sem lookahead (shift(1)) + evidências. Artefatos: scripts/t011_ingest_macro_us.py, data/ssot/macro_us.parquet, data/features/macro_features_us.parquet, data/ssot/t011_macro_report.json (ref: D-003)

## 2026-03-17

- chore: T-PURGE — purga física artefatos Phase 1 v1 + archive auditorias + reset SPEC/ROADMAP p/ v2 (ref: D-008)
- feat: T-006v2 — universo histórico anual via Polygon /v3/reference/tickers + evidências (ref: D-007)
- feat: T-007v2 — ingestão OHLCV+dividends+splits com adjusted=False + chunks retomáveis + report/failures (ref: D-007)
- feat: T-008av2 — reference data por ticker (details+events) no universo v2 com chunks+report+failures (ref: D-007)
- feat: T-008v2 — SPC Shewhart completo (I-MR + Xbar-R) + blacklist HARD/SOFT no universo v2 (ref: D-007)
- fix: T-008v2-FIX — remover outlier_rate da blacklist SOFT (SOFT apenas history_days<252) e alinhar SPEC ao RENDA_OPS (ref: D-009)
- feat: T-009v2 — exclusão de tickers com BDR na B3 (anti-sobreposição) no universo v2 + evidências. Artefatos: scripts/t009_exclude_bdrs_v2.py, data/ssot/bdr_exclusion_list.json, data/ssot/t009v2_bdr_exclusion_report.json (ref: D-007, D-001)
- feat: T-010v2 — SSOT canônico US v2 consolidando raw+SPC+reference+exclusão BDR + evidências. Artefatos: scripts/t010_build_canonical_us_v2.py, data/ssot/canonical_us.parquet, data/ssot/t010v2_canonical_report.json (ref: D-007, D-001)
- feat: T-011v2 — Macro US com `outer merge -> ffill -> filter` + features shift(1) + evidências. Artefatos: scripts/t011_ingest_macro_us_v2.py, data/ssot/macro_us.parquet, data/features/macro_features_us.parquet, data/ssot/t011v2_macro_report.json (ref: D-007, D-003)
- fix: T-012-FIX — remover gate quantitativo arbitrário (median_tickers_ge_3500) do scoring M3-US e manter métrica apenas como evidência. Artefatos: scripts/t012_compute_scores_m3_us.py, data/features/scores_m3_us.parquet, data/features/t012_scores_report.json (ref: D-011, D-012, D-010)
- feat: T-013 — feature engineering US (macro shiftado + SPC/M3 cross-section + equity proxy sem lookahead) + feature guard + evidências. Artefatos: scripts/t013_build_features_us.py, config/feature_guard_us.json, data/features/dataset_us.parquet, data/features/t013_features_report.json (ref: D-002, D-009, D-010, D-012)

## 2026-03-18

- feat: T-014 — labels de regime US (oracle drawdown-based no S&P 500 via FRED, threshold calibrado no TRAIN, walk-forward split) + dataset rotulado + evidências. Artefatos: scripts/t014_build_labels_us.py, data/features/labels_us.parquet, data/features/dataset_us_labeled.parquet, data/features/t014_labels_report.json (ref: D-002, D-009, D-010)
- fix: T-012-FIX2 — stale_tickers rolling por dia (elimina lookahead no backtest; preserva equivalência no último dia via gate) (ref: D-013)
- chore: T-012-FIX2-MANIFEST-SHA-ALIGN — atualização de SHA256 no MANIFESTO_ORIGEM.json após re-execução (t012, t013, t014). Artefatos: MANIFESTO_ORIGEM.json
