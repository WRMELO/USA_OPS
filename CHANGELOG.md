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
- feat: T-015 — framework de backtest US (C1/C2/C3, T+1, custos, outputs CSV/JSON/HTML + report com gates). Artefatos: backtest/run_backtest_variants_us.py, backtest/plot_t015_plotly.py, backtest/results/summary_t015_variants.csv, backtest/results/summary_t015_variants.json, backtest/results/t015_backtest_report.json, backtest/results/plot_t015_equity_comparison.html (ref: D-002)
- docs: D-014 — registrar decisão do Owner de aceitar T-015 como PASS (finding do auditor era leitura incompleta do código; C1 colapsando é evidência de rotação alta no universo US). (ref: D-014)
- feat: T-016 — venda defensiva permanente no backtest US (camada 0 split adjustment + severity score 0–6 + vendas 25/50/100 + quarentena + events + plots). Artefatos: backtest/run_backtest_variants_us.py, backtest/results/t016_backtest_report.json, backtest/results/events_defensive_sells.csv, backtest/results/events_split_adjustments.csv, backtest/plot_t015_plotly.py (ref: D-002)
- fix: T-016-FIX — backtest US: split event-based (sf_D/sf_{D-1}) para camada 0 + equity_base100 nas curvas + métricas no report. Artefatos: backtest/run_backtest_variants_us.py, backtest/plot_t015_plotly.py, backtest/results/t016_backtest_report.json (ref: D-015)
- feat: T-017 — ablação TopN × Cadence × K no backtest US (grade de parâmetros + summary CSV/JSON + report com gates). Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_report.json (ref: D-002)
- fix: T-017-FIX — ablação T-017 com filtro min_market_cap (>=300M USD) no universo operacional (aplicado no date do score, anti-lookahead) + novos summaries/report. Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_summary.csv, backtest/results/t017_ablation_summary.json, backtest/results/t017_ablation_report.json (ref: D-016, D-017)
- fix: T-017-FIX2 — ablação ampliada (TopN=[10,15,20,25], Cad=[5,10,21], K=[10,15,20,30]) com filtro min_market_cap=300M (anti-lookahead no date do score) + novos summaries/report. Artefatos: backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_summary.csv, backtest/results/t017_ablation_summary.json, backtest/results/t017_ablation_report.json, MANIFESTO_ORIGEM.json (ref: D-018, D-016)
- fix: T-016-FIX2 — corrigir split_event_wide no backtest: derivar ratio do preço raw (px_{D-1}/px_D) ao invés de sf_D/sf_{D-1} para preservar valor econômico por construção. 27 eventos divergentes corrigidos (15 no holdout). Re-executada ablação completa (72 combos). Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_summary.csv, backtest/results/t017_ablation_summary.json, backtest/results/t017_ablation_report.json, MANIFESTO_ORIGEM.json (ref: D-019, D-015)
- feat: T-018 — variante C4 (score-weighted com dampening + cap de concentração + trims no rebalanceamento) + ablação dedicada T-018 + plots Plotly. Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t018_ablation_us.py, backtest/plot_t018_plotly.py, backtest/results/t018_ablation_summary.csv, backtest/results/t018_ablation_summary.json, backtest/results/t018_ablation_report.json, backtest/results/plot_t018_*.html, backtest/results/events_concentration_trims.csv, MANIFESTO_ORIGEM.json (ref: D-019)
- feat: T-021 — análise de concentração + drawdown por ticker (decomposição MDD, série temporal concentração, efetividade do cap, plots Plotly). Artefatos: backtest/run_t021_concentration_analysis.py, backtest/results/t021_concentration_report.json, backtest/results/t021_daily_concentration.csv, backtest/results/t021_drawdown_decomposition.csv, backtest/results/plot_t021_*.html, MANIFESTO_ORIGEM.json (ref: D-002)
- feat: T-022 — dual acid window US (pior drawdown HOLDOUT em SP500 + proxy Russell1000 via FRED, min 6 meses) + métricas do motor nessas janelas + plots Plotly. Artefatos: backtest/run_t022_dual_acid_window_us.py, backtest/results/acid_analysis_us.json, backtest/results/plot_t022_*.html, MANIFESTO_ORIGEM.json (ref: D-002)
- feat: T-024 — declaração canônica do winner US (C4 TopN=20 Cad=10 K=10 cap=6% min_market_cap=300M) + métricas HOLDOUT + acid test + evidências e SHA256. Artefatos: config/winner_us.json, MANIFESTO_ORIGEM.json (ref: D-021, D-020)
- docs: CORPUS_FABRICA_US.md — consolidação de toda a experiência Phases 0-3, lições certas/erradas, divergências justificadas, lições cruzadas BR↔US, checklist Phase 4+. Artefatos: docs/CORPUS_FABRICA_US.md, MANIFESTO_ORIGEM.json, GOVERNANCE.md §8 atualizado
- docs: HANDOFF_PHASE4.md — documento de continuidade para novo chat (onde estamos, artefatos, regras, riscos, como iniciar). Artefatos: docs/HANDOFF_PHASE4.md, MANIFESTO_ORIGEM.json
- feat: T-025 — treinar XGBoost US (TRAIN-only) e gerar y_proba_cash + config/ml_model_us.json + report com gates (ref: D-002)
- feat: T-026 — ablação thr/h_in/h_out do ML trigger US (seleção TRAIN-only) + config/ml_trigger_us.json + report com gates (ref: D-002)
- feat: T-025v2 — retreinar ML trigger US com features estacionárias (sem _level) + report consolidado (ref: D-022)
- feat: T-027 — comparar C4 puro vs C4+ML trigger (histerese em y_proba_cash) + report consolidado (ref: D-023)
- fix: T-027v2 — corrigir reconciliação do baseline (z_wide 1:1) + adicionar Plotly comparação C4 puro vs C4+trigger (ref: D-023)
- feat: T-029 — pipeline operacional US (steps 01–12) + orquestrador `pipeline/run_daily.py` (ref: D-024)
