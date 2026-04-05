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
- feat: T-030 — painel diário HTML (USD/NYSE) com Plotly (252 pregões + Base100) + resumo DFC/Balanço mínimo (ref: D-024)
- feat: T-031 — servidor/lançador (porta 8788) + catch-up automático de pregões NYSE + atalho Desktop (ref: D-024)
- feat: T-032 — duplo-caixa US (T+1) no painel + salvamento de boletim (data/real) via lançador (ref: D-024)

## 2026-03-19

- feat: T-034 — blindagem do motor + operational_window + ingestão incremental diária (ref: D-025, D-026). Artefatos: `data/ssot/operational_window.parquet`, `data/ssot/operational_market_data_raw.parquet`, `pipeline/00_incremental_ingest.py`, `pipeline/rebuild_operational_window.py`, `.git/hooks/pre-commit`, `tools/pre_commit_motor_guard.sh`, `tools/install_git_hooks.sh`, `docs/OPERACAO_DADOS.md`, `docs/T034_REEXECUCAO_E_HIGIENE.md`. Tag: `v1.0.0-motor-us`
- feat: T-037 — painel diário US reescrito no formato exato do painel BR (RENDA_OPS): CSS/JS/estrutura idêntica, adaptações US (USD, NYSE, T+1, Top-20, Drawdown%, Base1 sem CDI), seções adicionadas (Carteira Comprada, Carteira Atual D-1, Top-20, Card de Venda, Duplo-Caixa + Balanço + DFC), seções removidas (Resumo, Reconciliação, Proventos), servidor `/salvar` retornando `paths[]`. Artefatos: `pipeline/painel_diario.py`, `pipeline/servidor.py`, `data/daily/painel_2026-03-19.html`, `data/cycles/2026-03-19/painel.html` (ref: D-027)

## 2026-03-20

- fix: D-028 — corrigir fórmula de Patrimônio Inicial no Balanço Simplificado: eliminar CAIXA_ORIGINAL (valor de mercado dinâmico), usar Aportes acumulados - Retiradas acumuladas (capital líquido injetado). Label renomeado para "Capital Líquido Aportado". Artefatos: `pipeline/painel_diario.py` (ref: D-028)

## 2026-03-22

- fix: T-038 — separar Top-20 informativo (top20_by_score) da carteira ativa no decision JSON e no painel. Artefatos: `pipeline/09_decide.py`, `pipeline/painel_diario.py` (ref: D-029)

## 2026-03-26

- fix: T-039 — mover venda defensiva do step 09 para o painel usando carteira real (`build_lot_ledger`) e eliminar sugestões fantasmas para posições zeradas. Artefatos: `pipeline/09_decide.py`, `pipeline/painel_diario.py` (ref: D-033, D-032)

## 2026-04-01

- 2026-04-01 | T-040 | feat: desacoplar semântica temporal nos artefatos operacionais (exec_day, market_day, trade_day) — D-040, R-022. Toca painel_diario.py (blindado). MOTOR_OVERRIDE.

## 2026-04-02

- 2026-04-02 | T-041 | fix: resiliência FRED no step 05 — fallback para macro_us existente + gate D-2 (D-041, paridade D-027 BR)
- 2026-04-02 | T-042 | fix: rebalance por contagem relativa de pregões desde último rebalance, com fallback bootstrap e persistência em last_rebalance.json (D-043, R-018, D-032). Toca 09_decide.py (blindado). MOTOR_OVERRIDE.
- 2026-04-02 | T-042r | fix: cura documental do rebalance relativo após validação do Auditor; decision_2026-04-02.json e last_rebalance.json alinhados ao estado operacional. Artefatos: data/daily/decision_2026-04-02.json, data/daily/last_rebalance.json (ref: D-043).
- 2026-04-02 | T-043 | feat: parâmetro dry_run em run() de 09_decide.py para isolar escrita de disco em testes/auditorias (D-044, D-032). Toca 09_decide.py (blindado). MOTOR_OVERRIDE.

## 2026-04-03

- feat: T-045 — SSOT ledger imutável (D-045). Novo pipeline/ledger.py com event sourcing financeiro. Migração de 9 boletins + gap 01/04. Painel e servidor refatorados para ler/escrever no ledger. Boletins preservados como artefato de exibição.
- fix: T-046 — Corrige injeção de caixa fantasma por SETTLEMENT sem ref_id e duplicação de eventos no servidor (D-045). Auditoria forense Gemini/Kimi.
- chore: T-047-HF — commit com MOTOR_OVERRIDE + tag v1.4.0-motor-us selando T-041/T-042/T-043/T-045/T-046 (D-041/D-043/D-044/D-045). Governança Git exigiu consolidação.

## 2026-04-04

- T-052: Separar pipeline em duas fases (--ingest-only / --decision-only) + --dry-run + orquestrador run_all.sh (D-052)
- audit: T-052 — auditoria retroativa pós-commit (Gemini PASS + Kimi PASS + Auditor Principal FAIL governança -> saneado em T-053, D-053)
- chore: T-053 — saneamento de governança pós-auditoria retroativa T-052. Tags v1.8.0-motor + v1.5.0-motor-us criadas. R-024 adicionada ao corpus. (D-053)
