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
- feat: T-007 — ingestão massiva OHLCV + dividendos + splits (Polygon) para universo do snapshot iShares (T-006), com dedupe (date,ticker) + evidências de cobertura. Artefatos: scripts/t007_ingest_us_market_data_raw.py (ref: D-003, D-005)
