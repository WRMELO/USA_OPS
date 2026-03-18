# ROADMAP — USA_OPS

## Legenda de status

- `BACKLOG` — planejada, não iniciada
- `IN_PROGRESS` — em andamento
- `DONE` — concluída e auditada
- `BLOCKED` — aguardando dependência

---

## Phase 0 — Fundação

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-001 | Setup repositório (git, venv, deps) | DONE | D-001 |
| T-002 | Portar componentes agnósticos do RENDA_OPS | DONE | D-001 |
| T-003 | MANIFESTO_ORIGEM.json | DONE | D-001 |
| T-004 | Copiar corpus BR como referência | DONE | D-001 |

## Phase 1 — Dados Reais US (v1 superseded)

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-005 | SPEC do pipeline US | DONE | D-003 |
| T-006 | Composição do universo via snapshot iShares (IWB/IJR) + evidências | DONE (SUPERSEDED) | D-005, D-007, D-008 |
| T-007 | Ingestão massiva OHLCV US (Polygon.io) | DONE (SUPERSEDED) | D-003, D-005, D-007, D-008 |
| T-008a | Reference data US por ticker (Ticker Details + Ticker Events) | DONE (SUPERSEDED) | D-006, D-007, D-008 |
| T-008 | Qualidade SPC + blacklist | DONE (SUPERSEDED) | D-002, D-007, D-008 |
| T-009 | Excluir BDRs do universo | DONE (SUPERSEDED) | D-001, D-007, D-008 |
| T-010 | SSOT canônico US | DONE (SUPERSEDED) | D-001, D-007, D-008 |
| T-011 | Macro expandido US (FRED) | DONE (SUPERSEDED) | D-003, D-007, D-008 |
| T-PURGE | Purga física dos artefatos Phase 1 v1 + archive auditorias | DONE | D-008 |
| T-006v2 | Universo histórico anual via Polygon `/v3/reference/tickers` | DONE | D-007 |
| T-007v2 | Ingestão OHLCV com `adjusted=False` | DONE | D-007 |
| T-008av2 | Reference data US reprocessado no universo v2 | DONE | D-007 |
| T-008v2 | SPC Shewhart completo (I-MR + Xbar-R) | DONE | D-007, D-009 |
| T-009v2 | Exclusão BDR reexecutada no universo v2 | DONE | D-007 |
| T-010v2 | Canonical US com `close_raw` + `close_operational` dinâmico | DONE | D-007 |
| T-011v2 | Macro US com `outer merge -> ffill -> filter` | DONE | D-007 |

## Phase 2 — Motor M3-US + Features

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-012 | Scoring M3-US diário | DONE | D-002, D-011, D-012, D-010 |
| T-013 | Feature engineering US | DONE | D-002, D-009, D-010, D-012 |
| T-014 | Labels de regime US | DONE | D-002, D-009, D-010 |

## Phase 3 — Backtest Comparativo com Venda Defensiva

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-015 | Framework de backtest US | DONE | D-002 |
| T-016 | Venda defensiva permanente | DONE | D-002, D-015 |
| T-017 | Ablação TopN × Cadence × K (grade inicial, universo filtrado) | DONE | D-002, D-016, D-017 |
| T-017-FIX2 | Ablação ampliada TopN=[10,15,20,25] × Cad=[5,10,21] × K=[10,15,20,30] | DONE | D-018 |
| T-016-FIX2 | Corrigir split_event_wide: ratio derivado do preço raw (preserva valor econômico) | DONE | D-019, D-015 |
| T-018 | Variante C4 (dampening + cap de concentração + trims) + ablação dedicada | DONE | D-019 |
| T-021 | Análise de concentração + drawdown por ticker | DONE | D-002 |
| T-022 | Dual acid window | DONE | D-002 |
| T-023 | Auditoria forense Phase 3 | DONE | D-002, D-020 |
| T-024 | Declaração do winner US | DONE | D-002, D-021 |

## Phase 4 — ML Trigger US

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-025 | XGBoost US (treino TRAIN-only, features v1 com _level) | DONE (SUPERSEDED by T-025v2) | D-002 |
| T-026 | Ablação threshold + histerese (features v1) | DONE (SUPERSEDED by T-025v2) | D-002 |
| T-025v2 | Retreinar XGBoost + ablação com features estacionárias (sem _level) | DONE | D-022 |
| T-027 | Integrar trigger no motor — comparar C4 puro vs C4 + ML trigger | BACKLOG | D-002 |
| T-028 | Auditoria forense Phase 4 | BACKLOG | D-002 |

## Phase 5 — Motor Operacional

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-029 | Pipeline steps 01–12 | BACKLOG | D-001 |
| T-030 | Painel diário HTML (USD, NYSE) | BACKLOG | D-001 |
| T-031 | Servidor/lançador (porta 8788) | BACKLOG | D-001 |
| T-032 | Duplo-caixa (T+1) | BACKLOG | D-001 |

## Phase 6 — Blindagem e Operação

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-033 | Auditoria forense final | BACKLOG | D-002 |
| T-034 | Blindagem (tag + hook) | BACKLOG | D-002 |
| T-035 | Dry-run (5 dias) | BACKLOG | D-001 |
| T-036 | Operação real (BTG Internacional) | BACKLOG | D-001 |
