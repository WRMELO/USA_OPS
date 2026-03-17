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
| T-007v2 | Ingestão OHLCV com `adjusted=False` | BACKLOG | D-007 |
| T-008av2 | Reference data US reprocessado no universo v2 | BACKLOG | D-007 |
| T-008v2 | SPC Shewhart completo (I-MR + Xbar-R) | BACKLOG | D-007 |
| T-009v2 | Exclusão BDR reexecutada no universo v2 | BACKLOG | D-007 |
| T-010v2 | Canonical US com `close_raw` + `close_operational` dinâmico | BACKLOG | D-007 |
| T-011v2 | Macro US com `outer merge -> ffill -> filter` | BACKLOG | D-007 |

## Phase 2 — Motor M3-US + Features

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-012 | Scoring M3-US diário | BACKLOG | D-002 |
| T-013 | Feature engineering US | BACKLOG | D-002 |
| T-014 | Labels de regime US | BACKLOG | D-002 |

## Phase 3 — Backtest Comparativo com Venda Defensiva

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-015 | Framework de backtest US | BACKLOG | D-002 |
| T-016 | Venda defensiva permanente | BACKLOG | D-002 |
| T-017 | Ablação TopN × Cadence × K | BACKLOG | D-002 |
| T-018 | Backtest C1 (Top-N puro) | BACKLOG | D-002 |
| T-019 | Backtest C2 (histerese K) | BACKLOG | D-002 |
| T-020 | Backtest C3 (só defensiva) | BACKLOG | D-002 |
| T-021 | Análise de concentração | BACKLOG | D-002 |
| T-022 | Dual acid window | BACKLOG | D-002 |
| T-023 | Auditoria forense Phase 3 | BACKLOG | D-002 |
| T-024 | Declaração do winner US | BACKLOG | D-002 |

## Phase 4 — ML Trigger US

| Task | Descrição | Status | Decisão |
|------|-----------|--------|---------|
| T-025 | XGBoost US | BACKLOG | D-002 |
| T-026 | Ablação threshold + histerese | BACKLOG | D-002 |
| T-027 | Integrar trigger no motor | BACKLOG | D-002 |
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
