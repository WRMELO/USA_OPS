# HANDOFF — Phase 4 ML Trigger US

> Data: 2026-03-18
> Chat anterior: cc74cd13-cd0d-4744-88a2-4ad6caa23ab8
> Autor: CTO
> Destino: Novo chat para Phase 4

---

## 1. Onde estamos

A Fábrica US (USA_OPS) completou as Phases 0-3:

| Phase | Status | Resultado |
|-------|--------|-----------|
| Phase 0 — Fundação | DONE | Repo, libs, governança |
| Phase 1 v2 — Dados | DONE | Pipeline de dados auditado (Gemini+Kimi PASS) |
| Phase 2 — Motor M3 + Features | DONE | Scoring, features, labels com anti-lookahead |
| Phase 3 — Backtest | DONE | **Winner C4 declarado** (T-024, D-021) |

**Winner US**: C4 — TopN=20, Cadence=10, K=10, cap=6%, k_damp=0.0, min_market_cap=300M, friction=2.5bps, settlement=T+1

**Métricas HOLDOUT (2023-01-02 a 2026-03-16)**:
- CAGR: 42.14%
- MDD: -40.12%
- Concentração top1 mediana: 6.29%

---

## 2. O que falta (Phase 4 — ML Trigger US)

Conforme ROADMAP.md:

| Task | Descrição | Status |
|------|-----------|--------|
| T-025 | XGBoost US — treinar classificador binário (y_cash) no TRAIN (2018-2022), avaliar no HOLDOUT | BACKLOG |
| T-026 | Ablação threshold + histerese — encontrar thr, h_in, h_out ótimos | BACKLOG |
| T-027 | Integrar trigger no motor — comparar C4 puro vs C4 + ML trigger | BACKLOG |
| T-028 | Auditoria forense Phase 4 — Gemini + Kimi | BACKLOG |

**Objetivo**: determinar se o ML trigger agrega valor ao motor C4. Se não agregar, manter motor puro (lição LL-PH10-007 do BR).

---

## 3. Artefatos disponíveis para Phase 4

### Dataset pronto para ML
- `data/features/dataset_us_labeled.parquet` — features + labels, walk-forward split embutido
- `config/feature_guard_us.json` — allowlist de features (anti-snooping)
- `data/features/labels_us.parquet` — labels oracle (drawdown SP500, threshold calibrado no TRAIN)

### Walk-forward split
- **TRAIN**: 2018-01-02 a 2022-12-30
- **HOLDOUT**: 2023-01-02 a 2026-03-16

### Referência BR (RENDA_OPS)
- Winner BR: C060X — XGBoost (thr=0.22, h_in=3, h_out=2, top_n=10)
- 35 features, XGBClassifier com hiperparâmetros em `config/ml_model.json`
- Modelo persistido como `.ubj`

### Motor de backtest
- `backtest/run_backtest_variants_us.py` — já suporta C1/C2/C3/C4
- Precisa ser estendido para aceitar sinal ML (y_proba_cash) como camada adicional

---

## 4. Documentos obrigatórios para o CTO ler no novo chat

Antes de orientar o Architect, o CTO DEVE ler:

1. `docs/CORPUS_FABRICA_US.md` — TODO o conhecimento acumulado (Phases 0-3), lições, erros, divergências
2. `docs/CORPUS_FABRICA_BR.md` — experiência BR (especialmente seções 5.3 Motor de venda e 6/7 Lessons Learned)
3. `GOVERNANCE.md` — regras vigentes, especialmente §7 (gate de paridade D-012)
4. `ROADMAP.md` — backlog atualizado
5. `config/winner_us.json` — declaração canônica do winner
6. `config/feature_guard_us.json` — features aprovadas

---

## 5. Regras de governança vigentes

- **D-010**: Execução task-a-task com ciclo completo (CTO→Architect→Owner→Executor→Auditor→Curator)
- **D-012**: Duplo bloqueio — CTO deve incluir `parity_cto_check` em toda orientação com threshold/gate; Architect rejeita se ausente
- **D-009**: SPC alimenta venda defensiva, não exclui tickers
- **D-013**: stale_tickers rolling (anti-lookahead)
- **Idioma**: Owner responde em português; CTO fala em português

---

## 6. Riscos conhecidos para Phase 4

1. **Overfitting no TRAIN**: universo US tem ~2.090 tickers operacionais vs ~906 no BR. Mais features ≠ melhor modelo.
2. **Label desbalanceado**: `y_cash` baseado em drawdown do SP500; verificar proporção TRAIN antes de treinar.
3. **Anti-lookahead obrigatório**: features já estão com shift(1), mas o Auditor deve re-verificar no XGBoost (feature importance não pode vazar informação futura).
4. **Walk-forward estrito**: treinar SOMENTE no TRAIN, jamais tocar HOLDOUT até avaliação final.
5. **Se ML não agregar**: manter motor puro C4 — não forçar complexidade.

---

## 7. Decisões pendentes para o Owner

Nenhuma decisão pendente. Phase 4 pode iniciar com a T-025.

---

## 8. Como iniciar o novo chat

```
/cto Leia docs/CORPUS_FABRICA_US.md, docs/CORPUS_FABRICA_BR.md, GOVERNANCE.md, 
ROADMAP.md e docs/HANDOFF_PHASE4.md. Estamos iniciando a Phase 4 (ML Trigger US). 
Oriente o Architect no Modo 2 para a T-025.
```
