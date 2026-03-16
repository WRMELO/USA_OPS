# PLANO DE EXECUÇÃO — USA_OPS (Fábrica US)

> Decisões de referência: D-001, D-002, D-003 (USA_OPS) | D-029 (RENDA_OPS)
> Corpus de lições: docs/CORPUS_FABRICA_BR.md
> Data: 2026-03-07

---

## 1. Contexto e Motivação

A Fábrica BR (RENDA_OPS) opera com sucesso em dry-run na B3, cobrindo 906 tickers (ações BR + BDRs de empresas americanas). Durante 27 decisões, 26+ tasks e 4 auditorias forenses, cristalizou-se uma metodologia completa: governança por trinca, cadeia de skills, motor com venda defensiva (severity score + quarentena + histerese C2 K=15), auditoria forense adversarial e blindagem pós-auditoria.

O Owner decidiu criar uma **Fábrica US independente** para operar diretamente no mercado americano (em USD, via BTG Internacional), com universo **complementar** à BR — sem nenhuma sobreposição de tickers.

### Por que não usar o winner US do AGNO (T122)?

O AGNO desenvolveu um winner US (T122: TopN=5, Cadence=10), mas com **4 problemas não resolvidos**:

1. **Survivorship bias (CRÍTICO)**: universo construído com composição S&P 500 de 2026, não 2018
2. **Sharpe 1.03 < S&P 500 buy-hold 1.37**: motor destrói valor ajustado ao risco
3. **MDD -28.3% viola hard constraint -15%**: sem venda defensiva, drawdowns se acumulam
4. **Sem venda defensiva**: mesmo padrão de falha da Fábrica BR antes da correção (E-03/E-06)

A Fábrica US será construída do zero, aplicando desde o início as lições que levaram 26 tasks para aprender na BR.

---

## 2. Parâmetros Fundamentais

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| **Universo** | Russell 1000 + S&P SmallCap 600 − BDRs B3 | ~1.100 tickers, zero sobreposição com BR (D-001) |
| **Moeda** | USD | Operação direta no mercado US |
| **Liquidação** | T+1 | Simplifica duplo-caixa vs D+2 da BR |
| **Tank (caixa)** | Fed Funds Rate | Equivalente ao CDI da BR |
| **Dados OHLCV** | Polygon.io (~$30/mês) | Composição histórica de índices, API estável (D-003) |
| **Dados macro** | FRED | Mesmo da BR, com resiliência D-027 |
| **Custos** | Dados reais BTG Internacional | Sem assumir zero-fee |
| **Broker** | BTG Internacional (conta do Owner) | API para execução futura |
| **Motor** | A descobrir por backtest comparativo (D-002) | Venda defensiva desde dia 1 |
| **Walk-forward** | TRAIN 2018→2022, HOLDOUT 2023→2026 | Mesmo período do AGNO para comparabilidade |
| **Composição histórica** | Obrigatória (Polygon.io) | Corrige survivorship bias (finding CRÍTICO AGNO) |

---

## 3. Roadmap de Execução

### Phase 0 — Fundação

**Objetivo**: Repo operacional com governança, componentes portáveis e configuração inicial.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-001 | Setup repositório | Git init, .gitignore, .env.example, requirements.txt, venv | Estrutura completa | D-001/D-002/D-003 RENDA_OPS |
| T-002 | Portar componentes agnósticos | Copiar `lib/engine.py`, `lib/metrics.py`, `lib/io.py` do RENDA_OPS. Criar `lib/adapters.py` com PolygonAdapter (novo) + FredAdapter (portado com resiliência D-027) | lib/*.py | L-12: resiliência API dia 1 |
| T-003 | MANIFESTO_ORIGEM.json | Mapear proveniência: USA_OPS → RENDA_OPS → AGNO | MANIFESTO_ORIGEM.json | L-06: rastreabilidade |
| T-004 | Portar corpus BR | Copiar docs/CORPUS_FABRICA_BR.md como referência | docs/CORPUS_FABRICA_BR.md | L-01: consultar antes de agir |

**Gate de saída Phase 0**: repo funcional, `lib/` testável, governança configurada.

---

### Phase 1 — Dados Reais US

**Objetivo**: SSOT canônico US com ~1.100 tickers, livre de survivorship bias, com SPC per-ticker.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-005 | SPEC do pipeline US | Schema, fontes, riscos, anti-lookahead (D+1 para features cross-market) | docs/SPEC_PIPELINE_US.md | LL-PH8-006: pipeline 4 etapas |
| T-006 | Composição histórica Russell 1000 + SmallCap 600 | Via Polygon.io: constituintes por data efetiva (2018–2026). Gerar `index_compositions_historical.parquet` | data/ssot/index_compositions.parquet | Corrige survivorship AGNO |
| T-007 | Ingestão massiva OHLCV US | ~1.600 tickers (pré-exclusão), 2018–2026, via Polygon.io. OHLCV + dividendos + splits. Retry exponencial. | data/ssot/us_market_data_raw.parquet | LL-PH8-007: alinhar start_date; L-12: resiliência |
| T-008 | Qualidade SPC + blacklist | SPC per-ticker (xbar, ucl, lcl), outliers, gaps, cobertura. Blacklist HARD/SOFT. | data/ssot/us_universe_operational.parquet, config/blacklist_us.json | LL-PH8-008: categorias |
| T-009 | Excluir BDRs do universo | Cruzar com RENDA_OPS/data/ssot/bdr_universe.parquet. Remover ~496 tickers. | data/ssot/bdr_exclusion_list.json | D-001: zero sobreposição |
| T-010 | SSOT canônico US | Canonical per-ticker com SPC metrics, close_operational, split_factor, dividend_rate. Universo final ~1.100 tickers. | data/ssot/canonical_us.parquet | Base limpa para tudo |
| T-011 | Macro expandido US | FRED: VIX, DXY, Treasury 10y/2y, Fed Funds, HY/IG OAS spreads. Com resiliência (retry + fallback D-2). | data/ssot/macro_us.parquet, data/features/macro_features_us.parquet | D-027: resiliência FRED |

**Gate de saída Phase 1**: canonical_us.parquet com ~1.100 tickers, macro_features_us.parquet, zero survivorship bias, SPC completo.

---

### Phase 2 — Motor M3-US + Features

**Objetivo**: Scoring M3-US diário + feature engineering para ML, com anti-lookahead estrito.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-012 | Scoring M3-US diário | Z-score cross-section sobre ~1.100 tickers. Campos `score_m3_us_exec` e `m3_rank_us_exec` (shift(1) obrigatório). | data/features/scores_m3_us.parquet | LL-PH8-005: anti-lookahead |
| T-013 | Feature engineering US | Features macro (FRED) + per-ticker (momentum, volatilidade, volume, SPC) + sector rotation + credit spreads (HY/IG OAS). Feature guard obrigatório. | data/features/dataset_us.parquet, config/feature_guard_us.json | LL-PH9-008: feature guard; LL-PH9-002: macro puro insuficiente para US |
| T-014 | Labels de regime US | Oracle drawdown-based sobre S&P 500 (ou Russell 1000 index). Walk-forward TRAIN only. | data/features/labels_us.parquet | T112 AGNO: labels por drawdown |

**Gate de saída Phase 2**: dataset_us.parquet com features + labels, anti-lookahead verificado (max_abs_diff=0.0).

---

### Phase 3 — Backtest Comparativo com Venda Defensiva

**Objetivo**: Descobrir o motor US ideal via backtest comparativo, com venda defensiva desde o dia 1.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-015 | Framework de backtest US | Adaptar `backtest/run_backtest_variants.py` do RENDA_OPS para: USD, T+1, custos BTG, Fed Funds como tank, lotes independentes, concentração. | backtest/run_backtest_variants_us.py | L-14: custos reais mudam conclusão |
| T-016 | **Venda defensiva permanente** | Implementar severity score (SPEC-001 a SPEC-004 do AGNO) como camada permanente em todas as variantes: regime defensivo (market-slope), score 0-6, vendas graduais 25/50/100%, quarentena pós-venda, ajuste de splits obrigatório (camada 0). | Integrado no backtest | E-03/E-06: nunca construir sem venda defensiva; L-15: splits obrigatórios |
| T-017 | Ablação TopN × Cadence × K | Grid: TopN=[5,8,10,12,15], Cadence=[5,10,21], K_histerese=[12,15,20,25]. Com venda defensiva permanente. | backtest/results/ablation_us.csv | D-022: ablação fina determina winner |
| T-018 | Backtest C1 (Top-N puro) | Rebalanceamento diário por ranking. Referência de giro máximo. | backtest/results/curve_C1_us.csv | Referência |
| T-019 | Backtest C2 (histerese K) | Buffer de histerese. Sweep K valores do T-017. | backtest/results/curve_C2_us_K*.csv | D-022: C2 K=15 dominou na BR |
| T-020 | Backtest C3 (só defensiva) | Sem rebalanceamento — só venda defensiva + CAIXA global. Referência de giro mínimo. | backtest/results/curve_C3_us.csv | Referência |
| T-021 | Análise de concentração | Verificar max_concentration_pct por variante. Aplicar regra: concentração-alvo 10% max 15% por ticker. | backtest/results/concentration_us.csv | D-020: concentração 15% max 20% na BR |
| T-022 | Dual acid window | Definir acid_us com critério objetivo (pior drawdown S&P 500/Russell 1000 no HOLDOUT, mínimo 6 meses). | backtest/results/acid_analysis_us.json | Corrige finding ALTO AGNO |
| T-023 | Auditoria forense Phase 3 | Gemini 3.1 Pro (lógica) + Kimi K2.5 (numérico). Barreira sanitária: auditores não participaram do desenvolvimento. | logs/audit_phase3_*.json | L-04: auditoria adversarial indispensável |
| T-024 | Declaração do winner US | `config/winner_us.json` com métricas, config, evidências. | config/winner_us.json | LL-PH10-004: declaração formal |

**Gate de saída Phase 3**: winner US declarado com Sharpe > S&P 500 buy-hold, MDD dentro do constraint, auditoria PASS.

---

### Phase 4 — ML Trigger US

**Objetivo**: Avaliar se ML trigger agrega valor ao motor US. Se sim, integrar. Se não, motor puro.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-025 | XGBoost US | Walk-forward estrito. Features da T-013. Ablação de hiperparâmetros. TRAIN-only selection. | data/models/xgb_us.ubj, config/ml_model_us.json | D-009: modelo persistido |
| T-026 | Ablação threshold + histerese | Sweep thr × h_in × h_out. Comparar Sharpe, MDD, switches. | data/features/trigger_ablation_us.parquet | D-022: ablação fina |
| T-027 | Integrar trigger no motor | Se Sharpe(motor+trigger) > Sharpe(motor puro) E MDD melhora: adotar. Senão: motor puro. | config/winner_us.json (atualizado) | LL-PH10-007: não forçar trigger |
| T-028 | Auditoria forense Phase 4 | Gemini + Kimi no motor completo (com ou sem trigger). | logs/audit_phase4_*.json | L-04 |

**Gate de saída Phase 4**: decisão trigger sim/não documentada. winner_us.json finalizado.

---

### Phase 5 — Motor Operacional

**Objetivo**: Pipeline diário funcional, painel HTML, servidor autônomo.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-029 | Pipeline steps 01–12 | Adaptar do RENDA_OPS: ingestão via Polygon (01-03), canonical US (04), macro FRED (05), scores M3-US (06), features (07), predição XGBoost (08), decisão (09), curva (10), reconciliação (11), painel (12). | pipeline/*.py | L-08: steps idempotentes |
| T-030 | Painel diário HTML | Adaptar painel_diario.py para USD, T+1, horários NYSE. Plotly (252 pregões + Base 100). Balanço + DFC em USD. Proventos trimestrais automáticos. | pipeline/painel_diario.py | D-016: painel único; D-023: proventos |
| T-031 | Servidor/lançador | Porta 8788. Catch-up com calendário NYSE. Catch-up automático de pregões faltantes. | pipeline/servidor.py, iniciar.sh | D-017/D-026: lançador + catch-up |
| T-032 | Duplo-caixa (T+1) | Simplificado: T+1 vs D+2 da BR. Transferência Contábil→Livre mais rápida. | pipeline/painel_diario.py | D-016: duplo-caixa |

**Gate de saída Phase 5**: pipeline roda end-to-end, painel funcional, servidor operacional.

---

### Phase 6 — Blindagem e Operação

**Objetivo**: Proteger motor auditado, validar em dry-run, iniciar operação real.

| ID | Task | Detalhe | Artefatos | Lesson BR |
|----|------|---------|-----------|-----------|
| T-033 | Auditoria forense final | Gemini + Kimi no motor operacional completo. | logs/audit_final_*.json | D-015/D-025 |
| T-034 | Blindagem | Tag `v1.0.0-motor-us` + pre-commit hook. Arquivos protegidos: painel_diario.py, ingestão, canonical. | .git/hooks/pre-commit | D-025: blindagem técnica |
| T-035 | Dry-run (5 dias) | Simulação pré-operacional. Checklist diário. Registrar em CICLO_DIARIO.md. | CICLO_DIARIO.md, data/real/*.json | RENDA_OPS: 3 dias de validação |
| T-036 | Operação real | Após dry-run PASS. Integrar com API BTG Internacional. | — | Fluxo comprovado na BR |

**Gate de saída Phase 6**: motor blindado, dry-run PASS, operação real iniciada.

---

## 4. Checklist Anti-Regressão (consultar antes de cada Phase)

Derivado do corpus RENDA_OPS + AGNO:

- [ ] Consultar `docs/CORPUS_FABRICA_BR.md` antes de iniciar a phase
- [ ] Anti-lookahead estrito: `shift(1)` em toda feature, verificar max_abs_diff=0.0
- [ ] Walk-forward: TRAIN 2018→2022, HOLDOUT 2023→2026 (não alterar)
- [ ] Feature guard obrigatório em toda task ML
- [ ] Composição histórica de índices (anti-survivorship bias)
- [ ] Venda defensiva permanente (severity score + quarentena) em toda variante de backtest
- [ ] Ajuste de splits como camada 0 (antes de qualquer cálculo de posição)
- [ ] Backtest com custos reais (BTG Internacional) e liquidação T+1
- [ ] Dual acid window (definir com critério objetivo)
- [ ] Auditoria forense (Gemini + Kimi) antes de declarar winner e antes de blindar
- [ ] Resiliência de APIs externas (retry exponencial + fallback) desde o dia 1
- [ ] Validação de entrada para dados manuais (anti-ticker errado)
- [ ] MANIFESTO_ORIGEM atualizado a cada cópia/extração
- [ ] Nenhuma task marcada DONE sem PASS do Auditor

---

## 5. Estimativa de Esforço

| Phase | Estimativa | Dependência |
|-------|-----------|-------------|
| Phase 0 — Fundação | 1 dia | Nenhuma |
| Phase 1 — Dados | 2-3 dias | Phase 0 + chave Polygon.io |
| Phase 2 — Motor + Features | 2 dias | Phase 1 |
| Phase 3 — Backtest | 2-3 dias | Phase 2 |
| Phase 4 — ML Trigger | 2 dias | Phase 3 |
| Phase 5 — Operacional | 2 dias | Phase 4 |
| Phase 6 — Blindagem | 1 dia | Phase 5 |
| **Total estimado** | **12-14 dias** | — |

---

## 6. Riscos Identificados

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| Polygon.io não cobre todos os ~1.600 tickers | Universo reduzido | Fallback: Yahoo Finance para tickers faltantes |
| Composição histórica incompleta | Survivorship bias parcial | Documentar lacunas, usar melhor disponível |
| Motor US não bate buy-hold (como no AGNO) | Fábrica sem alfa | Venda defensiva + features mais ricas (sector, credit) devem resolver; se não resolver, documentar e decidir |
| Rate limits Polygon.io na ingestão massiva | Pipeline lento | Retry + batching + cache local |
| BTG Internacional sem API documentada | Bloqueio na Phase 6 | Dry-run primeiro; resolver API em paralelo |
| "Pressa do CTO" (lição BR) | Erros por precipitação | Consultar corpus antes de cada phase; auditoria antes de avançar |

---

## 7. Artefatos de Proveniência

| Componente USA_OPS | Origem | Tipo |
|-------------------|--------|------|
| lib/engine.py | RENDA_OPS/lib/engine.py | copy |
| lib/metrics.py | RENDA_OPS/lib/metrics.py | copy |
| lib/io.py | RENDA_OPS/lib/io.py | copy |
| lib/adapters.py (FredAdapter) | RENDA_OPS/lib/adapters.py | extract |
| lib/adapters.py (PolygonAdapter) | novo | new |
| backtest/run_backtest_variants_us.py | RENDA_OPS/backtest/run_backtest_variants.py | rewrite |
| docs/CORPUS_FABRICA_BR.md | RENDA_OPS/docs/CORPUS_FABRICA_BR.md | copy |
| GOVERNANCE.md | RENDA_OPS/GOVERNANCE.md | rewrite |
| pipeline/*.py | RENDA_OPS/pipeline/*.py | rewrite |
| Venda defensiva (severity score) | AGNO SPECs 001-004 | rewrite |
