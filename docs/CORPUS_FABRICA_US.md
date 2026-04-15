# CORPUS DE CONHECIMENTO — Fábrica US (USA_OPS)

> Ref: D-059 | Data: 2026-04-14 (atualizado)
> Consolidação da experiência acumulada na Fábrica US (winner C4), das Phases 0-6 e da operação contínua (Phase 7).
> Complementar ao `docs/CORPUS_FABRICA_BR.md` (Fábrica BR, RENDA_OPS).

______________________________________________________________________

## 1. Visão Geral do Projeto

**Repositório**: USA_OPS  
**Ancestral**: RENDA_OPS (Fábrica BR) + AGNO_WORKSPACE (R&D)  
**Winner**: C4 — Score-weighted com cap de concentração (`TopN=20`, `Cad=10`, `K=10`, `cap=6%`, `k_damp=0.0`)  
**Mercado**: US equities (Russell 1000 + S&P SmallCap 600, excluindo tickers com BDR na B3)  
**Moeda**: USD  
**Fase de desenvolvimento**: 2026-03-07 a 2026-03-19  
**Operação real**: desde 2026-03-19 (em curso)  
**Status**: motor C4 puro em produção, com infraestrutura operacional consolidada até D-058

### Métricas do winner (C4, HOLDOUT 2023-01-02 a 2026-03-16)

| Métrica | Valor |
| --- | --- |
| CAGR | 42.14% |
| MDD | -40.12% |
| Max concentração (pontual) | 43.47% |
| Concentração top1 mediana | 6.29% |
| Concentração top1 P90 | 7.59% |
| Avg tickers em carteira | 18.4 |
| Custo total (3.2 anos) | $11,308 |
| Regime defensivo | 67.6% do tempo |
| Trims de concentração | 317 |

### Comparação com Fábrica BR

| Métrica | BR (C2 K=15) | US (C4 cap=6%) | Nota |
| --- | --- | --- | --- |
| CAGR HOLDOUT | 19.2% | 42.14% | US ~2x; universo mais amplo e volátil |
| MDD HOLDOUT | -23.2% | -40.12% | US mais agressivo; universo tem small-caps |
| Concentração máxima | 18.1% | 43.47% (pontual) | Cap de 6% controla mediana, mas picos persistem |
| Regime defensivo | 59.1% | 67.6% | US mais volátil, SPC dispara mais |
| TopN | 10 | 20 | US precisa de mais diversificação |
| Variante | C2 | C4 | US precisou de cap de concentração |
| ML Trigger | Sim (XGBoost) | Não agrega — motor C4 puro | D-023/T-027v2 encerrou discussão |

______________________________________________________________________

## 2. Cronologia de Fases

| Fase | Escopo | Tasks | Decisões-chave | Resultado |
| --- | --- | --- | --- | --- |
| Phase 0 — Fundação | Setup repo, governança, portar libs | T-001 a T-005 | D-001, D-003 | Skeleton funcional |
| Phase 1 v1 — Dados (REPROVADA) | Ingestão via snapshot iShares + SPC incorreto | T-006 a T-011 | D-004, D-005, D-006 | Reprovada por auditoria forense |
| Phase 1 v2 — Dados (APROVADA) | Universo histórico Polygon + SPC Shewhart real | T-006v2 a T-011v2 | D-007, D-008, D-009 | Pipeline de dados aprovado |
| Phase 2 — Motor M3 + Features | Scoring M3, features, labels anti-lookahead | T-012 a T-014 | D-010, D-011, D-012, D-013 | Dataset rotulado auditável |
| Phase 3 — Backtest | Framework, ablação, concentração, acid window | T-015 a T-024 | D-014 a D-021 | Winner C4 declarado |
| Phase 4 — ML Trigger | XGBoost US + ablação + integração C4 | T-025 a T-028 | D-022, D-023 | ML trigger não agrega |
| Phase 5 — Motor Operacional | Pipeline diário, painel, servidor, duplo-caixa | T-029 a T-032 | D-024 | Operação diária funcional |
| Phase 6 — Blindagem e Operação | Auditoria final, blindagem, operational_window, painel BR-format | T-033 a T-037 | D-025, D-026, D-027 | Operação real iniciada em 19/03/2026 |
| Phase 7 — Operação Contínua e Infraestrutura | Ledger SSOT, pipeline bifásico, calendário real, semântica temporal, split automático e errata Base 1 | T-038 a T-061-BASE1 | D-028 a D-058 | Pipeline maduro e robustecido em produção |

**Total consolidado**: 58 decisões (D-001 a D-058), 60+ tasks (incluindo fixes e v2), múltiplas auditorias forenses convergentes (Gemini, Kimi e auditor principal).

______________________________________________________________________

## 3. Arquitetura

### 3.1 Dados (estado atual)

```text
data/
├── ssot/
│   ├── canonical_us.parquet
│   ├── operational_window.parquet
│   ├── operational_market_data_raw.parquet
│   ├── us_market_data_raw.parquet
│   ├── ticker_reference_us.parquet
│   ├── macro_us.parquet
│   ├── blacklist_us.json
│   ├── blacklist_window_us.json
│   └── ledger.jsonl                    -> SSOT financeiro imutável (D-045)
├── features/
│   ├── scores_m3_us.parquet
│   ├── dataset_us.parquet
│   ├── labels_us.parquet
│   └── dataset_us_labeled.parquet
├── daily/
│   ├── decision_YYYY-MM-DD.json
│   ├── painel_YYYY-MM-DD.html
│   ├── last_rebalance.json
│   └── winner_curve_us.parquet
├── cycles/
│   └── YYYY-MM-DD/
│       ├── painel.html
│       └── boletim_preenchido.json
└── real/
    └── YYYY-MM-DD.json
```

### 3.2 SPC Shewhart no canonical_us

| Coluna | Descrição | Uso |
| --- | --- | --- |
| i_value | Individual chart (log-retorno) | z-score e severity |
| i_ucl, i_lcl | Limites do I chart | Regras SPC |
| mr_value, mr_ucl | Moving Range | Regras SPC |
| xbar_value, xbar_ucl, xbar_lcl | Xbar chart | Regras SPC |
| r_value, r_ucl | Range chart | Regras SPC |

Constantes tabeladas (`d2=1.128`, `D4=3.267`), em paridade com o RENDA_OPS.

### 3.3 Backtest (pesquisa)

```text
backtest/
├── run_backtest_variants_us.py
├── run_t017_ablation_us.py
├── run_t018_ablation_us.py
├── run_t021_concentration_analysis.py
├── run_t022_dual_acid_window_us.py
└── results/
```

### 3.4 Motor de venda e decisão (produção)

1. **Camada 0**: Ajuste econômico de split para métricas e lotes.
2. **Camada 0.5**: Detecção automática de split no `painel_diario.py` com ajuste de quantidade/preço e `corporate_actions` no JSON (D-055).
3. **Camada 1**: Venda defensiva permanente via SPC (severity 0-6, vendas 25/50/100, quarentena).
4. **Camada 2**: Rebalanceamento C4 com histerese e cap por ticker.
5. **Camada 2.1**: Disparo de rebalance por **contagem relativa de pregões desde último rebalance**, persistido em `data/daily/last_rebalance.json` (D-043).
6. **Camada 2.5**: Trims de concentração antes das compras.

### 3.5 Diferenças estruturais BR vs US

| Aspecto | BR (RENDA_OPS) | US (USA_OPS) | Justificativa |
| --- | --- | --- | --- |
| Liquidação | D+2 ações, D+1 BDR | T+1 | Regulação SEC |
| Caixa (tank) | CDI | Fed Funds | Banco central local |
| Universo | ~906 tickers | ~4.489 bruto, ~2k operacional | Escala e liquidez |
| TopN | 10 | 20 | Diversificação necessária |
| Variante | C2 | C4 | Controle explícito de concentração |
| Filtro market_cap | Não explícito | >= 300M (dinâmico D-1) | Evitar nano-caps impraticáveis |
| Dados OHLCV | BRAPI | Polygon (adjusted=False) | Controle explícito |
| Semântica temporal | Em convergência com US | `exec_day`, `market_day`, `trade_day` explícitos | D-040/R-022 |

### 3.6 ML Trigger US (resultado final)

| Aspecto | Tentativa 1 | Tentativa 2 | Decisão |
| --- | --- | --- | --- |
| Features | 7 `_level` (não estacionárias) | spreads/retornos/deltas | D-022 |
| Resultado | balanced_accuracy ~0.50 | melhora de proba, mas sem poder útil | D-023 |
| Impacto no motor | Não ajudou | Degrada CAGR e não protege drawdown | Motor C4 puro mantido |

### 3.7 Arquitetura Operacional (atual)

```text
pipeline/
├── run_daily.py               -> modos: --full, --ingest-only, --decision-only
├── 00_incremental_ingest.py   -> ingestão incremental e rebuild da janela
├── rebuild_operational_window.py
├── 01_ingest_macro.py
├── 02_ingest_prices_us.py
├── 03_ingest_reference_us.py
├── 04_build_canonical.py
├── 05_build_macro_expanded.py -> fallback FRED para dados existentes (D-041)
├── 06_compute_scores.py
├── 07_build_features.py
├── 08_predict.py              -> stub (motor puro)
├── 09_decide.py               -> dry_run, last_rebalance, C4
├── 10_extend_curve.py
├── 11_reconcile_metrics.py
├── painel_diario.py           -> venda defensiva real + split automático
├── servidor.py                -> /painel e /salvar
└── ledger.py                  -> event sourcing financeiro

lib/
└── trading_calendar.py        -> calendário real NYSE/B3 via exchange_calendars (D-054)
```

**Blindagem atual do motor**: `v1.6.0-motor-us` (tag em git).

### 3.8 Painel Diário (formato BR adaptado ao US)

| Seção | Conteúdo | Adaptação US |
| --- | --- | --- |
| Carteira Comprada | Posições registradas | USD |
| Carteira Atual (D-1) | Posições x preço D-1 | NYSE |
| Top-20 para compra | Ranking informativo (`top20_by_score`) | Separado da carteira ativa (D-029) |
| Card de Venda | Ações defensivas SPC da carteira real | Paridade com BR |
| Corporate Actions | Split detectado e ajustado automaticamente | D-055 |
| Gráfico Equity 252 | Equity + Drawdown | Sem P(Caixa) |
| Gráfico Base 1 | Patrimônio real acumulado, denominador cumulativo | D-057/D-058 |
| Duplo-Caixa T+1 | Caixa contábil e livre | T+1 |
| Temporalidade | `exec_day`, `market_day`, `trade_day` explícitos | D-040 |

______________________________________________________________________

## 4. Lessons Learned — O que funcionou

### 4.1 Processo

| # | Lição | Evidência |
| --- | --- | --- |
| L-US-01 | Auditoria forense antes de avançar de fase evita construir sobre base podre | D-007 |
| L-US-02 | Purga física de artefatos obsoletos evita contaminação | D-008 |
| L-US-03 | Execução task-a-task com ciclo completo aumenta controle do Owner | D-010 |
| L-US-04 | Bloqueio duplo de paridade evita thresholds arbitrários | D-012 |
| L-US-05 | Owner como detector visual de anomalias é essencial | D-019 |
| L-US-06 | Convergência entre auditores independentes aumenta confiança | D-020 |
| L-US-07 | Corpus BR como baseline acelera e reduz desvio metodológico | GOVERNANCE §8 |

### 4.2 Técnico

| # | Lição | Evidência |
| --- | --- | --- |
| L-US-08 | Universo histórico real é obrigatório para anti-survivorship | D-007 |
| L-US-09 | `adjusted=False` evita corrupção em reprocessamento incremental | D-007 |
| L-US-10 | SPC Shewhart com constantes tabeladas é padrão correto | D-007 |
| L-US-11 | Ratio de split derivado de preço raw é robusto | D-019 |
| L-US-12 | Filtro dinâmico de market_cap é operacional (não metodológico) | D-016 |
| L-US-13 | Cap por ticker é necessário em universo amplo | T-018 |
| L-US-14 | Ablação ampla evita overfitting de grade estreita | D-018 |
| L-US-15 | Acid window com critério objetivo aumenta rigor | T-022 |
| L-US-16 | `stale_tickers` rolling elimina lookahead sem afetar LIVE | D-013 |
| L-US-17 | Cadência >1 dia reduz rotação e custo operacional | T-017 |

### 4.3 Lições da Phase 4 (ML Trigger)

| # | Lição | Evidência |
| --- | --- | --- |
| L-US-18 | Features estacionárias são obrigatórias para ML cross-regime | D-022 |
| L-US-19 | Se ML não agrega, manter motor puro (após testar) | D-023 |
| L-US-20 | Descartar ML sem testar direito também é erro | D-022 |
| L-US-21 | Recall baixo em crise destrói utilidade prática do trigger | D-023 |

### 4.4 Lições da Phase 5-6 (Motor Operacional)

| # | Lição | Evidência |
| --- | --- | --- |
| L-US-22 | Separar canonical de operational_window reduz custo diário | D-026 |
| L-US-23 | Variáveis de ambiente para path canônico podem gerar falhas silenciosas | Bugs 19/03 |
| L-US-24 | Painel BR-format padroniza operação cross-factory | D-027 |
| L-US-25 | Blindagem do motor é pré-condição de estabilidade | D-039 |
| L-US-26 | Primeiro decision deve tratar bootstrap de rebalance | D-043 |
| L-US-27 | Endpoint `/salvar` com `paths[]` melhora rastreabilidade | T-037 |

### 4.5 Lições da Phase 7 (Operação Contínua)

| # | Lição | Evidência |
| --- | --- | --- |
| L-US-28 | **SSOT deve ser evento imutável, não estado computado** | D-045 substituiu boletins como SSOT por `ledger.jsonl` |
| L-US-29 | **Três tempos explícitos evitam confusão operacional** (`exec_day`, `market_day`, `trade_day`) | D-040 e R-022 |
| L-US-30 | **Pipeline bifásico reduz erro de data desatualizada** | D-052 (`--ingest-only` e `--decision-only`) |
| L-US-31 | **Calendário real deve ser infraestrutura, não patch** | D-054 + `lib/trading_calendar.py` |
| L-US-32 | **Rebalance por contagem relativa é robusto a drift do dataset** | D-043 com `last_rebalance.json` |
| L-US-33 | **`dry_run` em decisão protege artefato operacional de auditorias/testes** | D-044 |
| L-US-34 | **Split detectado no painel evita perda fantasma e boletim incorreto** | D-055 (caso POWL) |
| L-US-35 | **Guarda de frescura em ingestão evita retrabalho diário caro** | D-056 (`--ingest-only` com skip quando SSOT fresco) |
| L-US-36 | **Base 1 deve usar denominador cumulativo por ponto para integridade contábil** | D-057/D-058 |

______________________________________________________________________

## 5. Lessons Learned — O que deu errado

### 5.1 Erros de processo

| # | Erro | Causa raiz | Impacto | Ref |
| --- | --- | --- | --- | --- |
| E-US-01 | Phase 1 v1 refeita do zero | Snapshot + SPC incorreto + adjusted=True | Retrabalho estrutural | D-007 |
| E-US-02 | CTO inventou thresholds sem paridade BR | Falta de gate de origem | Tasks falharam por gate arbitrário | D-009/D-011 |
| E-US-03 | Despacho de 3 tasks em lote | Violação de uma orientação por vez | Fluxo confuso | D-010 |
| E-US-04 | FAIL factual do auditor em T-015 | Leitura incompleta | Re-triagem pelo Owner | D-014 |
| E-US-05 | Falta de alerta prévio sobre rotação US | Subestimação do efeito universo amplo | C1 colapsou | D-014 |
| E-US-22 | Agente AI usou `MOTOR_OVERRIDE` sem autorização nominal do Owner | Violação de guardrail operacional | Saneamento de governança retroativo | D-053 / R-024 |

### 5.2 Erros técnicos (histórico até Phase 3)

| # | Erro | Causa raiz | Impacto | Ref |
| --- | --- | --- | --- | --- |
| E-US-06 | `split_factor` cumulativo tratado como event-based | Paridade de código sem paridade semântica | Equity explodiu | D-015 |
| E-US-07 | `sf_D/sf_{D-1}` sem preservar valor econômico | Reset inconsistente do provider | CAGR inflado | D-019 |
| E-US-08 | Universo sem filtro de microcaps | Liquidez ignorada | Ablação irreal | D-016 |
| E-US-09 | `stale_tickers` global (lookahead) | Herdado do BR antigo | Viés no backtest | D-013 |
| E-US-10 | Gate checado antes da escrita completa | Ordem de operações errada | FAIL falso | T-021/T-016 |

### 5.2b Erros técnicos (Phase 4-6)

| # | Erro | Causa raiz | Impacto | Ref |
| --- | --- | --- | --- | --- |
| E-US-11 | Trigger com features não estacionárias | Drift de regime | Modelo cego | D-022 |
| E-US-12 | Trigger v2 com alarmes falsos massivos | Label raro e baixa precisão em crise | Não agregou | D-023 |
| E-US-13 | Decision com tickers fantasmas | Path canônico ausente em execução fora do pipeline | Painel contaminado | Bug 19/03 |
| E-US-14 | Decision HOLD sem bootstrap robusto | Estado parcial de janela operacional | Herança de seleção errada | Bug 19/03 |
| E-US-15 | Painel US parcialmente divergente do BR | Reescrita incompleta inicial | Retrabalho do painel | D-027 |
| E-US-16 | Split POWL não detectado no boletim | Painel não usava `split_factor` no fluxo final | Perda fantasma de -65.9% | D-055 |

### 5.2c Erros técnicos (Phase 7)

| # | Erro | Causa raiz | Impacto | Ref |
| --- | --- | --- | --- | --- |
| E-US-17 | SSOT financeiro baseado em boletim/estado | Modelo de dados orientado a snapshot | Pendência fantasma e risco de caixa negativo impossível | D-045 |
| E-US-18 | Aritmética de calendário civil espalhada | Ausência de infraestrutura de pregão | Bugs recorrentes em feriados/fins de semana | D-054 |
| E-US-19 | Rebalance por `day_idx % cadence` em produção | Índice dependente do tamanho histórico | Rebalance perdido sem intervenção manual | D-043 |
| E-US-20 | Mudança de path sem atualizar leitores | Escopo sem mapeamento downstream | `/painel` 404 após task tecnicamente “verde” | D-042 / R-023 |
| E-US-21 | Base 1 com base fixa no primeiro ponto | Formulação matemática inadequada para fluxo de caixa | Distorção do gráfico em eventos de aporte/retirada | D-057/D-058 |

### 5.3 Padrões de falha recorrentes

| Padrão | Descrição | Ocorrências | Mitigação |
| --- | --- | --- | --- |
| Paridade de código != paridade de semântica | Portar lógica sem validar significado dos dados de entrada | E-US-06, E-US-07, E-US-16 | D-012 + R-026 |
| Threshold inventado sem baseline BR | Critério novo sem gate de paridade | E-US-02 | D-012 (duplo bloqueio) |
| API/Calendário como ponto único de falha | Pipeline depende de data civil e API sem fallback correto | E-US-18 | D-041, D-054, R-021 |
| Estado computado como SSOT | Snapshot substituindo ledger de eventos | E-US-17 | D-045 |
| Mudança de escrita sem mapear leitores | Gerador atualizado, consumidor desatualizado | E-US-20 | R-023 |
| Governança contornada por automação | Override sem autorização explícita do Owner | E-US-22 | R-024, R-025 |

______________________________________________________________________

## 6. Divergências justificadas do RENDA_OPS

| Divergência | Justificativa | Decisão |
| --- | --- | --- |
| Variante C4 em vez de C2 | Controle de concentração em universo amplo | D-021 |
| TopN=20 em vez de 10 | Diversificação e redução de rotação | D-018 |
| Cadência=10 em vez de 1 | Custos e estabilidade operacional | T-017 |
| Filtro market_cap >= 300M | Exclusão de nano-caps impraticáveis | D-016 |
| Split ratio por preço raw no backtest | Robustez diante de reset de split cumulativo | D-019 |
| `stale_tickers` rolling por dia | Elimina lookahead | D-013 |
| Settlement T+1 | Regulação local | D-002 |
| Caixa remunerada por Fed Funds | Proxy monetária local | D-002 |
| Motor C4 puro sem ML trigger | Trigger não agrega | D-023 |
| Painel com Drawdown% e Base 1 sem CDI | BR benchmark não é transportável 1:1 | D-027 |
| Janela operacional separada do canônico | Eficiência diária com rastreabilidade mantida | D-026 |

______________________________________________________________________

## 7. Débitos Técnicos

| Débito | Descrição | Origem | Prioridade | Status |
| --- | --- | --- | --- | --- |
| DT-001 | Converter `split_factor` para event-based no pipeline | D-015 | Baixa | PENDENTE |
| DT-002 | Substituir proxy DJIA por índice mais aderente quando disponível | T-022 | Baixa | PENDENTE |
| DT-003 | Reforçar validação de entrada manual no fluxo operacional | L-BR-E-04 | Média | PENDENTE |
| DT-004 | Tornar isolamento de escrita da decisão robusto em teste/auditoria (`dry_run`) | E-US-13 | Alta | RESOLVIDO (D-044) |
| DT-005 | Bootstrap seguro de rebalance sem depender de índice absoluto | E-US-14 | Alta | RESOLVIDO (D-043) |
| DT-006 | Reconciliação semanal canonical <-> operational_window com evidência formal | D-026 | Média | PENDENTE |
| DT-007 | Alinhar `GOVERNANCE.md` §6.6 à tag mais recente de blindagem (`v1.6.0-motor-us`) | D-055 | Média | RESOLVIDO (D-060) |

______________________________________________________________________

## 8. Catálogo de Componentes

### 8.1 Compartilhados com BR (portados)

| Componente | Artefato US | Origem BR |
| --- | --- | --- |
| Motor M3 + histerese | `lib/engine.py` | `lib/engine.py` |
| Métricas | `lib/metrics.py` | `lib/metrics.py` |
| I/O | `lib/io.py` | `lib/io.py` |
| FredAdapter | `lib/adapters.py` | `lib/adapters.py` |
| Governança (trinca) | GOVERNANCE/DECISION_LOG/CHANGELOG | Template comum |

### 8.2 Específicos da Fábrica US

| Componente | Artefato | Função |
| --- | --- | --- |
| PolygonAdapter | `lib/adapters.py` | OHLCV/dividends/splits/reference |
| Universo histórico | `scripts/t006_build_index_compositions.py` | Composição histórica por data |
| SPC Shewhart v2 | `scripts/t008_quality_spc_and_blacklist_v2.py` | I-MR + Xbar-R |
| Canonical US | `scripts/t010_build_canonical_us_v2.py` | SSOT canônico |
| Backtest US | `backtest/run_backtest_variants_us.py` | C1/C2/C3/C4 com T+1 |
| Winner US | `config/winner_us.json` | Declaração canônica C4 |

### 8.3 Componentes Operacionais (Phase 5-7)

| Componente | Artefato | Função |
| --- | --- | --- |
| Orquestrador diário | `pipeline/run_daily.py` | `--full`, `--ingest-only`, `--decision-only` |
| Ingestão incremental | `pipeline/00_incremental_ingest.py` | Dias faltantes + rebuild janela |
| Rebuild janela | `pipeline/rebuild_operational_window.py` | Janela de pregões operacionais |
| Calendário de pregões | `lib/trading_calendar.py` | B3/NYSE com exchange_calendars |
| SSOT financeiro | `pipeline/ledger.py` + `data/ssot/ledger.jsonl` | Event sourcing imutável |
| Decisão C4 | `pipeline/09_decide.py` | C4 + `dry_run` + `last_rebalance` |
| Painel diário | `pipeline/painel_diario.py` | BR-format adaptado + split automático |
| Servidor HTTP | `pipeline/servidor.py` | Porta 8788 + `/salvar` |
| Blindagem | `tools/pre_commit_motor_guard.sh` | Proteção de arquivos do motor |
| Snapshot de blindagem | tag `v1.6.0-motor-us` | Estado auditado mais recente |

______________________________________________________________________

## 9. Lições Cruzadas BR ↔ US

### 9.1 Bugs do BR identificados/confirmados via experiência US

| Bug | Evidência no US | Ação |
| --- | --- | --- |
| `stale_tickers` com lookahead | Detectado na fase de auditoria US | Corrigido em ambas as fábricas (D-013) |
| Risco de baseline visual teórica | Discussão de Base 1 e patrimônio real no US | Reforço de alinhamento contábil cross-factory |

### 9.2 Lições do BR que evitaram erros no US

| Lição BR | Aplicação no US |
| --- | --- |
| Custos reais mudam conclusão | Backtest US já nasceu com custo e settlement reais |
| Split adjustment obrigatório | Camada dedicada desde o início |
| CEP/SPC como camada defensiva | Manteve venda defensiva permanente no motor |
| Auditoria forense adversarial | Estruturou validações da Phase 1 em diante |

### 9.3 Lições do US que retroagem ao BR

| Lição US | Aplicável ao BR? | Status |
| --- | --- | --- |
| Calendário real como infraestrutura (`trading_calendar`) | Sim | Em convergência |
| Semântica temporal explícita (`exec_day`/`market_day`/`trade_day`) | Sim | Incorporada em regras R-022 |
| Regras anti-role-bleed para auditor e override | Sim | Formalizadas em R-024 e R-025 |
| Validação semântica de dados ao portar lógica entre fábricas | Sim | Formalizada em R-026 |
| Monitoramento dinâmico de concentração pós-ignição | Sim | Formalizada em R-027 |

______________________________________________________________________

## 10. Checklist de Phases

### Phase 4 (ML Trigger) — CONCLUÍDA

- [x] Trigger testado com rigor.
- [x] Evidência de não agregação documentada.
- [x] Motor puro C4 formalizado.

### Phase 5 (Motor Operacional) — CONCLUÍDA

- [x] Pipeline diário funcional.
- [x] Painel e servidor em produção.
- [x] Duplo-caixa T+1 implementado.

### Phase 6 (Blindagem e Operação) — CONCLUÍDA

- [x] Auditorias finais convergentes.
- [x] Blindagem operacional consolidada.
- [x] Snapshot auditado atualizado para `v1.6.0-motor-us`.

### Phase 7 (Operação Contínua e Infraestrutura) — EM ANDAMENTO

- [x] Ledger financeiro imutável em produção (`data/ssot/ledger.jsonl`).
- [x] Semântica temporal explícita em boletim e painel.
- [x] Pipeline bifásico (`--ingest-only` / `--decision-only`).
- [x] `09_decide.py` com `dry_run` e `last_rebalance.json`.
- [x] `lib/trading_calendar.py` com calendário real de pregões.
- [x] Detecção de split automática com `corporate_actions`.
- [x] Guarda de frescura em ingestão diária.
- [x] Errata Base 1 com denominador cumulativo por ponto.

### Operação contínua — Checklist diário

- [ ] Rodar `pipeline/run_daily.py --ingest-only` e validar status (`ingested` ou `skipped_fresh`).
- [ ] Rodar `pipeline/run_daily.py --decision-only` e confirmar geração de `decision_*.json` e `painel_*.html`.
- [ ] Validar `market_day` no painel e coerência de `exec_day`/`trade_day`.
- [ ] Conferir `top20_by_score` sem preços zerados.
- [ ] Salvar boletim e confirmar escrita em `data/cycles/` e `data/real/`.
- [ ] Revisar balanço, DFC e alertas de `corporate_actions`.

______________________________________________________________________

## 11. Referências

| Documento | Path | Conteúdo |
| --- | --- | --- |
| Corpus US | `docs/CORPUS_FABRICA_US.md` | Documento consolidado (este arquivo) |
| Corpus BR | `/home/wilson/RENDA_OPS/docs/CORPUS_FABRICA_BR.md` | Referência metodológica e de formato |
| DECISION_LOG | `DECISION_LOG.md` | 58 decisões (D-001..D-058) + atualização D-059 |
| GOVERNANCE | `GOVERNANCE.md` | Regras vigentes e cadeia de comando |
| CHANGELOG | `CHANGELOG.md` | Histórico técnico cronológico |
| Plano US | `docs/PLANO_USA_OPS.md` | Plano de execução |
| SPEC pipeline | `docs/SPEC_PIPELINE_US.md` | Schemas, riscos e fontes |
| Operação de dados | `docs/OPERACAO_DADOS.md` | Guia operacional diário |
| Winner US | `config/winner_us.json` | Declaração canônica C4 |
| Regras Interfábricas | `/home/wilson/SALA_DE_CONTROLE/REGRAS_OPERACIONAIS.md` | Regras R-001 a R-029 |
