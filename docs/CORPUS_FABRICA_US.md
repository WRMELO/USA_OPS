# CORPUS DE CONHECIMENTO — Fábrica US (USA_OPS)

> Ref: D-022 | Data: 2026-03-18
> Consolidação de toda a experiência acumulada no desenvolvimento da Fábrica US (winner C4), Phases 0-3.
> Complementar ao `docs/CORPUS_FABRICA_BR.md` (Fábrica BR, RENDA_OPS).

---

## 1. Visão Geral do Projeto

**Repositório**: USA_OPS
**Ancestral**: RENDA_OPS (Fábrica BR) + AGNO_WORKSPACE (R&D)
**Winner**: C4 — Score-weighted com cap de concentração (TopN=20, Cad=10, K=10, cap=6%, k_damp=0.0)
**Mercado**: US equities (Russell 1000 + S&P SmallCap 600, excluindo tickers com BDR na B3)
**Moeda**: USD
**Período de desenvolvimento**: 2026-03-07 a 2026-03-18
**Status**: Winner declarado (Phase 3 concluída), aguardando Phase 4 (ML Trigger)

### Métricas do winner (C4, HOLDOUT 2023-01-02 a 2026-03-16)

| Métrica | Valor |
|---------|-------|
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
|---------|-------------|----------------|------|
| CAGR HOLDOUT | 19.2% | 42.14% | US ~2x; universo mais amplo e volátil |
| MDD HOLDOUT | -23.2% | -40.12% | US mais agressivo; universo tem small-caps |
| Concentração máxima | 18.1% | 43.47% (pontual) | Cap de 6% controla mediana, mas picos persistem |
| Regime defensivo | 59.1% | 67.6% | US mais volátil → SPC dispara mais |
| TopN | 10 | 20 | US precisa de mais diversificação |
| Variante | C2 | C4 | US precisou de cap de concentração |
| ML Trigger | Sim (XGBoost) | Pendente (Phase 4) | BR usa thr=0.22, h_in=3, h_out=2 |

---

## 2. Cronologia de Fases

| Fase | Escopo | Tasks | Decisões-chave | Resultado |
|------|--------|-------|-----------------|-----------|
| Phase 0 — Fundação | Setup repo, governança, portar libs | T-001 a T-005 | D-001, D-003 | Skeleton funcional |
| Phase 1 v1 — Dados (REPROVADA) | Ingestão via iShares snapshot + SPC incorreto | T-006 a T-011 | D-004, D-005, D-006 | **Reprovada por auditoria forense** |
| Phase 1 v2 — Dados (APROVADA) | Universo histórico Polygon + SPC Shewhart real | T-006v2 a T-011v2 | D-007, D-008, D-009 | Pipeline de dados auditado e aprovado |
| Phase 2 — Motor M3 + Features | Scoring M3, features, labels | T-012 a T-014 | D-010, D-011, D-012, D-013 | Dataset rotulado com anti-lookahead |
| Phase 3 — Backtest | Framework, ablação, concentração, acid window | T-015 a T-024 | D-014 a D-021 | **Winner C4 declarado** |

**Total**: 21 decisões, 30+ tasks (incluindo FIXes), 6 auditorias forenses (Gemini+Kimi em Phase 1, Phase 2 e Phase 3)

---

## 3. Arquitetura

### 3.1 Dados (Phase 1)

```
data/
├── ssot/
│   ├── canonical_us.parquet    → SSOT canônico: OHLCV + SPC (10 cols) + split_factor + market_cap
│   ├── macro_us.parquet        → FRED: VIX, USD Index, Treasuries (10Y/2Y), Fed Funds, HY/IG OAS
│   ├── ticker_reference_us.parquet  → Metadados: active/delisted, list_date, setor
│   └── us_market_data_raw.parquet   → OHLCV bruto (adjusted=False) + dividends + splits
├── features/
│   ├── scores_m3_us.parquet    → Scores M3 diários (z-score cross-section, janela 62, ddof=0)
│   ├── dataset_us.parquet      → Features consolidadas (macro + SPC + M3)
│   ├── labels_us.parquet       → Labels oracle (drawdown SP500, threshold TRAIN-only)
│   └── dataset_us_labeled.parquet → Dataset completo para ML
└── config/
    ├── blacklist_us.json       → HARD (delisted) + SOFT (history < 252 dias)
    ├── feature_guard_us.json   → Allowlist de features anti-snooping
    └── winner_us.json          → Declaração canônica do winner
```

### 3.2 SPC Shewhart no canonical_us

| Coluna | Descrição | Usado em |
|--------|-----------|----------|
| i_value | Individual chart value (log-retorno) | z-score → severity score |
| i_ucl, i_lcl | Upper/Lower control limits (I chart) | Regras Shewhart |
| mr_value, mr_ucl | Moving Range chart | Regras Shewhart |
| xbar_value, xbar_ucl, xbar_lcl | Xbar chart (subgrupos) | Regras Shewhart |
| r_value, r_ucl | Range chart | Regras Shewhart |

Constantes tabeladas (d2=1.128, D4=3.267) — idênticas ao RENDA_OPS.

### 3.3 Backtest (Phase 3)

```
backtest/
├── run_backtest_variants_us.py     → Motor principal (C1/C2/C3/C4)
├── run_t017_ablation_us.py         → Ablação TopN × Cadence × K
├── run_t018_ablation_us.py         → Ablação C4 (dampening × cap)
├── run_t021_concentration_analysis.py → Diagnóstico concentração + MDD
├── run_t022_dual_acid_window_us.py → Stress test dual window
├── plot_t015_plotly.py             → Plots equity comparison
├── plot_t018_plotly.py             → Plots C2 vs C4
└── results/                        → CSVs, JSONs, HTMLs gerados
```

### 3.4 Motor de venda (idêntico ao BR em conceito, adaptado em parâmetros)

1. **Camada 0**: Ajuste de splits — ratio derivado de `close_raw` (preserva valor econômico)
2. **Camada 1**: Venda defensiva permanente (SPC Shewhart)
   - Regime defensivo via market-slope (slope i_value dos holdings, janela 4 dias)
   - Severity Score composto (0-6): z-band + persistência + evidência de regras violadas
   - Vendas graduais: 25% (score 4), 50% (score 5), 100% (score 6)
   - Quarentena pós-venda (ticker bloqueado até voltar "in control")
3. **Camada 2**: Rebalanceamento C4 (histerese + score-weighted + cap de concentração)
   - Cadência de 10 dias
   - Histerese buffer K=10 (manter ticker se rank <= K)
   - Pesos proporcionais ao score M3 dampened (k_damp=0 no winner)
   - Cap de concentração 6% com trims FIFO
4. **Camada 2.5**: Trims de concentração (somente C4, antes das compras)

### 3.5 Diferenças estruturais BR vs US

| Aspecto | BR (RENDA_OPS) | US (USA_OPS) | Justificativa |
|---------|---------------|-------------|---------------|
| Liquidação | D+2 ações, D+1 BDR | T+1 | Regulação SEC |
| Caixa (tank) | CDI (Selic/252) | Fed Funds Rate (DFF/252) | Banco central local |
| Universo | ~906 tickers (fixo) | ~4.489 → ~2.090 após filtro market_cap | Mercado US muito mais amplo |
| Filtro market_cap | Não necessário (mercado filtra naturalmente) | >= $300M dinâmico por dia (D-016) | Nano-caps US são impraticáveis |
| TopN | 10 | 20 | Mais diversificação necessária |
| Variante | C2 (histerese pura) | C4 (histerese + cap concentração) | Universo amplo gera concentração excessiva sem cap |
| Split factor | Event-based (NaN exceto no dia do split) | Cumulativo no pipeline, event-based derivado no backtest | Polygon vs BRAPI semântica diferente |
| Dados OHLCV | BRAPI (adjusted implícito) | Polygon (adjusted=False) | Controle explícito |
| SPC | Idêntico | Idêntico | Paridade metodológica obrigatória (D-009) |
| Scoring M3 | Idêntico (log-retorno, janela 62, ddof=0) | Idêntico | Paridade metodológica |

---

## 4. Lessons Learned — O que funcionou

### 4.1 Processo

| # | Lição | Evidência |
|---|-------|-----------|
| L-US-01 | **Auditoria forense antes de avançar de fase** evita construir sobre base podre | Phase 1 v1 reprovada → Phase 1 v2 corrigiu 3 problemas fundamentais (D-007) |
| L-US-02 | **Purga física de artefatos obsoletos** elimina risco de contaminação | D-008: dados v1 deletados fisicamente, não apenas inativados |
| L-US-03 | **Execução task-a-task com ciclo completo** dá visibilidade e controle ao Owner | D-010: CTO despachando 3 tasks de uma vez foi não-conformidade |
| L-US-04 | **Bloqueio duplo (CTO checklist + Architect rejeição)** impede thresholds arbitrários | D-012: CTO violou paridade 2x; barreira estrutural resolveu |
| L-US-05 | **Owner como detector de anomalias visuais** é insubstituível | D-019: Owner viu o salto no gráfico que 3 agentes não viram |
| L-US-06 | **Convergência entre auditores independentes** dá confiança alta | D-020: Gemini e Kimi concordaram em todas as frentes |
| L-US-07 | **Corpus BR como referência obrigatória** acelera desenvolvimento e previne desvios | GOVERNANCE.md §8 e D-009 forçam consulta antes de cada fase |

### 4.2 Técnico

| # | Lição | Evidência |
|---|-------|-----------|
| L-US-08 | **Universo histórico real (não snapshot)** é obrigatório para anti-survivorship | D-007: snapshot iShares de 2026 excluía empresas mortas desde 2018 |
| L-US-09 | **adjusted=False** nos dados brutos evita corrupção em reprocessamento | D-007: adjusted=True do Polygon muda retroativamente |
| L-US-10 | **SPC Shewhart com constantes tabeladas** é o padrão; sigma fixo é errado | D-007: Phase 1 v1 usou sigma fixo em vez de d2=1.128 |
| L-US-11 | **Split ratio derivado de preço raw** é mais robusto que sf_D/sf_{D-1} | D-019: Polygon reseta split_factor cumulativo inconsistentemente; ratio = px_{D-1}/px_D preserva valor por construção |
| L-US-12 | **Filtro de market_cap dinâmico (D-1)** é operacional, não metodológico | D-016: 46% das seleções top-10 eram nano-caps < $300M |
| L-US-13 | **Cap de concentração per ticker** é necessário em universos amplos | T-018: sem cap, concentração > 50% em um ticker; com cap 6%, mediana controlada em 6.3% |
| L-US-14 | **Ablação ampla antes de declarar winner** previne overfitting a grade estreita | D-018: 80 combinações testadas (TopN × Cad × K × k_damp × cap) |
| L-US-15 | **Acid window com critério objetivo** (pior DD, min 126 dias) valida robustez | T-022: motor +36% vs benchmark +5% na janela de stress SP500 |
| L-US-16 | **stale_tickers com filtro rolling por dia** elimina lookahead no backtest | D-013: filtro global causava exclusão retroativa de 7 tickers; corrigido em ambas as fábricas |
| L-US-17 | **Rebalance cadence > 1** reduz custos e melhora estabilidade | T-017: cadência 10 dias é o sweet spot entre rotação e custos |

---

## 5. Lessons Learned — O que deu errado

### 5.1 Erros de processo

| # | Erro | Causa raiz | Impacto | Ref |
|---|------|-----------|---------|-----|
| E-US-01 | Phase 1 inteira reprovada e refeita do zero | Snapshot iShares (survivorship) + SPC incorreto + adjusted=True | 1 dia perdido, mas dados corretos depois | D-007 |
| E-US-02 | CTO inventou threshold sem correspondência no BR (2x) | Falta de checklist obrigatório na orientação CTO | Tasks falharam por gates arbitrários | D-009, D-011 |
| E-US-03 | CTO despachou 3 tasks simultâneas | Pressa; não respeitou regra "uma orientação por vez" | Confusão de fluxo, pergunta "junto ou separado?" | D-010 |
| E-US-04 | Auditor emitiu FAIL factualmente incorreto (T-015) | Leitura incompleta do código | Owner obrigado a intervir e reverter | D-014 |
| E-US-05 | CTO não alertou sobre rotação alta no universo US | Não antecipou que TopN=10 com 4.000 tickers causa subinvestimento | C1 colapsou de 100k para 494 | D-014 |

### 5.2 Erros técnicos

| # | Erro | Causa raiz | Impacto | Ref |
|---|------|-----------|---------|-----|
| E-US-06 | split_factor usado como event-based quando é cumulativo | Paridade de código (BR) sem paridade de semântica dos dados | Equity explodiu para 64 bilhões (15.607 overflow events) | D-015 |
| E-US-07 | sf_D/sf_{D-1} não preserva valor quando Polygon reseta sf | Polygon inconsistente no reset do split_factor cumulativo | CAGR inflado de 71.6% para ~36% real (27 split events divergentes) | D-019 |
| E-US-08 | 46% das seleções top-10 eram nano-caps impraticáveis | Universo US muito mais amplo que BR sem filtro de liquidez | Resultados de ablação não representavam operação real | D-016 |
| E-US-09 | stale_tickers com lookahead (filtro global em vez de rolling) | Herdado do RENDA_OPS (06_compute_scores.py, linha 54) | 7 tickers excluídos retroativamente; impacto 0.28% | D-013 |
| E-US-10 | gate outputs_written checado antes de todos os outputs serem escritos | Ordem de operações no script | Script falhava com gate FAIL apesar de sucesso lógico | T-021, T-016 |

### 5.3 Padrões de falha recorrentes

| Padrão | Descrição | Ocorrências | Mitigação implementada |
|--------|-----------|-------------|------------------------|
| **Paridade de código ≠ paridade de semântica** | Copiar código do BR sem verificar se os dados de entrada têm a mesma semântica | E-US-06, E-US-07 | D-012 (duplo bloqueio) + revisão de semântica no checklist |
| **CTO inventa thresholds** | CTO introduz gates/filtros sem correspondência no modelo BR | E-US-02 | D-012 (parity_cto_check obrigatório + rejeição pelo Architect) |
| **Snapshot como universo histórico** | Usar composição atual para simular o passado | E-US-01 | D-007 (universo histórico real via API datada) |
| **Gate order-of-operations** | Verificar existência de arquivo antes de escrevê-lo | E-US-10 | Padrão: escrever outputs → depois checar gate → depois salvar report |

---

## 6. Divergências justificadas do RENDA_OPS

Cada divergência abaixo foi explicitamente aprovada pelo Owner e registrada no DECISION_LOG.

| Divergência | Justificativa | Decisão |
|------------|---------------|---------|
| Variante C4 (score-weighted + cap) em vez de C2 | Universo US amplo gera concentração >50% sem cap; C4 controla via max_weight_cap | D-021 |
| TopN=20 em vez de 10 | Universo US ~5x maior; TopN=10 gera rotação excessiva e MDD catastrófico | D-018 |
| Cadence=10 em vez de 1 | Rotação diária no universo US de ~4.000 tickers destruía capital; cadência 10 é sweet spot | T-017 |
| min_market_cap >= $300M | Filtro operacional para excluir nano-caps impraticáveis (spreads, liquidez) | D-016 |
| Split ratio via preço raw (não sf_D/sf_{D-1}) | Polygon split_factor cumulativo tem resets inconsistentes; preço raw preserva valor por construção | D-019 |
| stale_tickers rolling por dia (não global) | Elimina lookahead no backtest; propagado de volta ao RENDA_OPS | D-013 |
| Proxy DJIA para acid test Russell 1000 | Séries FRED Russell 1000 indisponíveis (404); DJIA plausível como stress test | T-022 |
| Settlement T+1 (não D+2) | Regulação SEC para equities US | D-002 |
| Cash remuneration Fed Funds (não CDI) | Banco central relevante | D-002 |

---

## 7. Débitos Técnicos

| Débito | Descrição | Origem | Prioridade |
|--------|-----------|--------|------------|
| DT-001 | Converter split_factor para event-based no pipeline (T-008) | D-015 | Baixa (fix no backtest já resolve) |
| DT-002 | Proxy DJIA no acid test — substituir por Russell 1000 quando FRED disponibilizar | T-022 | Baixa (informativo) |
| DT-003 | Validação de entrada para dados manuais (operação futura) | L-BR-E-04 | Média (Phase 5) |

---

## 8. Catálogo de Componentes

### 8.1 Compartilhados com BR (portados)

| Componente | Artefato US | Origem BR |
|-----------|------------|-----------|
| Motor M3 + histerese | `lib/engine.py` | `lib/engine.py` (cópia) |
| Métricas | `lib/metrics.py` | `lib/metrics.py` (cópia) |
| I/O | `lib/io.py` | `lib/io.py` (cópia) |
| FredAdapter | `lib/adapters.py` | `lib/adapters.py` (parcial) |
| Governança (trinca) | GOVERNANCE/DECISION_LOG/CHANGELOG | Template idêntico |
| Skills de agentes | `.cursor/skills/` | Idênticos |

### 8.2 Específicos da Fábrica US

| Componente | Artefato | Função |
|-----------|---------|--------|
| PolygonAdapter | `lib/adapters.py` | OHLCV + dividends + splits + reference data |
| Universo histórico | `scripts/t006_build_index_compositions.py` | Polygon /v3/reference/tickers por ano |
| SPC Shewhart v2 | `scripts/t008_quality_spc_and_blacklist_v2.py` | I-MR + Xbar-R com constantes tabeladas |
| Canonical US | `scripts/t010_build_canonical_us_v2.py` | SSOT com close_raw + SPC + market_cap |
| Backtest US | `backtest/run_backtest_variants_us.py` | C1/C2/C3/C4 com T+1, Fed Funds, FIFO lots |
| Ablação | `backtest/run_t017_ablation_us.py`, `run_t018_ablation_us.py` | Grid search com market_cap filter |
| Concentração | `backtest/run_t021_concentration_analysis.py` | Diagnóstico temporal |
| Acid window | `backtest/run_t022_dual_acid_window_us.py` | Stress test dual benchmark |
| Winner US | `config/winner_us.json` | Declaração canônica C4 |

---

## 9. Lições Cruzadas BR ↔ US

### 9.1 Bugs que existiam no BR e foram descobertos no US

| Bug | Onde no BR | Descoberto por | Correção | Propagado? |
|-----|-----------|---------------|----------|------------|
| stale_tickers com lookahead | `06_compute_scores.py` L54 | Auditoria Gemini/Kimi Phase 2 US | Rolling por dia (D-013) | Sim (HF-STEP06-STALE-ROLLING no RENDA_OPS) |

### 9.2 Lições do BR que evitaram erros no US

| Lição BR | Como ajudou no US |
|----------|-------------------|
| L-14: Custos reais mudam conclusão | Backtest US modelou 2.5 bps + T+1 desde o dia 1 |
| L-15: Splits obrigatórios | Camada 0 de split adjustment presente em todas as variantes |
| E-03: CEP/SPC abandonado | SPC Shewhart integrado como camada defensiva permanente desde T-016 |
| E-06: Severity score simplificado | Score 0-6 completo implementado (band + persist + evidence) |
| D-027: FRED trava pipeline | FredAdapter com retry exponencial + max_retries=5 desde o dia 1 |

### 9.3 Lições do US que devem retroagir ao BR

| Lição US | Aplicável ao BR? | Status |
|----------|-----------------|--------|
| Split ratio via preço raw (D-019) | Não (BR usa event-based nativo) | N/A |
| Market cap filter dinâmico (D-016) | Possivelmente (filtro de liquidez) | Pendente avaliação |
| Cap de concentração per ticker (T-018) | Possivelmente (concentração BR <= 18%) | Pendente avaliação |
| Ablação ampla com grid expandido (D-018) | Sim (BR fez grid mais estreito) | Pendente avaliação |

---

## 10. Checklist para Phase 4+ (ML Trigger US)

Antes de iniciar a Phase 4, verificar:

- [x] Winner US declarado formalmente (`config/winner_us.json`)
- [x] Auditoria forense Phase 3 PASS (Gemini + Kimi)
- [x] Corpus US consolidado (este documento)
- [ ] Dataset rotulado disponível (`data/features/dataset_us_labeled.parquet`)
- [ ] Features anti-lookahead verificadas (shift(1) em todas)
- [ ] TRAIN/HOLDOUT split definido (2018-2022 / 2023-2026)
- [ ] Walk-forward estrito: treinar só no TRAIN, avaliar no HOLDOUT
- [ ] Comparar motor puro (C4 winner) vs motor + ML trigger
- [ ] Se ML não agregar: manter motor puro (lição LL-PH10-007 do BR)
- [ ] Auditoria forense Phase 4 antes de avançar para Phase 5

---

## 11. Referências

| Documento | Path | Conteúdo |
|-----------|------|----------|
| Corpus BR | `docs/CORPUS_FABRICA_BR.md` | Experiência completa da Fábrica BR |
| Plano US | `docs/PLANO_USA_OPS.md` | Plano de execução completo |
| SPEC Pipeline US | `docs/SPEC_PIPELINE_US.md` | Schemas, fontes, riscos |
| Winner US | `config/winner_us.json` | Declaração canônica C4 |
| Winner BR | `RENDA_OPS/config/winner.json` | Declaração canônica C060X |
| GOVERNANCE.md | `GOVERNANCE.md` | Regras vigentes |
| DECISION_LOG.md | `DECISION_LOG.md` | 21 decisões com contexto |
| CHANGELOG.md | `CHANGELOG.md` | Histórico técnico completo |
| ROADMAP.md | `ROADMAP.md` | Mapa de execução + backlog |
| MANIFESTO_ORIGEM.json | `MANIFESTO_ORIGEM.json` | Proveniência e SHA256 |
