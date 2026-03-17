# RELATÓRIO DE AUDITORIA FORENSE — KIMI K2.5 (REVISÃO)

## Resumo Executivo

**Status:** COMPROMETIDO — Concordância total com Auditor Gemini na revisão.

Esta é uma re-auditoria focada na **comparação estrutural entre Fábrica BR (RENDA_OPS) e Fábrica US (USA_OPS)**, especificamente nos critérios de **Venda Defensiva** que serão necessários nas Phases 2, 3 e 5.

Minha auditoria anterior focou em consistência numérica entre reports e anti-lookahead, mas **omiti a validação do contrato de dados entre Phase 1 (SSOT) e as fases operacionais futuras**. Essa omissão foi crítica.

**Veredito:** A Phase 1 do USA_OPS **NÃO PRODUZ** os dados necessários para executar a Venda Defensiva da Fábrica BR. O pipeline US está matematicamente incompleto para as fases subsequentes.

---

## Findings por Frente (6 Frentes + Análise Comparativa)

### Frente A — Consistência SPC BR vs US: METODOLOGIA DIVERGENTE 🔴 CRÍTICO

| Aspecto | RENDA_OPS (Correto) | USA_OPS (Implementado) | Impacto |
|---------|---------------------|------------------------|---------|
| **Tipo de retorno** | Log-retorno: `np.log(close/close.shift(1))` | Aritmético: `pct_change()` | Incompatível com cálculo de volatilidade e métricas de risco |
| **Risk-free adjustment** | Sim: `X_real = log_ret - cdi_log_daily` | Não: usa retorno bruto | Não calcula retorno real excesso vs caixa |
| **Janela de cálculo** | Rolling: `REF_WINDOW_K=60` com `shift(1)` | Estático: série inteira 2018-2026 | **Lookahead implícito** nos limites de controle |
| **Constantes SPC** | Shewhart tabeladas: A2=0.729, D4=2.282, E2=2.66 | Sigma fixo: 6.0 arbitrário | Metodologia estatística não padronizada |
| **Cartas de controle** | Completo: I-MR + Xbar-R | Simplificado: apenas X-bar estático | Impossibilita cálculo de severity score |

**Evidência Numérica:**
```
Ticker AA no US canonical:
  spc_xbar: 0.000748 (CONSTANTE para todas as 2.061 datas)
  spc_ucl:  0.218054 (CONSTANTE)
  spc_lcl: -0.216559 (CONSTANTE)
  
→ Valores únicos por ticker: 1 (confirma: estático, não rolling)
```

**Conclusão Frente A:** A metodologia SPC do US é estatisticamente incorreta e incompatível com o motor de venda defensiva.

---

### Frente B — Variáveis para Venda Defensiva: AUSÊNCIA TOTAL 🔴 CRÍTICO

**Variáveis necessárias (run_backtest_variants.py BR):**
- `i_value` — Valor individual para I-chart
- `i_ucl`, `i_lcl` — Limites I-chart
- `mr_value` — Moving range
- `mr_ucl` — MR upper limit  
- `xbar_value` — X-bar média (rolling)
- `xbar_ucl`, `xbar_lcl` — Limites X-bar (rolling)
- `r_value` — Range value
- `r_ucl` — R-chart upper limit

**Variáveis disponíveis no US:**
- `spc_xbar` — Apenas X-bar estático (não rolling)
- `spc_ucl`, `spc_lcl` — Limites fixos

**Impacto na Venda Defensiva:**

O severity score (0-6) no BR é calculado como:
```python
score = band(z_prev) + persist(z_prev2, z_prev3) + evidence(any_rule, strong_rule)
```

Onde:
- `any_rule = (i_value > i_ucl) | (i_value < i_lcl) | (mr_value > mr_ucl) | (r_value > r_ucl) | ...`
- `strong_rule = (i_value > i_ucl) | (i_value < i_lcl) | (mr_value > mr_ucl)`

**No US:** Como não existem `i_value`, `mr_value`, `r_value`, não é possível calcular `any_rule` nem `strong_rule`. O severity score **NÃO PODE SER COMPUTADO**.

**Conclusão Frente B:** A venda defensiva (camada 1 do motor) é matematicamente impossível com os dados atuais.

---

### Frente C — Recálculo Metodológico: DIVERGÊNCIA CONFIRMADA 🔴 CRÍTICO

**Verificação numérica direta:**

| Check | BR | US | Status |
|-------|-----|-----|--------|
| Rolling SPC | Sim, 60 dias com shift(1) | Não, estático 2018-2026 | ✗ Divergência |
| Log-retornos | Sim | Não (usa pct_change) | ✗ Divergência |
| Risk-free adjustment | Sim (CDI) | Não | ✗ Divergência |
| Shewhart constants | Sim (A2, D4, E2) | Não (sigma=6.0) | ✗ Divergência |
| I-MR charts | Sim | Não | ✗ Divergência |
| Xbar-R charts | Sim | Parcial (só X-bar estático) | ✗ Divergência |

**Exemplo prático:**
- No BR, `center_line` para o dia 2020-03-15 é calculado como média dos 60 dias anteriores (2019-12-16 a 2020-03-13), com `shift(1)`.
- No US, `spc_xbar` para o dia 2020-03-15 é calculado como média de **toda a série** 2018-2026, olhando dados de 2025 para definir limites em 2020.

**Conclusão Frente C:** Olha para o futuro implícito nos cálculos estáticos.

---

### Frente D — Integridade Canônica: DADOS INCOMPLETOS 🔴 CRÍTICO

**Colunas ausentes no canonical_us.parquet vs BR:**
```
✗ close_raw              # Preço bruto não ajustado
✗ log_ret_nominal        # Log-retorno bruto
✗ X_real                 # Retorno real descontado risk-free
✗ i_value                # Valor I-chart
✗ i_ucl, i_lcl           # Limites I-chart
✗ mr_value, mr_ucl       # Moving range
✗ xbar_value             # X-bar (rolling)
✗ xbar_ucl, xbar_lcl     # Limites X-bar (rolling)
✗ r_value, r_ucl         # Range chart
✗ center_line            # Linha central rolling
✗ mr_bar, r_bar          # Médias de referência
✗ feature_timestamp_cutoff # Evidência anti-lookahead
```

**Problemas adicionais identificados:**
1. **Preços:** O US não preserva `close_raw` (preço bruto). A Polygon entrega `adjusted=True` e o pipeline não guarda o preço original.
2. **Timestamp:** Não há coluna `feature_timestamp_cutoff` no canonical, impossibilitando auditoria temporal direta.
3. **SPC Estático:** Confirmado em 1032 tickers — todos têm valores SPC constantes para todas as datas.

**Conclusão Frente D:** O SSOT canônico está incompleto para uso operacional.

---

### Frente E — Análise Comparativa Estrutural: VIÉS DE SELEÇÃO NA ARQUITETURA 🔴 CRÍTICO

**O que foi portado do BR:**
- `lib/engine.py` (compute_m3_scores, apply_hysteresis, select_top_n) — ✓
- `lib/metrics.py` — ✓
- `lib/io.py` — ✓
- Conceito de venda defensiva — Documentado em T-016

**O que foi SIMPLIFICADO/DESCARTADO na T-008/T-010:**
- Cálculo completo de Shewhart (I-MR + Xbar-R) — ✗ Simplificado para X-bar estático
- Log-retornos — ✗ Substituído por pct_change()
- Rolling window com shift(1) — ✗ Substituído por cálculo estático
- Risk-free adjustment — ✗ Omitido
- `close_raw` preservado — ✗ Omitido (Polygon adjusted=True)

**Análise do erro arquitetural:**
O Architect na T-008 leu a SPEC que dizia "SPC por ticker (xbar/ucl/lcl)" e implementou literalmente **apenas** essas 3 colunas, ignorando que na Fábrica BR o SPC é um sistema completo de controle estatístico de processo com 13+ variáveis interligadas.

A SPEC (docs/SPEC_PIPELINE_US.md) na seção T-008 descreve:
> "SPC (padrao): `ret = close.pct_change()`, excluir dias com split, `xbar = mean(ret)`, `ucl = xbar + sigma*std`"

Isso é uma descrição de **sigma arbitrário**, não de **Shewhart SPC** como usado no BR.

---

### Frente F — Checklist de Venda Defensiva: IMPLEMENTAÇÃO INVIÁVEL 🔴 CRÍTICO

| Componente Venda Defensiva | Requer | Disponível no US | Status |
|---------------------------|--------|------------------|--------|
| Severity Score (0-6) | `i_value`, `mr_value`, `any_rule`, `strong_rule` | Não | ✗ Impossível |
| Bandas de Z-score | `z_prev` calculado de `i_value` | Não | ✗ Impossível |
| Persistência | `z_prev2`, `z_prev3` | Não | ✗ Impossível |
| Vendas parciais (25/50/100%) | Severity score + threshold | Não | ✗ Impossível |
| Quarentena | `in_control` status (sai de `any_rule`/`strong_rule`) | Não | ✗ Impossível |
| Regime defensivo | Market-slope de `i_value` médio | Não | ✗ Impossível |
| Ajuste de splits | `split_factor` aplicado em lotes | Sim (disponível) | ✓ Possível |

**Conclusão Frente F:** 6 de 7 componentes da venda defensiva são impossíveis com os dados atuais.

---

## Tabela de Findings Consolidada

| Frente | Severidade | Descrição | Evidência |
|--------|-----------|-----------|-----------|
| A | CRÍTICO | Metodologia SPC divergente (log vs aritmético, rolling vs estático) | `pct_change()` vs `np.log()`, `shift(1)` ausente |
| B | CRÍTICO | Variáveis Shewhart ausentes para severity score | `i_value`, `mr_value`, `r_value` inexistentes |
| C | CRÍTICO | Lookahead implícito em SPC estático | Valores SPC constantes por ticker (1 único valor) |
| D | CRÍTICO | SSOT incompleto — 15 colunas ausentes vs BR | `close_raw`, `X_real`, todas as cartas de controle |
| E | CRÍTICO | Arquitetura simplificada quebra contrato com motor | SPEC descreve sigma arbitrário, não Shewhart |
| F | CRÍTICO | Venda defensiva impossível (6/7 componentes) | Backtest BR requer dados inexistentes no US |

---

## Métricas de Impacto

| Métrica | Valor | Significado |
|---------|-------|-------------|
| Colunas SPC no BR | 14 | I-MR + Xbar-R completo |
| Colunas SPC no US | 3 | Apenas X-bar estático simplificado |
| Componentes venda defensiva bloqueados | 6/7 (86%) | Impossibilidade operacional |
| Tickers com SPC estático | 1032/1032 (100%) | Viés de lookahead confirmado |
| Tasks da Phase 1 a refazer | 6/7 (T-006 a T-011) | Escopo de correção |

---

## Veredito Final: [[ REPROVADO ]]

A Fábrica US, na forma como a Phase 1 foi executada, **não pode prosseguir** para as Phases 2, 3, 4, 5 ou 6 sem correção estrutural completa.

**Fundamentos quebrados:**
1. Dados canônicos não suportam venda defensiva (core do motor)
2. SPC calculado com lookahead (inválido para backtest)
3. Metodologia estatística simplificada (não Shewhart)
4. Ausência de preços brutos (impossibilita auditoria de ajustes)

**Concordância com Auditor Gemini:**
- Gemini identificou: Ausência de dados Shewhart, SPC estático, metodologia incorreta
- Kimi confirma: Todos os findings numéricos, 1032 tickers com SPC estático, 6/7 componentes de venda defensiva bloqueados

**Recomendação:**
Implementar integralmente as correções descritas no **D-007** (refazer Phase 1 com metodologia idêntica ao RENDA_OPS), portando obrigatoriamente:
1. Cálculo completo de Shewhart (linhas 182-211 de `04_build_canonical.py`)
2. Log-retornos com desconto de risk-free (Fed Funds)
3. Rolling window 60 dias com `shift(1)` obrigatório
4. Preservação de `close_raw` e `close_operational` separados

---

*Relatório gerado por Auditor Forense Kimi K2.5 (Revisão)*
*Data: 2026-03-17*
*Workspace: /home/wilson/USA_OPS*
*Referência: Comparação direta com RENDA_OPS/pipeline/04_build_canonical.py e backtest/run_backtest_variants.py*
