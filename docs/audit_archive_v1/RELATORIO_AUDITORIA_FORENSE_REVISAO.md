# RELATÓRIO DE AUDITORIA FORENSE DE PROFUNDIDADE — GEMINI 3.1 PRO (REVISÃO)

## Resumo Executivo
Fui convocado pelo Owner para uma re-auditoria focada na **comparação estrutural entre a Fábrica BR (RENDA_OPS) e a Fábrica US (USA_OPS)**, com escrutínio especial aos critérios de **Venda Defensiva**. 

Reconheço a falha na varredura anterior: ao focar puramente em *data leakage* e *survivorship bias* da Phase 1, omiti a validação do contrato de dados entre a Phase 1 (SSOT) e a Phase 3 (Motor de Vendas). 

O resultado desta revisão profunda é categórico: **A Phase 1 do USA_OPS, como está desenhada e executada, inviabiliza matematicamente a implementação da Venda Defensiva Consolidada da Fábrica BR.** As fundações de dados requeridas pelo mecanismo de severidade (Severity Score) não existem no pipeline atual dos EUA.

---

## Escrutínio Minucioso: Fábrica BR vs Fábrica US

A mecânica de Venda Defensiva desenvolvida no RENDA_OPS (arquivo `backtest/run_backtest_variants.py`) opera sobre uma "Camada 1" que requer inputs altamente específicos. Abaixo demonstro onde o USA_OPS falhou em prover esses insumos.

### 🔴 FINDING CRÍTICO 1: Ausência de Dados para o "Severity Score"
**Local:** Comparação entre `RENDA_OPS/pipeline/04_build_canonical.py` e `USA_OPS/scripts/t010_build_canonical_us.py`

**Mecanismo da Fábrica BR:**
O coração da venda defensiva no Brasil é o **Severity Score (0 a 6)**. Ele soma três componentes:
1. `band`: baseada no `z_prev` (Z-score contínuo de 60 dias do `i_value`).
2. `persist`: baseada no histórico de Z-scores (`z_prev2`, `z_prev3`).
3. `evidence`: baseada no rompimento de limites das cartas de controle de Shewhart (`any_rule` e `strong_rule`).
Para calcular `any_rule` e `strong_rule`, o RENDA_OPS calcula rigorosamente: `i_value`, `i_ucl`, `i_lcl`, `mr_value`, `mr_ucl`, `xbar_value`, `xbar_ucl`, `xbar_lcl`, `r_value`, `r_ucl` (Cartas I-MR e Xbar-R).

**O problema no USA_OPS:**
A T-008 calculou apenas 3 colunas esparsas: `spc_xbar`, `spc_ucl`, `spc_lcl`. O `canonical_us.parquet` resultante **NÃO CONTÉM** as features do Shewhart (`mr_value`, `r_value`, etc.) e não calcula o `X_real` descontando a risk-free rate. 
**Impacto:** Se avançássemos para a Phase 3, o código do backtest e do `painel_diario.py` iria colapsar (KeyError) ao tentar calcular o severity score. A "Venda Defensiva", na prática, não existiria.

### 🔴 FINDING CRÍTICO 2: SPC Estático vs SPC Dinâmico (Anti-Lookahead)
**Local:** `USA_OPS/scripts/t008_quality_spc_and_blacklist.py` L139-L151

**Mecanismo da Fábrica BR:**
Os limites de controle no Brasil (ucl, lcl) são calculados usando uma janela de referência rolante (`REF_WINDOW_K = 60`) com **`.shift(1)`**, garantindo que as regras de controle usadas no dia D só conheçam dados do dia D-1. O retorno utilizado é logarítmico (`np.log`).

**O problema no USA_OPS:**
O pipeline US calculou um `xbar` e um `std` estáticos sobre *toda a série temporal* do ticker (2018-2026), sem rolling window e sem shift, utilizando retornos aritméticos simples (`pct_change()`).
**Impacto:** Viés de lookahead severo na avaliação de qualidade estatística. O SPC gerado na T-008 é inutilizável para Venda Defensiva, servindo (e de forma enviesada) apenas para flag de outlier em blacklist.

### 🟡 FINDING ALTO 3: Mecanismo de Quarentena Quebrado
**Mecanismo da Fábrica BR:**
Quando uma ação sofre venda defensiva, ela entra em `quarantine`. Ela só sai da quarentena quando volta a estar "in control" — ou seja, quando `any_rule` e `strong_rule` voltam a ser falsas por conta do arrefecimento da volatilidade (avaliado pelas cartas I-MR e Xbar-R).

**O problema no USA_OPS:**
Sem as cartas de controle (`i_value`, `mr_value`, etc.) implementadas na Phase 1, a função de liberação da quarentena é cega. Um ticker vendido defensivamente nos EUA ficaria preso na quarentena para sempre ou seria solto de forma arbitrária.

### 🟡 FINDING MÉDIO 4: Regime de "Market-Slope" e Vendas Parciais
**Mecanismo da Fábrica BR:**
A Fábrica BR calcula a tendência de curto prazo da carteira atual (Regime Defensivo) tirando a média cross-sectional do `i_value` dos ativos possuídos e extraindo a regressão linear (`np.polyfit`) dos últimos 4 dias. Se `slope < 0` por 2 dias, o regime ativa e dispara as vendas parciais por FIFO (25%, 50%, 100%).

**O status no USA_OPS:**
Esse mecanismo é totalmente portável para o US e está previsto na T-016 (PLANO_USA_OPS.md). Contudo, assim como os itens anteriores, ele exige o `i_value` populado diariamente para todos os tickers do universo na Phase 1. A ausência atual bloquearia sua execução.

---

## Síntese de Ação Recomendada

A ordem do Owner para manter o pipeline "Idêntico ao RENDA_OPS" não foi cumprida no nível do processamento de dados (SSOT). O Architect simplificou os componentes estatísticos complexos sem perceber que eles eram os pilares essenciais do motor operacional das Phases 2, 3 e 5.

**Para que o desenvolvimento possa prosseguir com integridade total, a Phase 1 deve ser refeita incorporando:**
1. **O Algoritmo Completo de Shewhart:** Portar integralmente a lógica de cálculo (da linha 182 até 211 do arquivo `RENDA_OPS/pipeline/04_build_canonical.py`) para o pipeline do USA_OPS (T-008 e T-010).
2. **Uso de Taxa Livre de Risco (Risk-free):** A Fábrica BR usa o `cdi_log_daily`. A USA_OPS deve ingerir e usar a `fed_funds_rate` logarítmica para calcular o `X_real`.
3. **Log-Retornos e Rolling Shift:** Substituir retornos aritméticos e garantir o `.shift(1)` obrigatório nos limites de controle.

Apoio integralmente a decisão atual do CTO (D-007) de retornar à Phase 1 para efetivar essas correções, pavimentando uma via segura e compatível com a estrutura vitoriosa desenvolvida na Fábrica BR.