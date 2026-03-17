# RELATÓRIO DE AUDITORIA FORENSE — USA_OPS

## Resumo Executivo

Atuando como Auditor Forense Adversarial (Gemini 3.1 Pro), conduzi uma varredura profunda no código-fonte e nas decisões arquiteturais do pipeline de dados (`T-006` a `T-011`).

**Veredito:** O pipeline contém **duas falhas críticas** que invalidam matematicamente qualquer backtest futuro e corrompem a base de dados ao longo do tempo. Antes de prosseguir para o desenvolvimento do modelo (Phase 2), é imperativo corrigir a corrupção de preços ajustados e repensar o impacto do survivorship bias, sob pena de gerar resultados ilusórios e prejuízo financeiro real.

---

## Findings por Severidade

### 🔴 CRÍTICO: Corrupção de Preços em Ingestão Incremental (Structural Break)
* **Local:** `scripts/t007_ingest_us_market_data_raw.py` (L205-L232) e `lib/adapters.py` (L101: `adjusted=True`).
* **Mecanismo da Falha:** O Polygon.io retorna dados retroativamente ajustados para splits/dividendos com base no momento do request (`adjusted=True`). No entanto, o `t007` anexa (append) dados novos ao parquet existente. 
* **Exploit Adversarial:** Se uma ação tem um split 2:1 hoje, o novo request incremental de hoje trará preços ajustados (ex: $50). O arquivo local antigo tem os preços de ontem não ajustados (ex: $100). Ao concatenar, criamos um gap irreal de -50% no dia da emenda. O cálculo de SPC na `T-008` (que exclui o dia do split) não protegerá contra isso, pois o salto de preço ocorrerá na **fronteira da ingestão incremental**, não no dia do evento corporativo. Isso fará com que tickers válidos sejam classificados como `SOFT/HARD blacklist` erroneamente ao longo do tempo devido a "outliers" fantasmas.
* **Solução:** Ingerir sempre `adjusted=False` (preços puros) e usar os dataframes de `splits` e `dividends` para calcular o fator de ajuste retroativo dinamicamente durante a T-010 (`canonical_us`), garantindo consistência matemática unificada.

### 🔴 CRÍTICO / ALTO: Survivorship Bias Maciço no Universo
* **Local:** `scripts/t006_build_index_compositions.py` (L107: `coverage_mode=snapshot`).
* **Mecanismo da Falha:** Embora documentado e aceito em `D-005`, o uso de um CSV atual da iShares para definir o universo de 2018 a 2026 introduz um survivorship bias letal.
* **Exploit Adversarial:** Qualquer empresa que faliu, foi adquirida por um prêmio baixo ou foi expulsa do índice entre 2018 e 2026 NÃO está no snapshot de hoje. Ao treinar um modelo em 2018 com empresas que "sabemos que sobreviverão até 2026", o Sharpe Ratio será astronomicamente inflado.
* **Solução:** Reverter a decisão D-005 se o objetivo for operar capital real. É obrigatório obter composição histórica por *effective date*. Se o custo for o bloqueio, utilizar bases gratuitas alternativas ou aceitar que backtests anteriores a 2025 são matematicamente inválidos.

### 🟡 MÉDIO: Perda de Eventos Macro por Merge Inadequado
* **Local:** `scripts/t011_ingest_macro_us.py` (L99-L106).
* **Mecanismo da Falha:** O script faz um `left merge` das séries do FRED no calendário de pregões da bolsa, e *depois* aplica o `ffill`.
* **Exploit Adversarial:** Se o FRED publicar um dado em feriado ou domingo, esse dado é **descartado** pelo `left merge`. Na segunda-feira, o sistema fará o `ffill` usando o dado de sexta-feira, em vez do dado de domingo. Para séries diárias de mercado (DGS10, VIX) o impacto é baixo pois não há publicações no domingo, mas para séries macroeconômicas puras, causará delay artificial (data loss).
* **Solução:** Fazer um outer merge (unindo todas as datas), aplicar o `ffill` e *somente então* filtrar para o calendário de pregões.

### 🟢 BAIXO: Distorção Variância SPC
* **Local:** `scripts/t008_quality_spc_and_blacklist.py` (L139-L141).
* **Mecanismo da Falha:** Ao excluir dias de split e usar `pct_change()`, calcula-se um retorno de 2 dias (T-2 a T). Isso infla levemente a variância (std) base do SPC, alargando os limites de controle indevidamente.
* **Solução:** Usar a solução de preços desajustados acima, calcular o retorno total verdadeiro considerando o fator de split e não dropar nenhuma linha.

---

## Hipóteses Investigadas

| Hipótese | Status | Justificativa |
|---|---|---|
| **H1 - Lookahead leak (Features)** | LIMPO | `shift(1)` implementado rigorosamente na T-011 (L29). Cutoff timestamp registrado. |
| **H2 - Data leakage no CV** | N/A | Faremos na Phase 2. |
| **H3 - Survivorship bias** | CRÍTICO | D-005 aceita snapshot atual. Invalida backtest pre-2026. |
| **H4 - Custo Subestimado** | N/A | - |
| **H5 - Sharpe inflado** | N/A | Garantido de ocorrer devido à H3. |
| **H6 - MDD subestimado** | N/A | - |
| **H7 - Ingestão Incremental Inconsistente** | CRÍTICO | `adjusted=True` misturado com appends quebra a série temporal. |

---

## Veredito Final: [[ REPROVADO ]]

O pipeline está reprovado para avanço. O uso de `adjusted=True` da Polygon combinado com anexação incremental (append) de arquivos Parquet cria quebras estruturais irreversíveis no preço. Recomenda-se acionar o Arquiteto para planejar as correções críticas (T-007 para `adjusted=False` e fator dinâmico no T-010) antes de iniciar qualquer modelagem.
