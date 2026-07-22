# GOVERNANCE — USA_OPS

## 1) Identidade

Repositório operacional da Fábrica US (Russell 1000 + S&P SmallCap 600, excluindo BDRs B3).
Orientado a uso diário: dry-run e, posteriormente, operação real via BTG Internacional.

## 2) Cadeia de comando

```text
Owner <---> CTO <---> Architect ---> Executor ---> Auditor ---> Curator
```

- **Owner**: autoridade final. Toda execução exige autorização explícita.
- **CTO**: interlocutor técnico do Owner. Traduz, analisa, propõe — não executa.
- **Architect**: planeja e gera JSON de task a partir de orientações do CTO.
- **Executor**: implementa conforme JSON aprovado pelo Owner.
- **Auditor**: valida entrega do Executor. Emite PASS ou FAIL.
- **Curator**: registra conclusões nos documentos de governança após PASS.

## 3) Documentos de governança (trinca operacional)

| Documento | Finalidade | Quem escreve |
|-----------|-----------|--------------|
| `GOVERNANCE.md` | Regras fixas, políticas, restrições do repo | CTO (com aprovação do Owner) |
| `DECISION_LOG.md` | Decisões do Owner com contexto e justificativa | CTO (durante discussão com Owner) |
| `CHANGELOG.md` | Log técnico cronológico de mudanças | Executor (pós-task) / Curator (pós-audit) |

### Regras de escrita

- **Append-only**: nunca apagar entradas anteriores.
- **DECISION_LOG**: cada entrada tem ID sequencial (`D-NNN`), data, contexto, alternativas, decisão e responsável.
- **CHANGELOG**: cada entrada tem data ISO, task_id (quando aplicável) e descrição curta.
- **GOVERNANCE**: alterações via discussão CTO-Owner. Registrar a decisão de alteração no DECISION_LOG antes de editar.

## 4) Princípios operacionais

1. **Reprodutibilidade**: o pipeline deve produzir resultado determinístico dado os mesmos inputs.
2. **Rastreabilidade**: toda decisão, mudança e execução deve ser verificável nos documentos de governança.
3. **Dados regeneráveis fora do git**: parquets e outputs diários são gerados pelo pipeline, não versionados.
4. **Segurança**: `.env` e credenciais nunca no repositório.
5. **Evidências**: execuções de governança produzem gates verificáveis com status PASS/FAIL.
6. **Anti-sobreposição**: o universo US exclui todos os tickers que possuem BDR na B3, garantindo diversificação real entre as fábricas BR e US.

## 5) Fluxos de governança por natureza de trabalho

### 5.1 Tasks técnicas (backlog do ROADMAP.md)

```text
CTO orienta → Architect planeja → Owner autoriza → Executor implementa → Auditor valida → Curator registra
```

### 5.2 Rotina operacional diária (CICLO_DIARIO.md)

- Owner opera diretamente (pipeline + boletim)
- Validação automática no pipeline
- Auditoria consolidada semanal

### 5.3 Hotfixes

- Passam pela cadeia completa se envolvem lógica de pipeline
- Registrados no CHANGELOG como `fix:`

## 6) Políticas técnicas

### 6.1 Branch e versionamento

- Branch principal: `main`.
- Commits seguem conventional commits (`feat:`, `fix:`, `chore:`, `docs:`).

### 6.2 Dados

- Formato canônico: Parquet.
- Dados em `data/` são regeneráveis e excluídos do git via `.gitignore`.
- SSOT (Single Source of Truth) vive em `data/ssot/`.

#### 6.2.1 Dois parquets, dois propósitos (D-026)

| Artefato | Conteúdo | Propósito | Atualização |
|----------|----------|-----------|-------------|
| `data/ssot/canonical_us.parquet` | Todo o histórico desde 2018, todos os ~9.130 tickers (incluindo deslistados) | Anti-survivorship bias, backtest, auditoria forense, recálculo de scores | Semanal (reconciliação) ou sob demanda |
| `data/ssot/operational_window.parquet` | Últimos ~504 pregões, apenas tickers do universo operacional (~4.000-4.500 que passam no filtro de qualidade + market_cap >= 300M) | Pipeline diário: scoring, decisão, painel | Diária (ingestão incremental: date_max+1 até D-1) |

**Regras:**
- O pipeline diário (`run_daily.py`) opera exclusivamente sobre `operational_window.parquet`.
- `canonical_us.parquet` é read-only para o pipeline diário.
- Reconciliação semanal: regenerar `operational_window` a partir do `canonical` atualizado.
- Recuperação de gaps: se o pipeline falhar num dia, no dia seguinte a ingestão incremental busca todos os dias faltantes automaticamente (date_max+1 até D-1).

#### 6.2.2 Dataset de pesquisa congelado (research_dataset_us)

Para experimentos offline de motor, `backtest/research_dataset_us/` é o snapshot congelado e versionado dos insumos de pesquisa (`canonical_us.parquet`, `macro_us.parquet`, `scores_m3_us.parquet`) com `manifest.json` e SHA256.

Este diretório é exceção deliberada à regra geral de que dados Parquet são regeneráveis e excluídos do git: o objetivo é garantir reprodutibilidade de baseline em backtests. O SSOT vivo em `data/ssot/` continua sendo a fonte operacional; ele não deve ser usado como baseline de pesquisa em experimentos de motor.

Backtests de motor que dependem de `backtest/run_backtest_variants_us.py` devem apontar `US_RESEARCH_DATASET_DIR=backtest/research_dataset_us` ou passar paths explícitos para `load_inputs()`. Antes de executar, devem validar `manifest.json` e SHA256 via `backtest/research_dataset_us/verify_dataset.py`.

Novo freeze ou troca de versão do dataset exige ciclo formal, novo manifesto e registro em `DECISION_LOG.md`.

### 6.3 Ambiente

- Python via `.venv/` local ao workspace.
- Dependências em `requirements.txt`.
- Variáveis sensíveis em `.env` (nunca commitado).

### 6.4 Pipeline

- Orquestrador: `pipeline/run_daily.py`.
- Cada etapa deve ser idempotente para o mesmo dia.
- Logs em `logs/` (excluídos do git).
- Modo padrão (sem `--full`): opera sobre `operational_window.parquet`, ingestão incremental somente dos dias faltantes e tickers operacionais (~4.500). Tempo alvo: < 2 min.
- Modo full (`--full`): atualiza `canonical_us.parquet` com universo completo (~9.130 tickers), depois regenera `operational_window`. Uso semanal ou sob demanda.

### 6.5 Mercado US — Especificidades

- **Universo**: Russell 1000 + S&P SmallCap 600, excluindo tickers com BDR na B3 (~1.100 tickers).
- **Composição histórica**: obrigatório usar composição por data efetiva (anti-survivorship bias).
- **Liquidação**: T+1 (simplifica modelo de duplo-caixa vs D+2 da BR).
- **Custos**: usar dados reais do broker (BTG Internacional) no backtest.
- **Horário**: NYSE 9:30–16:00 ET. Pipeline roda após fechamento.
- **Proventos**: dividendos trimestrais. Stock splits mais frequentes que na B3.
- **Tank (caixa)**: Fed Funds Rate como proxy de retorno em caixa.
- **Dados**: Polygon.io (OHLCV, dividendos, splits, composição histórica) + FRED (macro).
- **Resiliência API**: retry exponencial + fallback obrigatórios desde o dia 1 (lição D-027 RENDA_OPS).

### 6.6 Blindagem do Motor Operacional (D-039)

**Arquivos protegidos** (auditados e selados em `v1.19.0-motor-us`):

| Arquivo | Função | Auditorias |
|---------|--------|------------|
| `pipeline/painel_diario.py` | Venda defensiva SPC, Base 1 patrimônio real, duplo-caixa, resolução de datas para pregão real e reconciliação D0 de rebalance (sell+buy) para autosave dry-run | Phase 5 completa, D-027, D-033, D-038, D-137 |
| `pipeline/02_ingest_prices_us.py` | Ingestão Polygon.io (OHLCV, dividendos, splits) | Phase 1 v2, D-007, D-026 |
| `pipeline/04_build_canonical.py` | Build canonical + operational_window | Phase 1 v2, D-026 |
| `pipeline/09_decide.py` | Motor C4 puro (TopN=20, Cad=10, K=10, cap=6%, min_market_cap=300M) | Phase 3-4, D-021, D-029, D-033, D-044, D-065 |
| `config/winner_us.json` | Declaração canônica do winner C4 com SHA256 das evidências | D-021, T-024 |

**Regras de proteção**:

1. Alterações nestes arquivos exigem ciclo completo: `Architect → Executor → Auditor duplo (Gemini + Kimi) → Curator`, com autorização explícita do Owner.
2. Um **pre-commit hook** no git bloqueia commits que alterem esses arquivos. Para sobrepor, usar: `MOTOR_OVERRIDE=1 git commit -m "descricao"`.
3. A tag `v1.19.0-motor-us` marca o snapshot auditado atual. Para restaurar: `git checkout v1.19.0-motor-us -- <arquivo>`.
4. Novas versões do motor devem gerar nova tag (`v1.20.0-motor-us`, etc.) após novo ciclo completo de auditoria.

#### 6.6.1 Protecao do SSOT append-only

O arquivo `data/ssot/ledger.jsonl` e protegido por politica append-only: commits que reduzam o numero de linhas sao bloqueados pelo pre-commit hook.

Esta protecao e distinta da blindagem de motor (`§6.6`): nao exige ciclo completo com Auditor duplo para appends legitimos (estes ocorrem automaticamente via `/salvar`), mas proibe rollback, truncamento ou qualquer operacao que apague registros existentes.

Modificacoes estruturais no ledger (ex: correcao de entrada incorreta) exigem task formal aprovada pelo Owner via cadeia Interlocutor -> CTO -> Architect -> Executor -> Auditor.

Ref: SALA D-036, D-035, R-025.

### 6.7 Regra de Documentação por Fábrica (D-061)

Decisões formalizadas na `SALA_DE_CONTROLE` que alterem código, skills, comportamento operacional ou produto da Fábrica US devem ser espelhadas na trinca do `USA_OPS` com ID próprio e referência cruzada explícita ao ID de origem.

**Convenção de prefixo (obrigatória)**: toda referência a decisão de outro workspace deve incluir o prefixo do repositório de origem.

- Exemplo: `SALA D-011` (decisão da SALA) vs `USA D-011` (decisão local do USA_OPS).
- Referências sem prefixo são interpretadas como locais ao workspace em que aparecem.
- Esta convenção aplica-se retroativamente à leitura de entradas existentes no `CHANGELOG.md` que mencionem `(D-003)` e `(D-004)` em 2026-04-11 — essas referências designam `SALA D-003` e `SALA D-004`, não `USA D-003`/`USA D-004`.

**Critério de espelhamento**:

1. Decisões da SALA que afetem apenas setup operacional da SALA (timer, autostart, organização de arquivos da SALA) **não** exigem entrada no `DECISION_LOG.md` do USA_OPS.
2. Decisões da SALA que alterem código de produto, skills operacionais ou comportamento do pipeline do USA_OPS **exigem** entrada com ID local e referência cruzada.
3. Decisões da SALA já cobertas por entradas USA com referência cruzada explícita dispensam novo espelhamento.

Alterações a esta seção exigem registro prévio no `DECISION_LOG.md`.

### 6.8 Launchers operacionais LIVE-REAL-TEST (F-16/F-17, D-131)

O regime `LIVE-REAL-TEST` (R-049) tem dois atalhos de desktop dedicados, que
orquestram exclusivamente `scripts/live_real_cutover.py` e
`scripts/friction_ruler.py` (nenhum arquivo blindado de §6.6 e tocado):

| Atalho | Script | Função |
|--------|--------|--------|
| `USA_REGISTRAR_ORDEM` | `scripts/registrar_ordem_real.sh` | Para cada fill real (BTG), registra o par BUY real + BUY sombra na mesma ação, com preço-sombra sugerido por auto-lookup em `data/ssot/operational_window.parquet` (`scripts/lookup_shadow_price.py`) e confirmação explícita antes de gravar. |
| `USA_ENCERRAR_DIA` | `scripts/encerrar_dia_real.sh` | Emite o boletim real-only (`emit-boletim`) e o relatório de fricção (`emit-friction-report`) do dia, reportando `n_live_real` via a mesma contagem do Trilho B/C. |

O corte inicial do regime (`freeze-dryrun` + `init-cutover --confirm`) e ato
único e irreversível - abre `data/live_real_test/ledger_real.jsonl` com o
aporte real (C0). Por isso nao recebe atalho de desktop; e executado uma única
vez pelo ciclo formal completo (`Architect -> Executor -> Auditor -> Curator`),
com autorização explícita do Owner (R-049, D-105/D-130).

Ref: SALA D-103, D-105, D-106; USA D-128, D-129, D-130, D-131; R-018; R-049.

### 6.9 Boletim real de abertura e roteamento do painel (F-16 execucao, D-107/D-132)

O corte inicial (`init-cutover --confirm`) foi executado em producao em
16/07/2026 (SALA D-107, USA D-132), abrindo
`data/live_real_test/ledger_real.jsonl` com APORTE C0 = US$ 20.008,72.

Novo subcomando `emit-abertura` em `scripts/live_real_cutover.py` (nao
blindado) compoe o boletim real de abertura: caixa/posicoes do ledger real +
Top-N operacional (`operational_ranking`/`target_weight`) do
`decision_<exec_day>.json` do dia, com preco de fechamento D-1 via
`pipeline.painel_diario.get_latest_prices` (leitura, sem alterar o arquivo
blindado).

`pipeline/servidor.py` (nao blindado) passa a rotear `/painel` (rota do dia)
para esse boletim real quando o regime LIVE-REAL-TEST estiver ativo (ledger
real com APORTE), preservando integralmente `/painel/<data>` como historico do
dry-run.

**Enderecos oficiais de gravacao**:

| Artefato | Caminho | Gerado por |
|----------|---------|------------|
| Boletim dry-run (diario) | `data/real/<market_day>.json` | `/salvar` (painel dry-run, inalterado) |
| Ledger real (SSOT do teste) | `data/live_real_test/ledger_real.jsonl` | `init-cutover --confirm` (ato unico) |
| Boletim real de abertura | `data/live_real_test/abertura_<exec_day>.json` | `emit-abertura` (novo, manual por ora -- ver F-18) |
| Boletim real de fechamento | `data/live_real_test/<exec_day>.json` | `emit-boletim` (atalho USA_ENCERRAR_DIA) |

Autosave do dry-run foi desmembrado para a F-18 e concluido na frente de
autosave autonomo (`SALA D-112` / `USA D-137`).

**Nota 2026-07-17 (D-138/SALA D-114)**: a frente 1 da F-18 foi concluida com
reescopo. A automacao diaria deixou de depender do `emit-abertura` e passou a
ser implementada pelo refresh automatico do contexto canonico
`data/ssot/contexto_analista_us.json` no ciclo diario + rollover automatico de
`draft_<exec_day>.json` pendente ao abrir `/painel` (ver §6.12).

Ref: SALA D-107, SALA D-112, SALA D-114; USA D-132, USA D-137, USA D-138;
R-018; R-020; R-049; R-050.

**Nota 2026-07-16 (D-133/SALA D-108)**: `pipeline/analise_us.py` passa a
detectar o regime LIVE-REAL-TEST pelo `ledger_real.jsonl` (evento `APORTE`) e,
quando ativo, ler caixa/posicoes diretamente do livro real
`data/live_real_test/ledger_real.jsonl` para gerar
`data/ssot/contexto_analista_us.json` (Analista real-aware). A ordenacao do
Top-20 de abertura/painel deixa de usar `rank` alfabetico e passa a seguir
`m3_rank`, preservando `operational_ranking` persistido pelo motor como fonte
unica. Nenhum arquivo blindado de `§6.6` foi tocado.

### 6.10 Boletim web primário LIVE-REAL-TEST (`/painel`) com rascunho e fechamento definitivo (D-136)

A partir de D-136, o endpoint `/painel` no regime LIVE-REAL-TEST deixa de ser
apenas leitor de snapshots e passa a ser a **interface primária operacional**
do dia, com três estados explícitos:

1. **Rascunho intermediário** (`data/live_real_test/draft_<exec_day>.json`):
   acumula operações (`COMPRA`/`VENDA`) com corretagem e preço-sombra sem tocar
   o ledger definitivo.
2. **Aplicação definitiva no ledger** (evento a evento, no encerramento):
   cada operação do rascunho vira evento em `ledger_real.jsonl` (`BUY`/`SELL` +
   `FEE` para corretagem) e, quando houver preço-sombra, evento espelho em
   `ledger_shadow.jsonl`.
3. **Fechamento do dia**: gera os artefatos derivados
   `data/live_real_test/<exec_day>.json` (boletim real-only) e
   `data/live_real_test/friction_report_<exec_day>.json`, depois arquiva o
   rascunho como `draft_<exec_day>_encerrado_<HHMMSS>.json`.

**Contrato de integridade**:

- `draft_*.json` é artefato intermediário operacional; **não** é SSOT nem
  substitui o ledger.
- O SSOT financeiro do regime real continua sendo `ledger_real.jsonl` (mais
  `ledger_shadow.jsonl` para o gêmeo sombra).
- Corretagem real deve ser registrada como `EventType.FEE`, impactando
  diretamente `cash_free`.
- O fechamento definitivo é atômico no sentido funcional: aplica ledger real +
  sombra, emite boletim/fricção e só então arquiva o rascunho.

**Fallback oficial preservado**:

- `scripts/registrar_ordem_real.sh` e `scripts/encerrar_dia_real.sh` seguem
  válidos como plano B operacional.
- A existência do fallback não altera o papel de `/painel` como primário.

Ref: SALA D-111, USA D-136, D-133, D-132, R-018, R-020, R-049, R-050.
Complemento de liquidação por venda em `VENDA` (`JA_NO_CAIXA` x
`EM_LIQUIDACAO`): ver §6.21 (D-150).

### 6.11 Autosave autonomo do dry-run US (F-18, D-137)

O ciclo diario US passa a executar autosave autonomo do dry-run apos a etapa de
decisao/painel (`pipeline/run_daily.py`), sem acao manual do Owner para
preenchimento de rebalance:

- `pipeline/painel_diario.py` expõe `compute_dryrun_autosave_operations`,
  mecanizando reconciliacao D0 de rebalance com vendas (fora da lista travada e
  aparo por carga) e compras por `target_weight`, respeitando ordem de
  `operational_ranking` e caixa disponivel (com fill parcial no ultimo ticker,
  quando necessario).
- `pipeline/servidor.py` extrai a logica de persistencia de `/salvar` para
  `apply_boletim_operations(payload)`, preservando a restricao "somente hoje"
  no endpoint HTTP e habilitando reuso interno pelo autosave.
- `pipeline/dryrun_autosave.py` executa catch-up idempotente por `market_day`,
  grava `data/real/<market_day>.json` apenas quando ausente e registra trilha
  append-only em `data/daily/autosave_log.jsonl`.

Escopo desta secao: frente 2 da F-18 (autosave do dry-run). A frente 1 foi
concluida em D-138 e esta detalhada em §6.12.

Ref: SALA D-112, SALA D-114, USA D-137, USA D-138, R-049, R-050.

### 6.12 Refresh diario do contexto Analista-USA e rollover automatico do rascunho (F-18 frente 1 re-escopada, D-138)

A partir de D-138, o ciclo diario US passa a fechar a lacuna operacional entre
SSOT e boletim web real sem depender de acao manual do Owner:

1. **Refresh canonico no pipeline diario**:
   `pipeline/run_daily.py` ganha o Step 14
   (`refresh_contexto_analista_us`), executado apos a etapa de autosave.
   O step regrava `data/ssot/contexto_analista_us.json` usando o ultimo pregao
   elegivel (`market_day` D-1), com comportamento nao bloqueante em caso de
   falha (warning em log).
2. **Rollover automatico ao abrir `/painel`**:
   `pipeline/servidor.py` chama `real_boletim_web.close_stale_drafts(...)`
   antes de renderizar o dia corrente em regime LIVE-REAL-TEST.
   Qualquer `draft_<exec_day>.json` anterior ao dia atual e fechado
   automaticamente (aplicacao no ledger, emissao de boletim/friccao e arquivo
   `draft_<exec_day>_encerrado_<HHMMSS>.json`).
3. **Livro de operacoes por ativo no boletim web**:
   `pipeline/ledger.py` expoe `build_operations_book(as_of_date)` (FIFO) e
   `pipeline/real_boletim_web.py` passa a exibir o bloco "Livro de operacoes por
   ativo" no `/painel`, com compras, vendas, quantidade liquida, custo medio,
   resultado realizado e nao realizado por ticker (cruzando Fech. D-1 do
   contexto canonico).

**Contrato operacional**:

- `market_day` permanece D-1 canonicamente (R-022) e `exec_day` permanece o dia
  civil de operacao.
- `emit-abertura` permanece disponivel como fallback manual, sem ser o mecanismo
  primario da frente 1.
- Nenhum arquivo blindado de §6.6 foi tocado neste pacote.

Ref: SALA D-114, USA D-138, R-018, R-022, R-049, R-050.

### 6.13 Ledger fracionario no caminho real + conferencia propositiva de nota oficial (D-139)

A partir de D-139, o regime LIVE-REAL-TEST US adota suporte fracionario no
**caminho real** sem alterar os arquivos blindados do motor:

1. **Semantica de quantidade no ledger real**:
   `pipeline/ledger.py` passa a tratar `qtd` como `float` (comparacao
   epsilon-safe), preservando FIFO e exportacoes com arredondamento controlado.
2. **Entrada operacional por principal investido**:
   no `/painel`, o registro de operacao aceita `valor_investido` (US$), com
   derivacao de quantidade por `qtd = valor_investido / preco` e fallback para
   `qtd` manual quando o valor nao for informado.
3. **Conferencia de nota como fonte da verdade (modo propositivo)**:
   `scripts/reconcile_broker_note.py` compara nota oficial vs ledger e grava
   apenas propostas/decisoes em `reconciliation_log.jsonl`; o script **nao**
   altera ledger automaticamente.
4. **Excecao controlada para 16/07/2026**:
   `scripts/migrate_ledger_20260716_fractional_fix.py` implementa migracao
   supervisionada (dry-run por default, backup obrigatorio e `--confirm`
   explicito) para corrigir os dois BUYs historicos (MRVI/HPP) do dia 16/07,
   refletindo principal de US$1.000 por ativo da nota oficial.

**Contrato de escopo**:

- `pipeline/painel_diario.py` permanece intocado.
- Nao ha auto-aplicacao de proposta de reconciliacao.
- A confirmacao de migracao real permanece ato explicito do Owner.

Ref: SALA D-115, USA D-139, R-018, R-020, R-023, R-049.

### 6.14 Gate de conferencia de nota oficial no Analista-USA (D-140)

A partir de D-140, o Analista-USA (skill consultiva/read-only) passa a
executar, como parte do proprio fluxo diario, a conferencia de notas oficiais
de corretagem contra o ledger real, reaproveitando o motor propositivo criado
em D-139:

1. **Descoberta de notas por pasta**:
   `scripts/reconcile_broker_note.py` ganha `discover_notes(notes_dir)` e
   `propose_dir(notes_dir, ledger_dir)` (e o subcomando CLI `propose-dir`), que
   varrem uma pasta e processam apenas arquivos `Confirm_*.pdf`
   (case-insensitive), ignorando qualquer outro arquivo (ex.: imagens).
2. **Gate obrigatorio no Analista-USA**:
   `.cursor/skills/analista-usa/SKILL.md` ganha o Passo 0c, executado apos o
   Passo 0b e somente quando `ctx["real_test"]["active"] == true`. O passo roda
   `propose-dir` contra `dados_oficiais_btg/` e, se o resultado indicar
   `has_blocking_divergence == true` (divergencia nao decidida ou erro de
   leitura de nota), o Analista **para o diagnostico** e comunica a divergencia
   ao Owner, sem prosseguir para os Passos 1-9.
3. **Retificacao permanece ato separado**:
   o Analista nunca executa `resolve` ou qualquer migracao por conta propria. A
   retificacao continua sendo comando explicito do Owner, via
   `scripts/reconcile_broker_note.py resolve` e/ou migracao supervisionada
   (D-139, §6.13), preservando o charter consultivo do Analista-USA e R-056.

**Contrato de escopo**:

- Nenhum arquivo blindado de §6.6 foi tocado.
- O Analista-USA continua sem autoridade de escrita no ledger real.
- A ausencia de notas na pasta ou a ausencia de divergencia nao bloqueia o
  diagnostico.

Ref: SALA D-116, USA D-140, USA D-139, SALA D-115, R-020, R-056.

### 6.15 Extensao da migracao supervisionada: HNGE 17/07 com preco interino (D-141)

A partir de D-141, o contrato de migracao supervisionada de §6.13 e estendido
para corrigir o BUY de HNGE de 17/07 no mesmo padrao operacional:

1. **Execucao de pendencia ja autorizada**:
   a migracao de MRVI/HPP de 16/07 (`scripts/migrate_ledger_20260716_fractional_fix.py`),
   aprovada em D-139, foi efetivamente executada com `--confirm`, backup
   obrigatorio e trilha de evidencia.
2. **Nova migracao supervisionada para HNGE**:
   `scripts/migrate_ledger_20260717_hnge_price_fix.py` corrige o evento
   `fedd1bfa-44d8-496e-9197-a42f9dd6a03b` para preco interino informado pelo
   Owner (US$86,76), com dry-run default, validacoes de precondicao e backup
   obrigatorio antes de sobrescrita.
3. **Sem auto-aplicacao e com reconciliacao pendente**:
   o preco interino nao substitui a precedencia da nota oficial. A confirmacao
   final do HNGE permanece vinculada ao mesmo gate de conferencia de §6.14
   (`scripts/reconcile_broker_note.py`) quando a nota oficial de 17/07 estiver
   disponivel em `dados_oficiais_btg/`.
4. **Regeneracao de artefatos derivados**:
   apos a correcao do ledger, os arquivos arquivais `2026-07-16.json`,
   `2026-07-17.json`, `friction_report_2026-07-16.json` e
   `friction_report_2026-07-17.json` foram regenerados pelos CLIs oficiais
   (`scripts/live_real_cutover.py emit-boletim` e
   `scripts/friction_ruler.py emit-friction-report`) para manter consistencia
   entre fonte e derivados.

**Contrato de escopo**:

- Nenhum arquivo blindado de §6.6 foi tocado.
- A retificacao continua dependente de comando explicito do Owner.
- O gate read-only do Analista-USA permanece sem autoridade de escrita.

Ref: SALA D-117, USA D-141, USA D-140, USA D-139, R-018, R-020, R-023, R-049,
R-056.

### 6.16 Caixa Livre de Balanco vs Caixa Livre Real no /painel (D-142)

A partir de D-142, o boletim web LIVE-REAL-TEST passa a diferenciar
explicitamente:

1. **Caixa Livre de Balanco**:
   valor derivado de `compute_cash` (aporte/dividendo/settlement menos
   retirada/buy/fee), sem mudanca de semantica financeira.
2. **Caixa Livre Real**:
   valor informado manualmente pelo Owner no encerramento do `/painel`, gravado
   no ledger real como evento observacional `CAIXA_REAL_INFORMADO`.
3. **Delta de Friccao (Balanco - Real)**:
   diferenca exibida no painel e no boletim diario para monitoramento de
   divergencias operacionais.

**Contrato tecnico**:

- O novo evento `CAIXA_REAL_INFORMADO` nao entra em `compute_cash`.
- O registro ocorre apenas no fechamento (`/painel/encerrar`) quando o campo
  opcional de Caixa Real for informado.
- A ausencia do campo mantem o comportamento anterior (sem gravar evento e sem
  friccao calculada).
- Nenhum arquivo blindado de §6.6 foi tocado.

Ref: SALA D-118, USA D-142, R-018, R-020, R-023, R-049, R-056.
Granularidade por venda e delta ajustado por liquidação: ver §6.21 (D-150).

### 6.17 Integracao do preview no /painel LIVE-REAL-TEST com Base 1 por cotizacao plena e ponte de friccao dinamica (D-143)

A partir de D-143, o `/painel` web LIVE-REAL-TEST incorpora a camada visual
operacional validada no preview, sem copiar o prototipo `scratch` nem herdar
seus atalhos de dados:

1. **Base 1 real por cotizacao plena (R-049)**:
   `pipeline/ledger.py` passa a expor `build_real_base1_series(...)`, calculada
   por cota ancorada no aporte real e neutra a novos aportes/retiradas no eixo
   de retorno.
2. **Sem overlay hardcoded de quantidade/preco**:
   o `/painel` deixa de depender de qualquer `QTY_FIXES`; leitura e exibicao
   usam exclusivamente o ledger real corrigido como SSOT (R-056).
3. **Grafico e sparklines sem dependencia externa**:
   o grafico Base 1 x CAGR e os sparklines 62d do Top-20 sao renderizados em
   SVG inline, sem `Chart.js` via CDN.
4. **Ponte de friccao Balanço -> Real -> NAV no proprio painel**:
   o boletim exibe e recalcula ao vivo (input de Caixa Livre Real no
   encerramento) a reconciliacao:
   Carteira + Caixa Livre de Balanco + Caixa Contabil -> Total Bruto ->
   Friccao Operacional/Total -> NAV -> Resultado -> Rentabilidade.
5. **Contrato de persistencia preservado (D-142)**:
   o recálculo em tela e apenas observacional; o valor oficial de Caixa Livre
   Real segue persistindo no encerramento (`/painel/encerrar`) como
   `CAIXA_REAL_INFORMADO`, sem alterar `compute_cash`.

**Contrato de escopo**:

- Nenhum arquivo blindado de §6.6 foi tocado.
- `pipeline/servidor.py` manteve o contrato de rota `/painel` sem alteracao de
  semantica.
- A reconciliacao final de HNGE contra nota oficial de 17/07 continua fora
  desta task (R-056 / D-141).

Ref: SALA D-119, USA D-143, USA D-142, USA D-141, R-018, R-023, R-049, R-056.
Balancete/DFC simplificados com flag por venda: ver §6.21 (D-150).

### 6.18 Reconciliacao autonoma BTG com invariante de caixa (D-145)

A partir de D-145, o regime LIVE-REAL-TEST US adota reconciliacao autonoma
blindada, superando parcialmente o contrato propositivo de §6.13/§6.14 para
divergencias imateriais:

1. **Ledger real versionado**: `data/live_real_test/ledger_real.jsonl`,
   `reconciliation_log.jsonl` e `reconciliation_checkpoint.json` passam a ser
   rastreados em git, com protecao append-only equivalente a §6.6.1 (pre-commit
   hook local bloqueia reducao de linhas).
2. **Script de aplicacao**: `scripts/reconcile_and_apply.py` (distinto do
   `reconcile_broker_note.py` propositivo) alinha quantidade/preco/amount/
   commission a nota oficial via par `CORRECTION` + evento reemitido, SEMPRE
   append-only, com backup previo obrigatorio.
3. **Invariante de caixa**: toda auto-aplicacao exige `|amount_diff + fee_diff|
   < US$ 1,00`. Divergencia igual ou acima do limiar NUNCA e auto-aplicada;
   permanece como `PROPOSTA` para migracao supervisionada (R-056, inalterado
   para esses casos).
4. **Checkpoint forward-only**: nota com todos os itens resolvidos avanca
   `reconciliation_checkpoint.json`; nota com item bloqueado nao avanca.
5. **Auditoria obrigatoria**: a skill `reconciliador-btg` invoca o `auditor`
   (Gemini 3.1 Pro) apos cada aplicacao, com maximo de 2 iteracoes; falha na
   2a escala ao Owner sem forcar PASS.

**Contrato de escopo**:

- Nenhum arquivo blindado de §6.6 foi tocado.
- `pipeline/ledger.py` nao foi alterado (reaproveita `EventType.CORRECTION`
  ja existente).
- R-056 permanece vigente para divergencias >= US$ 1,00.

Ref: SALA D-123, USA D-145, R-018, R-020, R-023, R-038, R-049, R-056, R-058.

### 6.19 Paridade completa do preview no /painel LIVE-REAL-TEST (D-144)

A partir de D-144, o `/painel` LIVE-REAL-TEST fecha a diferenca residual para o
padrao validado em `scratch/preview.html`, com contrato de producao (sem copiar
atalhos de dados do scratch):

1. **Layout operacional completo (01-08)**:
   o painel passa a exibir 8 secoes numeradas (Evolucao, Acao do dia, Carteira
   real, Livro de operacoes, Top-20, Operacoes sugeridas pelo Analista,
   Balanco, Reconciliacao de caixa) com identidade visual azul do USA.
2. **Acao do dia + defensivas no proprio /painel**:
   o renderer passa a calcular sugestoes defensivas a partir de `holdings`
   (`heat_pct`, `spc_status`, `drawdown_pct`) e do estado do forno
   (`action`, `is_rebalance_day`), sem escrever no ledger.
3. **Secao local de Operacoes sugeridas pelo Analista**:
   o bloco e estritamente local (frontend), sem formulario POST e sem nova rota
   de escrita; serve como area de preenchimento/simulacao.
4. **Contrato de escrita preservado**:
   os fluxos operacionais existentes permanecem oficiais e inalterados:
   `Salvar rascunho`, `Remover` e `Encerrar o Dia`.
5. **Sem hardcode e sem CDN**:
   `QTY_FIXES` continua proibido no caminho produtivo e o painel permanece sem
   dependencia de `Chart.js`/CDN.

**Contrato de escopo**:

- `pipeline/servidor.py` manteve o contrato da rota `/painel` sem adicionar
  endpoint novo.
- Nenhum arquivo blindado de §6.6 foi tocado.
- Abrir `/painel` nao cria nova escrita para o dia corrente por efeito de
  renderizacao.

Ref: SALA D-121, USA D-144, USA D-143, R-018, R-023, R-049, R-056.

### 6.20 Delegacao do gate de conferencia ao Reconciliador BTG (D-146)

A partir de D-146, o contrato do gate de conferencia do Analista-USA e
ajustado para remover execucao duplicada e preservar bloqueio apenas no caso
material:

1. **Execucao da reconciliacao sai do Analista-USA**:
   o Passo 0c da skill `analista-usa` deixa de executar
   `scripts/reconcile_broker_note.py` e `scripts/reconcile_and_apply.py`.
   A execucao permanece exclusiva da skill `reconciliador-btg` (R-058).
2. **Passo 0c vira leitura de estado oficial**:
   o Analista-USA passa a ler
   `data/live_real_test/reconciliation_checkpoint.json` e
   `data/live_real_test/reconciliation_log.jsonl`.
3. **Bloqueio apenas para divergencia material pendente**:
   o diagnostico so bloqueia quando houver `PROPOSTA` sem `DECISAO` com
   `|cash_delta| >= US$ 1,00`, no mesmo invariante de R-058.
4. **Divergencia imaterial nao bloqueia o Analista**:
   itens com `|cash_delta| < US$ 1,00` seguem no fluxo autonomo da
   `reconciliador-btg` e nao geram bloqueio consultivo.

**Contrato de escopo**:

- Esta secao supera parcialmente o §6.14 (D-140) apenas na parte de execucao
  pelo Analista e no criterio de bloqueio por "qualquer divergencia".
- Permanecem vigentes: R-056 (nota oficial como SSOT para conferencia),
  R-058 (autonomia sob invariante de caixa), e a vedacao de escrita do
  Analista-USA no ledger real.
- Nenhum arquivo blindado de §6.6 e tocado por esta mudanca de skill.

Ref: SALA D-124, USA D-146, SALA D-116, USA D-140, SALA D-123, USA D-145,
R-056, R-058.

### 6.21 Flag de liquidação por venda no /painel + Balancete/DFC simplificados (D-150)

A partir de D-150, o `/painel` LIVE-REAL-TEST US passa a carregar, por venda,
o estado operacional de liquidação e a refletir isso de forma auditável no
ledger e no fechamento:

1. **Flag obrigatoria por venda (`liquidacao`)**:
   o rascunho web passa a exigir, para cada `VENDA`, uma das opcoes:
   `JA_NO_CAIXA` ou `EM_LIQUIDACAO`.
2. **Persistencia append-only no ledger real**:
   o evento `SELL` preserva o registro economico da ordem; quando a opcao e
   `JA_NO_CAIXA`, o fechamento gera tambem `SETTLEMENT` same-day com `ref_id`
   do `SELL`, sem sobrescrever eventos historicos.
3. **Semantica de caixa ajustada para conciliacao por ref_id**:
   `compute_cash` passa a calcular `cash_accounting` por `SELL.amount -
   settled_by_ref`, mesmo quando `settle_date` do `SELL` estiver no futuro,
   eliminando dupla contagem quando ja existe `SETTLEMENT` antecipado.
4. **Balancete simplificado e DFC simplificado no `/painel`**:
   secoes 07/08 passam a explicitar:
   Caixa Livre de Balanco, Caixa Contabil, Caixa Livre Real BTG,
   Delta Livre-Real, corretagem do dia/acumulada e (quando houver caixa
   contabil) Delta ajustado por liquidacao.
5. **Neutralidade de NAV/Base1 preservada**:
   a reclassificacao entre `cash_accounting` e `cash_free` nao altera o total
   economico (`carteira + cash_free + cash_accounting`) nem a cotizacao plena
   (R-049).
6. **Remediacao historica PENG 21/07**:
   a correcao ocorre por script supervisionado append-only que adiciona
   `SETTLEMENT` referenciado ao `SELL` de 2026-07-21 (sem rewrite da linha
   original), com regeneracao de artefato derivado do dia.

**Contrato de escopo**:

- Nenhum arquivo blindado de §6.6 e tocado.
- A flag e obrigatoria no fluxo web atual; rascunhos legados sem o campo
  recebem fallback de compatibilidade para `JA_NO_CAIXA` no fechamento.
- O valor de `CAIXA_REAL_INFORMADO` segue observacional (D-142), sem entrada
  em `compute_cash`.

Ref: SALA D-132, USA D-150, USA D-142, USA D-143, R-006, R-018, R-023, R-049.

### 6.22 Novacao da reconciliacao 17/07 e novo contrato RECON_ADJUST (D-147)

A partir de D-147, o contrato tecnico da reconciliacao autonoma BTG e
endurecido para eliminar dupla contagem estrutural e blindar investido/caixa:

1. **Scanner com semantica de evento ativo (R-038)**:
   `scripts/reconcile_broker_note.py` e `scripts/reconcile_and_apply.py`
   passam a excluir eventos cancelados por `CORRECTION.ref_id` ao comparar
   nota x ledger.
2. **Auto-aplicacao restrita a qtd/preco**:
   divergencia imaterial sem impacto de caixa e aplicada via
   `EventType.RECON_ADJUST` (ref_id do evento ativo, `amount=0` por
   construcao), sem reemitir BUY/SELL/FEE.
3. **Impacto de caixa sempre supervisionado**:
   qualquer divergencia com `abs(amount_diff) > US$ 0,01` ou
   `abs(commission_diff) > US$ 0,01` NUNCA e auto-aplicada, mesmo com
   `|cash_delta| < US$ 1,00`; permanece em `PROPOSTA` para fluxo de R-056.
4. **Novacao append-only da nota 17/07**:
   os pares `CORRECTION + BUY reemitido` da auto-reconciliacao anterior sao
   superados por migracao append-only que:
   (a) cancela o BUY reemitido, (b) restaura base pre-reconciliacao e
   (c) aplica `RECON_ADJUST` para qtd/preco, com relink de FEE para o BUY
   restaurado, preservando caixa e investido.
5. **Gate consultivo do Analista-USA**:
   o Passo 0c passa a bloquear por `abs(cash_delta) > US$ 0,01`, alinhado ao
   novo contrato de supervisao de caixa.

**Contrato de escopo**:

- Esta secao supersede parcialmente §6.18 e §6.20 apenas na parte de
  auto-aplicacao de `amount`/`commission` e no limiar do gate consultivo.
- Permanecem vigentes: append-only, checkpoint forward-only e vedacao de
  escrita do Analista-USA no ledger real.
- Nenhum arquivo blindado de §6.6 e tocado por esta mudanca.

Ref: SALA D-125, USA D-147, SALA D-124, USA D-146, R-038, R-056, R-058, R-059.

### 6.23 Invalidacao do estudo defensivo V1 e contrato de harness V2 (D-154)

Os vereditos registrados em USA D-153 para
`T-SDC-DEFENSIVE-REINVEST-POLICY-US-V1` sao invalidos para qualquer finalidade
decisoria. A V1 herdou a ancora LIVE `2026-04-16` em uma janela iniciada em
2021, deixando os bracos-base sem exposicao na maior parte da amostra e o A2
em deadlock por bloqueios sem reset e posicoes-po ocupando vagas. Os artefatos
da V1 permanecem preservados como evidencia historica invalida.

O harness de `T-SDC-DEFENSIVE-REINVEST-POLICY-US-V2` adota:

1. primeiro rebalance no primeiro dia elegivel por scores;
2. phase sweep dos offsets 0 a 9 da cadencia 10;
3. pool de caixa defensivo, impedindo compra fora do gatilho estudado;
4. venda defensiva integral por contagem exata de acoes;
5. reset de `defensive_blocked` em cada ciclo;
6. posicao-po abaixo de 0,1% do equity sem ocupar vaga ativa;
7. gates G1-G6 executados antes de metricas ou bootstrap.

O teste regressivo comprovou que o helper compartilhado de venda integral pode
vender 98 de 99 acoes por divisao em ponto flutuante. Sua correcao e avaliacao
de impacto historico exigem task separada; a V2 usa wrapper local exato. Nenhum
arquivo blindado de §6.6 foi tocado.

Ref: SALA D-136, USA D-154, USA D-153, R-041, R-046, R-048, R-061.

## 7) Gate de paridade metodológica com RENDA_OPS (D-009, D-012)

**Regra**: toda task que introduzir um mecanismo, threshold, filtro ou lógica de pipeline **deve** demonstrar correspondência explícita com o RENDA_OPS antes de ser aprovada. Se o mecanismo não existir no RENDA_OPS, o Architect deve declarar isso no JSON da task e justificar a divergência. O Auditor deve verificar este gate.

### 7.1) Barreira 1 — Checklist obrigatório na orientação do CTO (D-012)

Toda orientação Modo 2 do CTO para o Architect que contenha **qualquer** threshold, gate numérico, filtro, critério de aprovação ou mecanismo de pipeline **deve** incluir a seção `parity_cto_check`:

```json
"parity_cto_check": [
  {
    "item": "<nome do threshold/gate/filtro>",
    "exists_in_renda_ops": "sim/não",
    "renda_ops_reference": "<path ou 'n/a'>",
    "if_not_exists_justification": "<justificativa ou 'n/a'>",
    "requires_owner_approval": true/false
  }
]
```

- Se `exists_in_renda_ops = não` e `requires_owner_approval = true`: o CTO deve **sinalizar explicitamente ao Owner** antes de o Architect receber a orientação.
- Se o CTO omitir a seção `parity_cto_check` em uma orientação que contenha critérios numéricos, o Architect **deve** rejeitar (ver §7.2).

**Motivação**: o CTO violou o D-009 duas vezes (outlier_rate na T-008v2, median_tickers na T-012), introduzindo thresholds sem correspondência no RENDA_OPS. Esta barreira força a declaração explícita na origem.

### 7.2) Barreira 2 — Rejeição obrigatória pelo Architect (D-012)

O Architect **deve** verificar, antes de produzir o JSON de task, se a orientação do CTO contém thresholds, gates ou filtros numéricos. Se contiver:

1. Verificar presença da seção `parity_cto_check` na orientação.
2. Se **ausente**: devolver ao CTO com `FAIL — parity_cto_check ausente (D-012)` antes de produzir qualquer JSON.
3. Se **presente**: validar cada item contra o RENDA_OPS. Se `exists_in_renda_ops = não` e `requires_owner_approval = true`, confirmar que o Owner foi consultado.

**Checklist obrigatório no JSON de task (campo `parity_check`):**
1. Mecanismo existe no RENDA_OPS? (sim/não, com path de referência)
2. Se sim: parâmetros idênticos? Se não, justificativa da diferença.
3. Se não existe no RENDA_OPS: justificativa técnica para introdução, com aprovação explícita do Owner.

**Motivação**: a Fábrica US é uma réplica metodológica da Fábrica BR adaptada ao mercado americano. Divergências só são aceitáveis quando impostas pelas diferenças de mercado (ex.: T+1 vs D+2, Fed Funds vs CDI), nunca por decisão autônoma de um agente.

## 8) Corpus de referência

Antes de iniciar qualquer fase, consultar:
- `docs/CORPUS_FABRICA_US.md` — lições aprendidas da Fábrica US (Phases 0-3), divergências justificadas, lições cruzadas BR↔US
- `docs/CORPUS_FABRICA_BR.md` — lições aprendidas da Fábrica BR
- `docs/PLANO_USA_OPS.md` — plano de execução completo
- `RENDA_OPS/docs/CORPUS_FABRICA_BR.md` — corpus original

## 9) Vigência

Esta governança entra em vigor com o primeiro commit que a inclui.
Alterações exigem registro prévio no `DECISION_LOG.md`.
