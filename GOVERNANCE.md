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

**Arquivos protegidos** (auditados e selados em `v1.14.0-motor-us`):

| Arquivo | Função | Auditorias |
|---------|--------|------------|
| `pipeline/painel_diario.py` | Venda defensiva SPC, Base 1 patrimônio real, duplo-caixa, resolução de datas para pregão real | Phase 5 completa, D-027, D-033, D-038 |
| `pipeline/02_ingest_prices_us.py` | Ingestão Polygon.io (OHLCV, dividendos, splits) | Phase 1 v2, D-007, D-026 |
| `pipeline/04_build_canonical.py` | Build canonical + operational_window | Phase 1 v2, D-026 |
| `pipeline/09_decide.py` | Motor C4 puro (TopN=20, Cad=10, K=10, cap=6%, min_market_cap=300M) | Phase 3-4, D-021, D-029, D-033, D-044, D-065 |
| `config/winner_us.json` | Declaração canônica do winner C4 com SHA256 das evidências | D-021, T-024 |

**Regras de proteção**:

1. Alterações nestes arquivos exigem ciclo completo: `Architect → Executor → Auditor duplo (Gemini + Kimi) → Curator`, com autorização explícita do Owner.
2. Um **pre-commit hook** no git bloqueia commits que alterem esses arquivos. Para sobrepor, usar: `MOTOR_OVERRIDE=1 git commit -m "descricao"`.
3. A tag `v1.14.0-motor-us` marca o snapshot auditado atual. Para restaurar: `git checkout v1.14.0-motor-us -- <arquivo>`.
4. Novas versões do motor devem gerar nova tag (`v1.15.0-motor-us`, etc.) após novo ciclo completo de auditoria.

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

Autosave do dry-run e automacao diaria do `emit-abertura` permanecem fora de
escopo desta decisao -- ver `TEMAS_PARA_ACAO.MD` F-18 (SALA) para a decisao de
politica pendente.

Ref: SALA D-107; USA D-132; R-018; R-020; R-049; R-050.

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
