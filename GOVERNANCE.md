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

**Arquivos protegidos** (auditados e selados em `v1.5.0-motor-us`):

| Arquivo | Função | Auditorias |
|---------|--------|------------|
| `pipeline/painel_diario.py` | Venda defensiva SPC, Base 1 patrimônio real, duplo-caixa, resolução de datas para pregão real | Phase 5 completa, D-027, D-033, D-038 |
| `pipeline/02_ingest_prices_us.py` | Ingestão Polygon.io (OHLCV, dividendos, splits) | Phase 1 v2, D-007, D-026 |
| `pipeline/04_build_canonical.py` | Build canonical + operational_window | Phase 1 v2, D-026 |
| `pipeline/09_decide.py` | Motor C4 puro (TopN=20, Cad=10, K=10, cap=6%, min_market_cap=300M) | Phase 3-4, D-021, D-029, D-033, D-044 |
| `config/winner_us.json` | Declaração canônica do winner C4 com SHA256 das evidências | D-021, T-024 |

**Regras de proteção**:

1. Alterações nestes arquivos exigem ciclo completo: `Architect → Executor → Auditor duplo (Gemini + Kimi) → Curator`, com autorização explícita do Owner.
2. Um **pre-commit hook** no git bloqueia commits que alterem esses arquivos. Para sobrepor, usar: `MOTOR_OVERRIDE=1 git commit -m "descricao"`.
3. A tag `v1.5.0-motor-us` marca o snapshot auditado atual. Para restaurar: `git checkout v1.5.0-motor-us -- <arquivo>`.
4. Novas versões do motor devem gerar nova tag (`v1.5.0-motor-us`, etc.) após novo ciclo completo de auditoria.

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
