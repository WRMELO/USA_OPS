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

### 6.3 Ambiente

- Python via `.venv/` local ao workspace.
- Dependências em `requirements.txt`.
- Variáveis sensíveis em `.env` (nunca commitado).

### 6.4 Pipeline

- Orquestrador: `pipeline/run_daily.py`.
- Cada etapa deve ser idempotente para o mesmo dia.
- Logs em `logs/` (excluídos do git).

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

### 6.6 Blindagem do Motor Operacional

Após auditoria forense dual (Gemini + Kimi) com PASS, os arquivos do motor serão protegidos com:
1. Tag git (`v1.0.0-motor-us`)
2. Pre-commit hook bloqueando alterações sem flag `MOTOR_OVERRIDE=1`

## 7) Corpus de referência

Antes de iniciar qualquer fase, consultar:
- `docs/CORPUS_FABRICA_BR.md` — lições aprendidas da Fábrica BR
- `docs/PLANO_USA_OPS.md` — plano de execução completo
- `RENDA_OPS/docs/CORPUS_FABRICA_BR.md` — corpus original

## 8) Vigência

Esta governança entra em vigor com o primeiro commit que a inclui.
Alterações exigem registro prévio no `DECISION_LOG.md`.
