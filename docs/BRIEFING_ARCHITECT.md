# BRIEFING PARA O ARCHITECT — USA_OPS

> Este documento é a orientação formal do CTO para o Architect, com base nas decisões D-001, D-002 e D-003.
> O Architect deve ler este documento + `docs/PLANO_USA_OPS.md` + `docs/CORPUS_FABRICA_BR.md` antes de planejar qualquer task.

---

## Orientação do CTO

```json
{
  "orientacao_cto": {
    "workspace": "/home/wilson/USA_OPS",
    "estado_do_projeto": "Phase 0 — Fundação. Repo criado com governança, roadmap e plano completo. Nenhuma task executada ainda.",
    "decisao_do_owner": "Criar Fábrica US independente da Fábrica BR (RENDA_OPS). Universo: Russell 1000 + S&P SmallCap 600 excluindo tickers com BDR na B3 (~1.100 tickers, zero sobreposição). Motor a descobrir por backtest comparativo com venda defensiva desde o dia 1. Dados via Polygon.io. Broker BTG Internacional. Não aceitar winner AGNO (T122) como válido — reconstruir do zero.",
    "linha_de_conducao": "Aplicar metodologia RENDA_OPS desde o dia 1: governança por trinca, cadeia de skills, venda defensiva obrigatória, auditoria forense adversarial, anti-survivorship, anti-lookahead. Consultar CORPUS_FABRICA_BR.md antes de cada phase para não repetir os 9 erros documentados.",
    "escopo_imediato": {
      "task_id": "T-001",
      "descricao": "Setup completo do repositório: git init, venv, requirements.txt com dependências conhecidas (polygon-api-client, fredapi, pandas, numpy, xgboost, plotly, flask, pyarrow), placeholder lib/ e pipeline/",
      "detalhamento": [
        "git init com .gitignore e .env.example já criados",
        "requirements.txt com versões fixas",
        "Criar venv e instalar dependências",
        "Criar __init__.py em lib/ e pipeline/",
        "Verificar que import polygon funciona",
        "Registrar no CHANGELOG"
      ]
    },
    "restricoes": [
      "Não modificar nada no RENDA_OPS — fábricas 100% independentes",
      "Não assumir que nenhum parâmetro do AGNO (TopN=5, Cadence=10, threshold=0.45) é válido",
      "Composição histórica de índices OBRIGATÓRIA (anti-survivorship bias)",
      "Venda defensiva OBRIGATÓRIA em toda variante de backtest (não é camada opcional)",
      "Walk-forward TRAIN 2018→2022, HOLDOUT 2023→2026 (não alterar sem decisão do Owner)",
      "Custos reais BTG Internacional em todo backtest",
      "API resiliência (retry exponencial + fallback) desde o dia 1",
      "Toda feature com shift(1) anti-lookahead",
      "Nenhuma task marcada DONE sem PASS do Auditor"
    ],
    "insumos": {
      "arquivos_existentes": [
        "docs/PLANO_USA_OPS.md — plano completo com 36 tasks em 7 phases",
        "docs/CORPUS_FABRICA_BR.md — 15 lições positivas, 9 erros, 4 padrões de falha",
        "GOVERNANCE.md — regras do repo",
        "DECISION_LOG.md — D-001, D-002, D-003",
        "ROADMAP.md — status de todas as tasks",
        ".gitignore, .env.example — já criados"
      ],
      "decisoes_anteriores": [
        "D-001: Universo Russell 1000 + SmallCap 600 − BDRs",
        "D-002: Motor a descobrir por backtest com venda defensiva",
        "D-003: Dados via Polygon.io + FRED"
      ],
      "referencias_externas": [
        "RENDA_OPS/docs/CORPUS_FABRICA_BR.md — corpus original",
        "RENDA_OPS/data/ssot/bdr_universe.parquet — lista de BDRs para exclusão",
        "RENDA_OPS/lib/ — componentes portáveis (engine, metrics, io, adapters)",
        "AGNO_WORKSPACE/02_Knowledge_Bank/ — SPECs de venda defensiva (001-004)",
        "AGNO_WORKSPACE/src/data_engine/portfolio/T127_US_WINNER_DECLARATION.json — winner AGNO (NÃO usar como válido)"
      ]
    },
    "governanca": {
      "decision_log": "DECISION_LOG.md",
      "governance": "GOVERNANCE.md",
      "changelog": "CHANGELOG.md",
      "roadmap": "ROADMAP.md"
    }
  }
}
```

---

## Resumo para o Architect

Você está iniciando a **Fábrica US** — um projeto independente e paralelo à Fábrica BR (RENDA_OPS). O objetivo é construir um sistema operacional diário para o mercado de ações americano, seguindo exatamente a mesma metodologia que levou a Fábrica BR ao sucesso, mas corrigindo desde o início os erros que custaram semanas de retrabalho.

### O que já existe neste workspace:

- Governança completa (GOVERNANCE, DECISION_LOG, CHANGELOG, ROADMAP)
- Plano de execução com 36 tasks em 7 phases (`docs/PLANO_USA_OPS.md`)
- Corpus de lições da Fábrica BR (`docs/CORPUS_FABRICA_BR.md`)

### O que NÃO existe ainda:

- Nenhum código
- Nenhum dado
- Nenhuma configuração de ambiente

### Próximo passo: T-001 (Setup repositório)

Planeje a T-001 conforme a skill do Architect. Consulte o `docs/PLANO_USA_OPS.md` para detalhes da task e o `docs/CORPUS_FABRICA_BR.md` antes de iniciar.

### Lições críticas a aplicar desde a T-001:

1. **L-12 (resiliência API)**: retry exponencial no PolygonAdapter desde o dia 1
2. **E-08 (validação de entrada)**: nunca aceitar ticker sem validação
3. **L-06 (proveniência)**: MANIFESTO_ORIGEM para cada arquivo copiado
4. **L-01 (consultar corpus)**: ler CORPUS_FABRICA_BR.md antes de cada task
