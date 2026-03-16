# USA_OPS — Fábrica US

Repositório operacional da Fábrica US. Orientado a uso diário em dry-run e, posteriormente, operação real via BTG Internacional.

**Ancestral direto**: RENDA_OPS (Fábrica BR)
**Ancestral de R&D**: AGNO_WORKSPACE (70+ tasks, SPECs 001-004)
**Decisão de criação**: D-029 (RENDA_OPS/DECISION_LOG.md)

## Identidade

| Atributo | Valor |
|----------|-------|
| Mercado | NYSE / NASDAQ |
| Moeda | USD |
| Universo | Russell 1000 + S&P SmallCap 600 − BDRs B3 (~1.100 tickers) |
| Liquidação | T+1 |
| Tank (caixa) | Fed Funds Rate / Treasury |
| Dados | Polygon.io (pago) + FRED (gratuito) |
| Broker | BTG Internacional (conta do Owner) |
| Motor | A descobrir via backtest comparativo |

## Relação com a Fábrica BR

As duas fábricas são **100% independentes**:
- Capital próprio separado
- Universos sem sobreposição (BDRs excluídos do universo US)
- Motores independentes (winner US a descobrir)
- Operação independente (servidores em portas diferentes)

## Uso Diário (após Phase 5)

```bash
source .venv/bin/activate
./iniciar.sh              # porta 8788
# Browser: http://localhost:8788
```

## Estrutura

```
USA_OPS/
├── config/          Configurações do winner, modelo ML, blacklist
├── data/
│   ├── ssot/        Dados canônicos (atualizáveis)
│   ├── features/    Features e predições (regeneráveis)
│   ├── models/      Modelo XGBoost persistido
│   ├── portfolio/   Curvas e resultados do winner
│   ├── real/        Posição real por dia
│   ├── cycles/      Artefatos diários (painel + boletim)
│   └── daily/       Output diário (decisões)
├── pipeline/        Scripts operacionais numerados (01-12) + orquestrador
├── lib/             Módulos compartilhados (adapters, metrics, engine)
├── backtest/        Backtesting comparativo
├── docs/            Documentação técnica e corpus
├── scripts/         Scripts utilitários
└── logs/            Logs de execução diária
```

## Variáveis de Ambiente

Criar `.env` na raiz com:

```
POLYGON_API_KEY=<sua_chave>
```

## Proveniência

O arquivo `MANIFESTO_ORIGEM.json` mapeia cada arquivo deste repo ao seu ancestral.
