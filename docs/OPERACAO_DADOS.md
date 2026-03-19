# Operação de Dados — SSOT vs Janela Operacional (D-026)

Este documento descreve **como a Fábrica US opera dados no dia-a-dia** sem sacrificar integridade histórica (anti-survivorship) e sem inviabilizar tempo de execução.

## Artefatos (2 parquets, 2 propósitos)

### 1) SSOT FULL — `data/ssot/canonical_us.parquet`

- **Conteúdo**: histórico completo desde 2018, com todas as colunas do canônico (SPC, `close_operational`, `market_cap`, etc).
- **Propósito**: pesquisa, auditoria forense, backtests, reconstruções completas.
- **Atualização**: **semanal** (ou sob demanda) via `pipeline/run_daily.py --full`.

### 2) Operação diária — `data/ssot/operational_window.parquet`

- **Conteúdo**: somente os **últimos ~504 pregões** (janela rolling) e somente o universo operacional usado na rotina diária.
- **Propósito**: pipeline diário (scores, decisão, painel) com tempo alvo < 2 min.
- **Atualização**: diária via ingestão incremental + rebuild da janela.

## Como a atualização diária funciona

Ao rodar:

```bash
python pipeline/run_daily.py --date YYYY-MM-DD
```

o orquestrador executa:

1. **Step 00** (`pipeline/00_incremental_ingest.py`)
   - Detecta `date_max` em `data/ssot/operational_market_data_raw.parquet`
   - Ingera **apenas os dias faltantes** até **D-1** do `target_date`
   - Rebuild completo da janela operacional:
     - `t008_quality_spc_and_blacklist_v2.py` (SPC/blacklist) sobre o raw operacional
     - `t009_exclude_bdrs_v2.py` (exclusão BDR)
     - `t010_build_canonical_us_v2.py` (gera `operational_window.parquet`)

2. Steps 05–12: rodam usando a janela operacional como base (`USA_OPS_CANONICAL_PATH`).

## Recuperação (gaps)

Se a rotina falhar em um dia, no dia seguinte o Step 00 calcula o intervalo faltante \([date_max+1, D-1]\) e ingere automaticamente todos os dias pendentes.

## Rotina semanal (reconciliação)

Ao rodar:

```bash
python pipeline/run_daily.py --full --date YYYY-MM-DD
```

o pipeline:

- Atualiza SSOT FULL (steps 01–04)
- Regera a janela operacional a partir do SSOT (`pipeline/rebuild_operational_window.py`)
- Executa steps 05–12 já sobre a janela operacional (modo diário)

## Variáveis de ambiente (internas)

O orquestrador define:

- `USA_OPS_CANONICAL_PATH=data/ssot/operational_window.parquet`
- `USA_OPS_RAW_PATH=data/ssot/operational_market_data_raw.parquet`
- `USA_OPS_BLACKLIST_PATH=data/ssot/blacklist_window_us.json`

Essas variáveis evitam “hardcode” de paths e garantem que scripts (T-012/T-013) operem no mesmo artefato.

