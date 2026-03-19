# T-034 — Reexecução e Higiene de Artefatos

Data: 2026-03-19  
Referência: D-026

## Objetivo

Registrar de forma explícita:

1. Quais artefatos foram **sobrescritos** na reexecução da T-034;
2. Quais artefatos temporários foram **desativados** (removidos) por não serem parte do estado operacional final.

## Artefatos sobrescritos (estado ativo)

Regerados com o mesmo nome:

- `data/ssot/operational_market_data_raw.parquet`
- `data/ssot/us_universe_operational_window.parquet`
- `data/ssot/blacklist_window_us.json`
- `data/ssot/bdr_exclusion_list_window.json`
- `data/ssot/operational_window.parquet`
- `data/ssot/t008v2_quality_report_window.json`
- `data/ssot/t009v2_bdr_exclusion_report_window.json`
- `data/ssot/t010v2_operational_window_report.json`
- `logs/operational_incremental_ingest.json`
- `data/daily/decision_2026-03-19.json`
- `data/daily/painel_2026-03-19.html`

## Artefatos desativados (temporários)

Não fazem parte do estado final do pipeline diário e são removidos automaticamente:

- `data/ssot/operational_market_data_raw_delta.parquet`
- `logs/t007_ingestion_report_delta.json`
- `logs/t007_failures_delta.json`
- `data/ssot/tmp_t008_window_chunks/` (recriado limpo a cada execução)

### Observação da reexecução (2026-03-19)

- Como não havia gap de datas (`start_date > end_date` no delta), o pipeline não precisou gerar novos arquivos `*_delta`.
- Mesmo assim, a lógica de higiene mantém remoção defensiva de resíduos antigos.
- Estado final validado: **não há `*_delta` residual em `data/ssot` nem em `logs`**.

## Regra operacional aplicada

- O modo diário (`run_daily.py` sem `--full`) mantém apenas artefatos operacionais persistentes.
- Artefatos intermediários `delta` e chunks temporários são descartados para evitar ambiguidade em auditoria/curadoria.
- O report `logs/operational_incremental_ingest.json` inclui `deactivated_artifacts` para rastreabilidade.
