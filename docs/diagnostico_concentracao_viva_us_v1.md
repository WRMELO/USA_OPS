# Diagnostico de Concentracao Viva US v1

## Escopo

- task_id: `T-SDC-USA-LFS-CAP-DIAGNOSTIC-LLM-MATRIX-V1`
- market_day avaliado: `2026-07-23`
- contexto gerado em: `2026-07-24T20:09:32.394979+00:00`
- fonte: `pipeline/analise_us.py` (`build_context`) com `./.venv/bin/python pipeline/analise_us.py --real-test auto`

## Regra de comparacao

- cap nominal do forno US (`forno.max_weight_cap`): `0.06` (6.00%)
- metrica usada por posicao: `carga_termica_pct` do `contexto_analista_us.json`
- classificacao:
  - `ACIMA_CAP` se `carga_termica_pct > 6.00`
  - `DENTRO_CAP` caso contrario

## Holdings ordenados por carga termica

| ticker | valor_mercado_usd | carga_termica_pct | status_vs_cap |
| --- | ---: | ---: | --- |
| FCEL | 1255.67 | 6.32 | ACIMA_CAP |
| MXL | 1251.75 | 6.30 | ACIMA_CAP |
| LQDA | 1213.25 | 6.10 | ACIMA_CAP |
| MRVI | 1190.64 | 5.99 | DENTRO_CAP |
| REPL | 1189.34 | 5.98 | DENTRO_CAP |
| VRNS | 1124.79 | 5.66 | DENTRO_CAP |
| SLS | 1120.05 | 5.64 | DENTRO_CAP |
| RLJ | 1078.54 | 5.43 | DENTRO_CAP |
| VPG | 1059.45 | 5.33 | DENTRO_CAP |
| URGN | 1058.17 | 5.32 | DENTRO_CAP |
| HNGE | 1039.53 | 5.23 | DENTRO_CAP |
| SNOW | 992.79 | 4.99 | DENTRO_CAP |
| PGNY | 968.32 | 4.87 | DENTRO_CAP |
| GH | 963.90 | 4.85 | DENTRO_CAP |
| TOI | 958.67 | 4.82 | DENTRO_CAP |
| HPP | 930.12 | 4.68 | DENTRO_CAP |
| RDVT | 916.28 | 4.61 | DENTRO_CAP |
| PENG | 0.04 | 0.00 | DENTRO_CAP |

## Leitura objetiva

- 3 de 18 holdings ficaram acima do cap nominal de 6.00% no snapshot atual.
- pico observado no snapshot: `6.32%` (FCEL), isto e `+0.32 p.p.` acima do cap nominal.

## Nota operacional

Este diagnostico e consultivo/read-only (R-020). Nao executa venda automatica e nao altera o motor.
