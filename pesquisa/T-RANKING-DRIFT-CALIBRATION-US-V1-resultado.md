# T-RANKING-DRIFT-CALIBRATION-US-V1 - Resultado

- top_n utilizado: **20**
- eventos totais elegiveis: **167**
- ciclos observados: **2**

## Integridade e pre-registro

- Pre-registro: `pesquisa/T-RANKING-DRIFT-CALIBRATION-US-V1-preregistration.md`
- SHA256 confirmado:
  - `operational_window.parquet`: `6c97f426b221e04785096118c084dd8cc3d2b8c37e11f0c9ea0301c0aeecadec`
  - `scores_m3_us.parquet`: `6e2f5538860b321214bb00c7c6449a7452c2d7a3625a45c71b573d79bdb2fe57`

## Estatisticas por split

| Split | n_signal | n_control | media_signal | media_controle | delta_media (signal-controle) | IC95 delta | taxa_neg_signal | taxa_neg_controle |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| overall | 128 | 39 | 0.042965 | 0.037374 | 0.005591 | [-0.037422, 0.052442] | 25.781% | 28.205% |
| calibration | 90 | 36 | 0.052946 | 0.037850 | 0.015097 | [-0.034810, 0.063217] | 18.889% | 30.556% |
| validation | 38 | 3 | 0.019324 | 0.031660 | -0.012335 | [-0.041209, 0.022007] | 42.105% | 0.000% |

## Veredito

**INCONCLUSIVO**
- IC95 da diferenca media cruza zero na validacao

## Observacoes

- Esta task e read-only em motor/painel/skill; resultado serve como insumo para eventual ciclo PDCA separado.
- Se veredito INCONCLUSIVO por amostra, manter coleta e reavaliar em janela adicional.
