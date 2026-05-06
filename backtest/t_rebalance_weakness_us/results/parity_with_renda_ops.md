# Paridade metodologica BR x US (T-REBALANCE-WEAKNESS-US-FASE-B-PREP)

- Gerado em UTC: 2026-05-06T16:23:19.725090+00:00
- Filtro comum: sample_group=TOPN_ALL, split=HOLDOUT, lookback_L=5
- Fonte BR: `/home/wilson/RENDA_OPS/backtest/t082_rebalance_weakness/results/observations.csv`
- Fonte US: `/home/wilson/USA_OPS/backtest/t_rebalance_weakness_us/results/observations_top20.csv`

## Q1 — Magnitude do sinal BR vs US

Comparacao do contraste principal (INSTAVEL - ESTAVEL) em `log_ret_3` no mesmo corte (TOPN_ALL, HOLDOUT, L=5):
- BR: -1.33 p.p.
- US: -1.01 p.p.
- Leitura: ambos mostram degradacao para INSTAVEL contra ESTAVEL no horizonte de 3 dias; a ordem de grandeza e semelhante.
- Amostras INSTAVEL/ESTAVEL: BR=613/7374 ; US=1425/14872.

## Q2 — Tautologia em became_instavel_K

Padrao observado para `became_instavel_1` dentro do grupo `spc_status=INSTAVEL` (mesma definicao do BR e do US):
- BR share(became_instavel_1 == 1 | INSTAVEL): +100.00%
- US share(became_instavel_1 == 1 | INSTAVEL): +100.00%
- Conclusao: a tautologia e estrutural e igual nas duas fabricas, pois a janela futura inclui o proprio `d_reb`.

## Q3 — Gate do motor BR: blocked_bc vs spc_status=INSTAVEL

No BR, o motor nao usa `spc_status=INSTAVEL` isolado como gate de entrada. O gate operacional esta em `build_spc_bc_blocked_set(...)` e retorna `blocked_bc`, que agrega Regra 1 e regras Nelson/WE B+C nas cartas I/MR/Xbar/R.
Logo, `blocked_bc` e semanticamente mais amplo que apenas `INSTAVEL` do classificador simples por limites.

## Q4 — Existe threshold numerico de log_ret no BR?

Nao foi identificado threshold numerico de `log_ret` no motor BR para habilitar gate. A decisao BR (D-088/D-090) referencia criterio pre-registrado em ablacoes (T-088), nao um corte numerico unico de retorno.

## Tabela comparativa BR × US

| Fabrica | n_ESTAVEL | n_ATENCAO | n_INSTAVEL | log_ret_3 ESTAVEL | log_ret_3 ATENCAO | log_ret_3 INSTAVEL | INSTAVEL-ESTAVEL | share(became_instavel_1==1 \| INSTAVEL) |
|---------|-----------|-----------|------------|-------------------|-------------------|--------------------|------------------|-------------------------------------------|
| BR | 7374 | 203 | 613 | -0.13% | -0.47% | -1.46% | -1.33 p.p. | +100.00% |
| US | 14872 | 443 | 1425 | +0.07% | -0.63% | -0.94% | -1.01 p.p. | +100.00% |

## Pontos de decisao para o Owner

1. Escopo de gate para Fase B no US: `INSTAVEL` puro ou `blocked_bc` BR-like?
2. Incluir `ATENCAO` no gate US ou manter escopo BR-equivalente?
3. Criterio de desbloqueio: adotar ablacoes BR-like (sem threshold unico) ou criar threshold proprio formalizado?
