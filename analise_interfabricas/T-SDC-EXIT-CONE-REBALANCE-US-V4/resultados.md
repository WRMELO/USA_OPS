# Relatorio - T-SDC-EXIT-CONE-REBALANCE-US-V4

- Decision Ref: `PENDING-DECISION-LOG`
- Status: `COMPLETED`
- Executado em UTC: `2026-07-08T11:38:09.828940+00:00`

## Declaracoes de Escopo

- Nenhuma venda de SNDK/VSH foi decidida por esta task.
- Nenhuma regra operacional, skill ou motor blindado foi promovido.
- Espelhamento BR permanece fora do escopo ate haver veredito US e validacao semantica R-026.
- Nao ha selecao post-hoc de melhor configuracao; se os gates pre-analiticos falham, os bracos candidatos nao sao executados.

## Gates

- Gate 1 hashes: `PASS`
- Gate 2 baseline: `PASS`, max_rel_abs_diff=`0.0`

## Comparacoes

| Braco | Familia | k | Veredito | Delta Sharpe val | Delta CAGR val | Triggers val |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| C4_CONE_MU_150 | cone_mu | 1.5 | INCONCLUSIVO | 0.180241 | 0.062749 | 579 |
| C4_CONE_MU_200 | cone_mu | 2.0 | INCONCLUSIVO | -0.047577 | -0.035336 | 248 |
| C4_CONE_MU_250 | cone_mu | 2.5 | INCONCLUSIVO | -0.024223 | -0.020946 | 116 |
| C4_CONE_ZERO_150 | cone_zero | 1.5 | INCONCLUSIVO | 0.042165 | 0.000314 | 258 |
| C4_CONE_ZERO_200 | cone_zero | 2.0 | INCONCLUSIVO | 0.021174 | -0.002100 | 114 |
| C4_CONE_ZERO_250 | cone_zero | 2.5 | INCONCLUSIVO | 0.077963 | 0.032178 | 53 |

## Nota Interpretativa

Este relatorio e consultivo/read-only. Qualquer promocao futura exige nova decisao do Owner e nova task formal.
