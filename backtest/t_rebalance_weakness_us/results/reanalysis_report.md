# Reanalise Fase A — T-REBALANCE-WEAKNESS-US

## Contexto

Reanalise sobre os dados ja gerados (`observations_top20.csv` e `observations_top30.csv`), sem rerun do backtest.

## Bug semantico identificado

- O `compute_verdict` procurou `signal == "INSTAVEL"` nos summaries.
- Nos summaries, `signal` representa `rank_trend` (`SUBINDO/ESTAVEL/CAINDO`) e nao `spc_status`.
- Consequencia: `n_INSTAVEL = NaN` e veredito `INCONCLUSIVO` por construcao.

## Tautologia em `became_instavel_K`

A janela usada para `became_instavel_K` inclui `d_reb`; quando o ticker ja esta `INSTAVEL` em `d_reb`, a metrica tende a 1.0 por construcao.

| spc_status | n_total | mean_became_instavel_1 | mean_became_instavel_3 | share_became_instavel_1_eq_1 | share_became_instavel_3_eq_1 | log_ret_3_mean |
| --- | --- | --- | --- | --- | --- | --- |
| INSTAVEL | 1425 | 1.00 | 1.00 | 1.00 | 1.00 | -0.009415 |
| ATENCAO | 443 | 0.00 | 0.20 | 0.00 | 0.20 | -0.006254 |
| ESTAVEL | 14872 | 0.00 | 0.05 | 0.00 | 0.05 | 0.000677 |

## Matriz honesta HOLDOUT TOPN_ALL L=5 (primaria: Top-20)

| spc_status | signal | n | became_instavel_3_rate | log_ret_3_mean | log_ret_3_std |
| --- | --- | --- | --- | --- | --- |
| INSTAVEL | CAINDO | 201 | 1.000000 | -0.002564 | 0.211463 |
| INSTAVEL | ESTAVEL | 96 | 1.000000 | -0.063667 | 0.296455 |
| INSTAVEL | SUBINDO | 1127 | 1.000000 | -0.005724 | 0.218205 |
| INSTAVEL | N/A | 1 | 1.000000 | -0.338105 | 0.000000 |
| ATENCAO | CAINDO | 117 | 0.170940 | -0.008020 | 0.127452 |
| ATENCAO | ESTAVEL | 36 | 0.222222 | -0.006288 | 0.121349 |
| ATENCAO | SUBINDO | 290 | 0.214533 | -0.005535 | 0.138143 |
| ESTAVEL | CAINDO | 4324 | 0.036848 | -0.000590 | 0.100036 |
| ESTAVEL | ESTAVEL | 2258 | 0.042185 | -0.000845 | 0.118881 |
| ESTAVEL | SUBINDO | 8282 | 0.052192 | 0.001785 | 0.096011 |
| ESTAVEL | N/A | 8 | 0.000000 | -0.031495 | 0.063684 |


### Comparativo Top-30 (mesma janela)

| spc_status | signal | n | became_instavel_3_rate | log_ret_3_mean | log_ret_3_std |
| --- | --- | --- | --- | --- | --- |
| INSTAVEL | CAINDO | 201 | 1.000000 | -0.002564 | 0.211463 |
| INSTAVEL | ESTAVEL | 96 | 1.000000 | -0.063667 | 0.296455 |
| INSTAVEL | SUBINDO | 1127 | 1.000000 | -0.005724 | 0.218205 |
| INSTAVEL | N/A | 1 | 1.000000 | -0.338105 | 0.000000 |
| ATENCAO | CAINDO | 117 | 0.170940 | -0.008020 | 0.127452 |
| ATENCAO | ESTAVEL | 36 | 0.222222 | -0.006288 | 0.121349 |
| ATENCAO | SUBINDO | 290 | 0.214533 | -0.005535 | 0.138143 |
| ESTAVEL | CAINDO | 4324 | 0.036848 | -0.000590 | 0.100036 |
| ESTAVEL | ESTAVEL | 2258 | 0.042185 | -0.000845 | 0.118881 |
| ESTAVEL | SUBINDO | 8282 | 0.052192 | 0.001785 | 0.096011 |
| ESTAVEL | N/A | 8 | 0.000000 | -0.031495 | 0.063684 |

## Aplicacao dos gates pre-registrados (Top-20, HOLDOUT, L=5)

- Gate 1 (`became_instavel_3`): 100.00% vs 4.62% + 5 p.p. => **PASS**
- **FLAG TAUTOLOGICO**: Gate 1 nao deve ser tratado como evidencia primaria.
- Gate 2 (`log_ret_3_mean`): -0.94% vs 0.07% - 1.5 p.p. => **FAIL**
- Diferenca observada INSTAVEL-ESTAVEL em `log_ret_3`: -1.01%

## Subgrupos obrigatorios

### 1) `signal=ESTAVEL x spc_status=INSTAVEL` (Top-20, HOLDOUT)

| lookback_L | n | became_instavel_3_rate | log_ret_3_mean | log_ret_3_std |
| --- | --- | --- | --- | --- |
| 1.00 | 247.00 | 1.000000 | -0.039821 | 0.259879 |
| 2.00 | 170.00 | 1.000000 | -0.036352 | 0.281256 |
| 3.00 | 129.00 | 1.000000 | -0.032202 | 0.270678 |
| 5.00 | 96.00 | 1.000000 | -0.063667 | 0.296455 |
| 10.00 | 57.00 | 1.000000 | -0.042306 | 0.286696 |


### 2) `spc_status=ATENCAO` (Top-20, HOLDOUT, L=5)

| signal | n | became_instavel_3_rate | log_ret_3_mean | log_ret_3_std |
| --- | --- | --- | --- | --- |
| CAINDO | 117 | 0.170940 | -0.008020 | 0.127452 |
| ESTAVEL | 36 | 0.222222 | -0.006288 | 0.121349 |
| SUBINDO | 290 | 0.214533 | -0.005535 | 0.138143 |


### 3) `sample_group=IGNITION_TRUE x spc_status=INSTAVEL` (Top-30, HOLDOUT)

| lookback_L | n | became_instavel_3_rate | log_ret_3_mean | log_ret_3_std |
| --- | --- | --- | --- | --- |
| 1.00 | 821.00 | 1.000000 | -0.010022 | 0.221854 |
| 2.00 | 821.00 | 1.000000 | -0.010022 | 0.221854 |
| 3.00 | 821.00 | 1.000000 | -0.010022 | 0.221854 |
| 5.00 | 821.00 | 1.000000 | -0.010022 | 0.221854 |
| 10.00 | 821.00 | 1.000000 | -0.010022 | 0.221854 |


## Ponto de Decisao do Owner

O sinal observado justifica executar a **Fase B** (patch + rerun + auditoria dupla) ou a frente deve ser **arquivada**?
