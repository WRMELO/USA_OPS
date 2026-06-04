# Pre-registro T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1

- Data: 2026-06-04
- Autores: Architect + Executor

IMPORTANTE: este documento deve ser escrito e versionado antes de rodar qualquer execucao da calibracao. Alterar thresholds apos ver resultados invalida o estudo.

## Escopo

- Universo: todos os tickers presentes no Top-20 (`m3_rank <= 20`) do `scores_m3_us.parquet` em qualquer pregao do periodo.
- Janela de dados: `operational_window.parquet` + `scores_m3_us.parquet`.
- Horizonte de avaliacao: 5 pregoes a frente (`forward_5d_ret`).

## Split temporal pre-registrado

- Calibracao: 2024-08-28 a 2025-07-31
- Validacao: 2025-08-01 a 2026-06-03

## Metricas primarias

Para cada hipotese, comparar:

- `mean(forward_5d_ret | sinal=True)` vs `mean(forward_5d_ret | sinal=False)`
- Bootstrap percentile IC95 (n=2000, seed=42)
- p-valor unilateral (hipotese direcional: sinal precede queda)

## Criterios de veredito

- PASS: media_sinal < media_controle E p < 0.10 E media_sinal negativa na validacao
- INCONCLUSIVO: direcao correta mas p >= 0.10, ou calibracao PASS sem confirmacao na validacao
- FAIL: media_sinal >= media_controle

## Hipoteses pre-registradas (thresholds fixos)

### H1 - Exaustao parabolica sem persistencia

- Sinal: `ret_62 >= 0.80` (log) E amplitude LARGA (`i_ucl - i_lcl` acima do percentil 66.7 do universo no dia)
- Thresholds fixos: `ret_62 = 0.80`, amplitude = tercil superior

### H2 - Run downside na carta-I

- Sinal: 6 ou mais pontos consecutivos com `i_value < center_line`
- `center_line = (i_ucl + i_lcl) / 2`
- Threshold fixo: `run >= 6`

### H3 - Drawdown desde pico local

- Sinal: `(close_operational / rolling_max_10 - 1) < -0.10`
- Threshold fixo: `drawdown < -10%` em janela de 10 pregoes

### H4 - Amplitude LARGA + qualquer negativo desde ignicao

- Sinal: amplitude LARGA E `ret_desde_ignicao < 0`
- Thresholds fixos: amplitude = tercil superior, retorno negativo estrito (sem margem)

## Regras de integridade

- Nenhum threshold pode ser ajustado pos-hoc.
- O relatorio final deve citar este pre-registro e confirmar explicitamente que os parametros nao foram alterados.
