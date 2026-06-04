# Pre-registro T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2

Data: 2026-06-04

IMPORTANTE: este documento deve ser escrito e seus timestamps devem ser anteriores a execucao do script. Alterar thresholds apos ver resultados invalida o estudo.

ENQUADRAMENTO: calibracao de SAIDA condicional. Comparar gatilho V2 (forma da curva + confirmacao dupla Nelson/WE downside + deterioracao de rank) contra baseline R-035.

HIPOTESE: o filtro de confirmacao reduz o falso positivo da V1 (60-78%) para abaixo de 30%, mantendo melhoria de preco acima de 1.03.

UNIVERSO: identico a V1 - todos os (ticker, entry_date) onde ticker estava no Top-20 (m3_rank <= 20) por ao menos 5 pregoes consecutivos.

SPLIT TEMPORAL:
- Calibracao: 2024-08-28 a 2025-07-31.
- Validacao: 2025-08-01 a 2026-06-03.

GRADE PRE-REGISTRADA (nao alterar apos ver resultados):
- DRAWDOWN_THRESHOLDS = [-0.10, -0.125, -0.15]
- SLOPE_THRESHOLDS = [-0.08, -0.10, -0.12]
- RANK_DELTA_THRESHOLDS = [10, 15] (m3_rank no dia D deve ser >= m3_rank no dia D-5 + rank_delta, calculado dentro do holding).

DEFINICAO DE NELSON/WE DOWNSIDE:
True se qualquer um dos sinais de baixa for True no dia D:
- i_w4_dn (8 pontos consecutivos abaixo da linha central),
- i_w3_dn (4 de 5 pontos abaixo de 1-sigma),
- i_w2_dn (2 de 3 pontos abaixo de 2-sigma),
- i_n3_dn (5 diferencas consecutivas negativas).

Derivado das colunas i_value, i_ucl, i_lcl do operational_window.parquet, usando a logica exata do run_t088c.py linhas 214-239.

METRICAS: identicas a V1 - melhoria_preco, falso_positivo_pct, cobertura.

CRITERIOS DE VEREDITO:
- PASS = melhoria_preco > 1.03 E falso_positivo_pct < 0.30 em calibracao E confirmado em validacao.
- INCONCLUSIVO = melhoria positiva mas nao confirmada ou < 1.03.
- FAIL = melhoria_preco <= 1.00 ou falso_positivo_pct >= 0.50.

SANITY-CHECK SATL:
entry 2026-05-18, custo $9.7982; verificar quais configuracoes disparariam em 01/06 ($8.67) vs baseline R-035 em 03/06 ($7.84).
