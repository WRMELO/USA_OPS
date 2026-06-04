# Pre-registro T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1

Data: 2026-06-04

IMPORTANTE: este documento deve ser escrito e seus timestamps devem ser anteriores à execução do script. Alterar thresholds após ver resultados invalida o estudo.

ENQUADRAMENTO: calibração de SAÍDA (não entrada). Comparar gatilho candidato (forma da curva) contra baseline R-035 (nível acumulado).

UNIVERSO: todos os (ticker, entry_date, exit_date) onde ticker estava no Top-20 (m3_rank <= 20) por ao menos 5 pregões consecutivos.

Entry_price = close_operational no entry_date.

Exit_price_baseline = close_operational no primeiro dia em que (close/entry_price - 1) <= -0.1285 (R-035 MEDIO), ou close no last_date se nunca atingido.

SPLIT TEMPORAL:
- Calibração: 2024-08-28 a 2025-07-31.
- Validação: 2025-08-01 a 2026-06-03.

GRADE PRÉ-REGISTRADA (não alterar após ver resultados):
- drawdown_thresholds = [-0.10, -0.125, -0.15, -0.175, -0.20]
- slope_thresholds = [-0.08, -0.10, -0.12] (slope 5 pregões de close_operational)

Cada configuração é DD_{dd}_SL_{sl} onde dd e sl são os valores absolutos.

MÉTRICAS POR CONFIGURAÇÃO:
- melhoria_preco = mean(exit_price_candidato / exit_price_baseline) nos holdings onde candidato dispara ANTES do baseline — valores > 1.0 indicam saída melhor;
- falso_positivo_pct = proporção de holdings onde candidato dispara mas baseline NUNCA dispara (queimador se recuperou);
- cobertura = proporção de holdings onde candidato dispara antes ou junto com baseline.

CRITÉRIOS DE VEREDITO por configuração:
- PASS = melhoria_preco > 1.03 E falso_positivo_pct < 0.30 em calibração E confirmado em validação.
- INCONCLUSIVO = melhoria positiva mas não confirmada em validação, ou melhoria < 1.03.
- FAIL = melhoria_preco <= 1.00 ou falso_positivo_pct >= 0.50.

SANITY-CHECK SATL:
Para SATL (entry 2026-05-18, custo $9.7982), verificar quais configurações teriam disparado em 01/06 ($8.67) vs baseline R-035 em 03/06 ($7.84).
