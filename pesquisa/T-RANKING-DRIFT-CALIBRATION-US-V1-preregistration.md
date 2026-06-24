# T-RANKING-DRIFT-CALIBRATION-US-V1 — Pre-registro

## 1) Pergunta de pesquisa

Entre rebalanceamentos do Forno US, holdings com `m3_rank > top_n` e aquecimento LEVE negativo apresentam degradacao estatisticamente relevante versus holdings comparaveis dentro do Master (`m3_rank <= top_n`)?

## 2) Definicao operacional do sinal (consultivo)

Evento candidato (grupo sinal):

- holding ativo no ciclo;
- `m3_rank > top_n` (`top_n = 20`);
- `aquecimento_pct < 0` e `aquecimento_pct > -12.85%` (faixa LEVE).

Grupo de controle:

- holdings ativos no mesmo conjunto de ciclos;
- `m3_rank <= top_n`;
- mesma faixa LEVE (`-12.85% < aquecimento_pct < 0`).

## 3) Metricas

- **Primaria**: diferenca de media do log-return do dia do sinal ate o proximo rebalance (`grupo_sinal - controle`).
- **Secundarias**:
  - taxa de eventos com retorno negativo ate o proximo rebalance;
  - mediana de retorno por grupo;
  - diferenca de medias por split (calibracao vs validacao).

## 4) Split temporal

- Calibracao: primeiros 60% dos ciclos elegiveis.
- Validacao: ultimos 40% dos ciclos elegiveis.

## 5) Criterios de veredito (pre-definidos)

- **SIGNAL_UTIL**: diferenca media > 0.5 p.p. em modulo no holdout de validacao e IC95 bootstrap sem cruzar zero.
- **SIGNAL_FRACO**: diferenca significativa (IC95 nao cruza zero), mas <= 0.5 p.p.
- **INCONCLUSIVO**: IC95 cruza zero **ou** `n_sinal < 30`.

## 6) Dados de entrada (pre-declarados)

- `data/ssot/operational_window.parquet`
- `data/features/scores_m3_us.parquet`
- `data/daily/decision_*.json`
- `data/real/*.json`
- `config/winner_us.json`

## 7) Integridade de dataset (R-041)

SHA256 dos parquets (registrar antes de leitura):

- `data/ssot/operational_window.parquet`: `6c97f426b221e04785096118c084dd8cc3d2b8c37e11f0c9ea0301c0aeecadec`
- `data/features/scores_m3_us.parquet`: `6e2f5538860b321214bb00c7c6449a7452c2d7a3625a45c71b573d79bdb2fe57`

Observacao: nenhum parquet foi aberto antes deste pre-registro.

## 8) Restricoes

- Estudo consultivo. Nao cria regra automatica.
- Nao altera skill, motor ou painel nesta task.
- Se resultado for SIGNAL_UTIL, proposta de R-NNN sera apenas sugestao para ciclo formal separado.
