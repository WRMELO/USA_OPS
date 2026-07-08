# Pre-registro - T-SDC-EXIT-CONE-REBALANCE-US-V4

- Task: `T-SDC-EXIT-CONE-REBALANCE-US-V4`
- Decision Ref: `PENDING-DECISION-LOG`
- Data: 2026-07-08
- Natureza: estudo consultivo/read-only, sem promocao automatica a motor e sem decisao de venda real.

## Hipotese

O V3 (`T-SDC-EXIT-MECHANISMS-REPLAY-US-V3`) terminou `COMPLETED`, com Gate 1/2 `PASS`, baseline reconciliado e todos os bracos `INCONCLUSIVO`. O Owner propos um novo frame: durante a janela inter-rebalance do Forno US, cada posicao deve ser avaliada contra o padrao estatistico que justificou sua permanencia/entrada no ranking daquele ciclo, e nao contra uma banda SPC que se alarga dinamicamente.

O estudo testa se um cone de trajetoria ancorado na referencia estatistica do rebalance reduz devolucoes relevantes sem degradar a economia total do C4.

## Base estatistica congelada

A referencia do cone vem do mesmo dataset congelado usado pelo ranking:

- `mu_reb`: coluna `score_m0` de `scores_m3_us.parquet`.
- `sigma_reb`: coluna `vol_62` de `scores_m3_us.parquet`.
- Data da referencia: `prev_d`, a mesma data cuja pontuacao decide o rebalance executado em `d`.

Essas colunas seguem a formula do ranking M3-US:

- `score_m0 = logret.rolling(window=62, min_periods=62).mean()`
- `vol_62 = logret.rolling(window=62, min_periods=62).std(ddof=0)`

## Formula do sinal

A cada dia de rebalance `d`, definido por `i % rebalance_cadence == 0`, para cada ticker que permanece ou entra na carteira apos Camada 2, trim de concentracao e compras C4:

- `reb_anchor_date = d`
- `cum_logret = 0`
- `n_since_reb = 0`
- `mu_reb` e `sigma_reb` ficam congelados a partir de `scores_m3_us.parquet` em `prev_d`

Em cada dia subsequente com o ticker aceso:

- `n_since_reb += 1`
- `cum_logret += log(close_operational_d / close_operational_d-1)`

O sinal e avaliado no inicio do processamento do dia seguinte usando o estado acumulado ate o fechamento do dia anterior, mantendo a convencao `prev_d` dos mecanismos de Camada 3 do V3.

Para cada braco:

```text
lower_bound(n) = anchor_value * n - k * sigma_reb * sqrt(n)
```

Onde:

- `anchor_value = mu_reb` na familia `cone_mu`.
- `anchor_value = 0.0` na familia `cone_zero`.
- Disparo: `n_since_reb >= 1` e `cum_logret < lower_bound(n_since_reb)`.
- Se `mu_reb` ou `sigma_reb` nao forem finitos, ou se `sigma_reb <= 0`, o cone fica inativo ate o proximo rebalance.

## Grade pre-registrada

Seis bracos, sem selecao post-hoc:

| Braco | Familia | k |
| --- | ---: | ---: |
| `C4_CONE_MU_150` | `cone_mu` | 1.5 |
| `C4_CONE_MU_200` | `cone_mu` | 2.0 |
| `C4_CONE_MU_250` | `cone_mu` | 2.5 |
| `C4_CONE_ZERO_150` | `cone_zero` | 1.5 |
| `C4_CONE_ZERO_200` | `cone_zero` | 2.0 |
| `C4_CONE_ZERO_250` | `cone_zero` | 2.5 |

## Execucao economica

- Venda candidata: 100% do ticker no dia do disparo.
- Friccao: 2.5 bps one-way, igual ao C4/V3.
- Liquidacao: `settlement_days=1`, igual ao C4/V3.
- Quarentena: 10 pregoes, igual ao V3.
- Camadas originais C4 preservadas: Camada 1 defensiva SPC, Camada 2 rebalance, Camada 2.5 trim de concentracao e compras C4.

## Gates pre-analiticos

### Gate 1 - hashes do dataset congelado

Verificar SHA256, contra `research_dataset_us_v2_full_history/manifest.json`, dos arquivos:

- `canonical_us.parquet`
- `macro_us.parquet`
- `blacklist_us.json`
- `scores_m3_us.parquet`

Se falhar, parar como `FAIL_METODOLOGICO` sem rodar bracos candidatos.

### Gate 2 - reconciliacao baseline C4

Reexecutar o baseline C4 sem Camada 3 e reconciliar contra:

- `backtest/canonical_daily_history_us/curve.parquet`

Criterio:

- `max_rel_abs_diff <= 1e-6`

Se falhar, parar como `FAIL_METODOLOGICO` sem rodar bracos candidatos.

## Splits e bootstrap

- Calibracao: ate `2022-12-30`.
- Validacao: a partir de `2023-01-02`.
- Bootstrap: block bootstrap com `block_size=21`, `n_paths=5000`, `seed=42`.

## Veredito R-048

O veredito segue o mesmo criterio do V3:

- `DOMINA_FORTE_MELHOR` / `DOMINA_FORTE_PIOR`: IC95 do delta Sharpe inteiramente de um lado do zero.
- `FAVORECIDO_MELHOR` / `FAVORECIDO_PIOR`: massa bootstrap unilateral >= 90%, sinais consistentes em calibracao/validacao, delta CAGR de validacao na mesma direcao e materialidade `|delta_sharpe_validacao| >= 0.30`.
- `INCONCLUSIVO`: ausencia de direcao clara ou materialidade insuficiente.

## Fora de escopo

- Nenhuma venda de SNDK/VSH ou de qualquer outro ticker sera decidida por esta task.
- Nenhuma regra operacional, skill, painel operacional ou motor blindado sera promovido.
- Espelhamento BR fica fora do escopo ate haver veredito US e validacao semantica R-026.
- Os mecanismos do V3 (`trailing_stop`, `frozen_spc_ratchet`, `band_expansion`) nao sao candidatos do V4; aparecem apenas como contexto historico.
- Nao ha selecao post-hoc de melhor configuracao.
