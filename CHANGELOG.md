# CHANGELOG — USA_OPS

## 2026-07-23

- docs(backtest): T-SDC-R001-R037-ENTRY-VETO-US-V1 — fecha a rastreabilidade do estudo read-only de veto de entrada no Top-20 por R-001 restrito a downside e R-037 severo sobre baseline C4+R-060, com phase sweep e tiers independentes; vereditos `R001_DOWNSIDE=INCONCLUSIVO` e `R037_SEVERE=FAVORECIDO_R037_SEVERE`. Artefatos: `backtest/t_r001_r037_entry_veto_us_v1/`, `DECISION_LOG.md`, `ROADMAP.md`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/CHANGELOG.md`. Decision: SALA D-139 / USA D-156.
- docs(curadoria): T-SDC-DEFENSIVE-HOLD-CASH-DRIFT-LADDER-US-V2 — fecha a rastreabilidade do estudo read-only de caixa parado ate o proximo rebalanceamento; V1 preservada como evidencia do aborto correto do gate G6, V2 corrige apenas a janela de medicao do G6 e conclui com PASS do Auditor, G1-G7 PASS e vereditos `INCONCLUSIVO` em todos os pares. Artefatos: `backtest/t_defensive_hold_cash_drift_ladder_us_v1/`, `backtest/t_defensive_hold_cash_drift_ladder_us_v2/`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/CHANGELOG.md`, `../SALA_DE_CONTROLE/ROADMAP.md`. Decision: SALA D-137 / USA D-155.

## 2026-07-22

- docs(curadoria): T-SDC-DEFENSIVE-REINVEST-POLICY-US-V2 — errata de D-135/D-153 (V1 invalida por ancora LIVE + deadlock A2), reexecucao pre-registrada com harness corrigido (phase sweep, pool defensivo, venda exata, gates G1-G6) e regra R-061. Vereditos V2: `Zero-A_vs_Zero-B=INCONCLUSIVO`, `Zero-A_vs_A1=INCONCLUSIVO`, `Zero-A_vs_A2=INCONCLUSIVO`, `A1_vs_A2=INCONCLUSIVO`. Artefatos: `DECISION_LOG.md`, `GOVERNANCE.md`, `ROADMAP.md`, `backtest/t_defensive_reinvest_policy_us_v2/`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/REGRAS_OPERACIONAIS.md`, `../SALA_DE_CONTROLE/CHANGELOG.md`. Decision: SALA D-136 / USA D-154.
- docs(curadoria): T-SDC-DEFENSIVE-REINVEST-POLICY-US-V1 — fecha a rastreabilidade do estudo read-only pre-registrado de 4 braços para politica de venda defensiva/reinvestimento com liquidacao D0, com vereditos `INCONCLUSIVO` em todos os pares e artefatos em `backtest/t_defensive_reinvest_policy_us_v1/`. Artefatos: `DECISION_LOG.md`, `backtest/t_defensive_reinvest_policy_us_v1/`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/CHANGELOG.md`. Decision: D-153 (ref cruzada: SALA D-135).
- feat(web+ledger): T-SDC-BOLETIM-DFC-LIQUIDACAO-FLAG-US-V1 — adiciona flag obrigatória de liquidação por venda no `/painel` (`JA_NO_CAIXA`/`EM_LIQUIDACAO`), persiste `SETTLEMENT` same-day referenciado ao `SELL` quando aplicável, ajusta `pipeline/ledger.py::compute_cash` para reconciliar `cash_accounting` por `SELL - settled_by_ref` mesmo com `settle_date` futuro, remodela as seções 07/08 para Balancete+DFC simplificados (Caixa Livre Real BTG, corretagem dia/acumulada, delta ajustado por liquidação) e inclui script de remediação append-only do caso PENG 21/07 (`scripts/migrate_ledger_20260721_peng_ja_no_caixa.py`) com testes dedicados. Decision: D-150 (ref cruzada: SALA D-132).

## 2026-07-21

- feat(motor-us): T-SDC-MOTOR-BANDEXP-RET62-VETO-US-V1 — promove o gate `Flag_BandExp ∩ ret_62>=1.00` a veto operacional efetivo no Top-20 US (remoção + substituição pelo próximo elegível), adiciona o módulo `lib/band_exp_gate.py`, integra o veto em `pipeline/09_decide.py` (blindado) com `ranking_schema_version=3`, registra ativação em `config/winner_us.json` (blindado) e compatibiliza exibição consultiva em `pipeline/analise_us.py`. Inclui testes `tests/test_band_exp_gate.py`, `tests/test_09_decide_bandexp_ret62_veto.py` e paridade formal em `backtest/t_motor_bandexp_ret62_veto_us_v1/verify_operational_parity_v1.py`. Decision: SALA D-131 (ref cruzada: USA D-149).
- docs(backtest): T-SDC-BANDEXP-RET62-ENTRY-US-V1 — executa estudo read-only pre-registrado para tier dedicado do filtro amplo `Flag_BandExp ∩ ret_62>=1.00` no Top-20 US sobre `backtest/research_dataset_us_v2`, com decomposicao de merito (`Arm_BandExp_Isolado` vs `Arm_Ret62_Isolado`) e veredito `DOMINA_FORTE` no par `Baseline_C4puro` vs `Arm_BandExpRet62`. Artefatos: `backtest/t_bandexp_ret62_entry_us_v1/`, `../SALA_DE_CONTROLE/analise_interfabricas/relatorio_bandexp_ret62_entry_us_v1.md`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/CHANGELOG.md`. Decision: SALA D-130.
- docs(backtest): T-SDC-BANDEXP-R037-MATERIALITY-ENTRY-US-V1 — executa estudo read-only para medir a materialidade do veto encavalado `Flag_BandExp ∩ R-037_recon` no Top-20 US sobre `backtest/research_dataset_us_v2`, com braco de sensibilidade `ret_62>=1.00` puro, diagnostico de redundancia (`R-001` vs `ret_62`) e profundidade de substituicao por `m3_rank`. Artefatos: `backtest/t_bandexp_r037_materiality_entry_us_v1/`, `../SALA_DE_CONTROLE/analise_interfabricas/relatorio_bandexp_r037_materiality_entry_us_v1.md`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/CHANGELOG.md`. Decision: SALA D-129.
- docs(backtest): T-SDC-BAND-EXPANSION-ENTRY-VETO-US-V1 — fecha a curadoria do estudo read-only que validou expansao sistematica/monotonica da banda SPC como sinal consultivo de entrada no Forno US, com replay confirmatorio em `backtest/research_dataset_us_v2`, event-study exploratorio em SSOT vivo e registro de D-128. Artefatos: `backtest/t_band_exp_entry_us_v1/`, `DECISION_LOG.md`, `../SALA_DE_CONTROLE/DECISION_LOG.md`, `../SALA_DE_CONTROLE/analise_interfabricas/event_study_band_exp_entry_us_v1.md`, `../SALA_DE_CONTROLE/analise_interfabricas/relatorio_band_exp_entry_us_v1.md`. Decision: D-128 (ref cruzada: SALA D-128).
- feat(ledger-real): T-USA-RECON-QTYPRICE-NOVACAO-V1 — corrige os scanners `scripts/reconcile_broker_note.py` e `scripts/reconcile_and_apply.py` para considerar apenas eventos ativos (R-038), introduz `EventType.RECON_ADJUST` (`amount=0`) no `pipeline/ledger.py`, restringe auto-aplicação a ajuste de quantidade/preço sem impacto de caixa, e aplica novação append-only da nota `Confirm_BTGP_001_BPXB000057_07172026.pdf` (16 tickers) com relink de `FEE` para BUY restaurado via `scripts/migrate_ledger_20260721_novacao_recon_qtyprice.py`. Inclui cobertura em `tests/test_ledger.py`, `tests/test_reconcile_broker_note.py` e `tests/test_migrate_ledger_20260721_novacao_recon_qtyprice.py`, além de ajustes de contrato em `GOVERNANCE.md`, `.cursor/skills/reconciliador-btg/SKILL.md` e `.cursor/skills/analista-usa/SKILL.md`. Decision: D-147 (ref cruzada: SALA D-125).
- docs(analista-usa): T-SDC-ANALISTA-USA-DELEGAR-RECONCILIACAO-BTG-V1 — o Passo 0c da skill `analista-usa` deixa de executar conferencia via CLI e passa a ler `data/live_real_test/reconciliation_checkpoint.json` + `data/live_real_test/reconciliation_log.jsonl`, bloqueando apenas pendencia material (`|cash_delta| >= US$ 1,00`) sem alterar o motor nem o `reconciliador-btg`. Artefatos: `../.cursor/skills/analista-usa/SKILL.md`, `GOVERNANCE.md`, `DECISION_LOG.md`. Decision: D-146 (ref cruzada: SALA D-124; supersede parcialmente D-140).

## 2026-07-20

- feat(web): T-SDC-PAINEL-PREVIEW-PARIDADE-COMPLETA-US-V1 — `pipeline/real_boletim_web.py` passa a renderizar o `/painel` LIVE-REAL-TEST com paridade completa ao preview (8 secoes numeradas 01-08, identidade visual azul, bloco Acao do dia com defensivas calculadas do contexto real e bloco local de Operacoes sugeridas pelo Analista sem POST), preservando os formularios oficiais de rascunho/encerramento e mantendo o contrato sem `QTY_FIXES` e sem CDN. Artefatos: `pipeline/real_boletim_web.py`, `DECISION_LOG.md`, `GOVERNANCE.md`. Decision: D-144 (ref cruzada: SALA D-121).

## 2026-07-18

- docs(curadoria): T-SDC-PAINEL-BASE1-FRICCAO-SPARKLINE-US-V1 — fecha a integracao do preview no `/painel` LIVE-REAL-TEST com Base 1 por cotizacao plena, sparklines 62d, ponte de friccao Balanço -> Real -> NAV e SVG local sem CDN, removendo overlays `QTY_FIXES` do caminho produtivo. Artefatos: `pipeline/ledger.py`, `pipeline/real_boletim_web.py`, `tests/test_ledger.py`, `tests/test_real_boletim_web.py`, `DECISION_LOG.md`, `GOVERNANCE.md`. Decision: D-143 (ref cruzada: SALA D-119).

## 2026-07-17

- feat(analista-usa): T-SDC-ANALISTA-CONFERENCIA-NOTA-GATE-US-V1 — adiciona `discover_notes`/`propose_dir` e subcomando `propose-dir` em `scripts/reconcile_broker_note.py` (varredura de `Confirm_*.pdf` por pasta, ignorando ruido, com `divergent_items`/`parse_errors`/`has_blocking_divergence`), amplia `tests/test_reconcile_broker_note.py` e inclui Passo 0c no `.cursor/skills/analista-usa/SKILL.md` para bloquear o diagnostico quando houver divergencia pendente entre nota oficial e ledger real no regime LIVE-REAL-TEST. Decision: D-140 (ref cruzada: SALA D-116).
- feat(ledger-real): T-SDC-LEDGER-FRACIONARIO-CONFERENCIA-NOTA-US-V1 — migra `qtd` para fracionario no caminho real em `pipeline/ledger.py` (epsilon-safe em duplicidade/FIFO/snapshot/livro por ativo), estende `pipeline/real_boletim_web.py` + `pipeline/servidor.py` para registrar operacao por `valor_investido` (derivando `qtd`), cria `scripts/reconcile_broker_note.py` (propose/resolve com log append-only de reconciliacao) e `scripts/migrate_ledger_20260716_fractional_fix.py` (migracao supervisionada com backup, dry-run default e `--confirm` explicito), adiciona `pdfplumber==0.11.10` em `requirements.txt` e amplia cobertura em `tests/test_ledger.py`, `tests/test_real_boletim_web.py`, `tests/test_reconcile_broker_note.py` e `tests/test_migrate_ledger_20260716_fractional_fix.py`. Decision: D-139 (ref cruzada: SALA D-115).
- feat(boletim): T-SDC-BOLETIM-REAL-CONTEXTO-LIVRO-US-V1 — adiciona `refresh_contexto_analista_us` no Step 14 de `pipeline/run_daily.py` (refresh automatico do `data/ssot/contexto_analista_us.json` por `market_day` elegivel, nao bloqueante), inclui `build_operations_book` (FIFO) em `pipeline/ledger.py`, estende `pipeline/real_boletim_web.py` com `close_stale_drafts` (autoencerramento de `draft_<exec_day>.json` anterior ao dia atual) e nova secao "Livro de operacoes por ativo" no `/painel`, e integra o rollover automatico em `pipeline/servidor.py` antes da renderizacao live; adiciona cobertura em `tests/test_run_daily_contexto_refresh.py`, `tests/test_ledger.py` e `tests/test_real_boletim_web.py`. Decision: D-138 (ref cruzada: SALA D-114).

## 2026-07-16

- feat(autosave): T-SDC-DRYRUN-AUTOSAVE-F18-US-V1 — mecaniza o autosave do dry-run US: `pipeline/painel_diario.py` ganha `_build_rebalance_buy_suggestions` e `compute_dryrun_autosave_operations` (sell+buy de rebalance por `target_weight`, ordem de `operational_ranking` e caixa disponivel); `pipeline/servidor.py` extrai `apply_boletim_operations(payload)` para reuso interno mantendo a regra "somente hoje" no endpoint `/salvar`; novo `pipeline/dryrun_autosave.py` executa catch-up idempotente por `market_day` com trilha append-only em `data/daily/autosave_log.jsonl`; `pipeline/run_daily.py` integra Step 13 de autosave (nao bloqueante); adiciona testes `tests/test_painel_diario_autosave.py` e `tests/test_dryrun_autosave.py` e expande `tests/test_servidor_real_boletim.py`. Decision: D-137 (ref cruzada: SALA D-112).
- feat(web): T-SDC-LIVE-REAL-TEST-WEB-BOLETIM-LEDGER-US-V1 — adiciona `EventType.FEE` ao ledger (`pipeline/ledger.py`), cria o módulo `pipeline/real_boletim_web.py` (rascunho persistente + aplicação no ledger + fechamento definitivo + render HTML), integra novas rotas POST em `pipeline/servidor.py` (`/painel/rascunho`, `/painel/rascunho/remover`, `/painel/encerrar`) e troca o `/painel` de leitura por visão live real-aware; adiciona `record-fee`/`build_boletim_payload` em `scripts/live_real_cutover.py` e `build_friction_report_payload` em `scripts/friction_ruler.py`; inclui suíte `tests/test_real_boletim_web.py` e ajusta `tests/test_servidor_real_boletim.py`; registra corretagem retroativa de 16/07 em `data/live_real_test/ledger_real.jsonl` (MRVI/HPP) e regenera `data/live_real_test/2026-07-16.json` + `friction_report_2026-07-16.json` (`cash_free=18018.34`). Decision: D-136 (ref cruzada: SALA D-111).
- docs(backtest): T-SDC-M3-REDUNDANCY-REWEIGHT-US-V1 — cria estudo-filho read-only em `backtest/t_m3_redundancy_reweight_us_v1/` com novo freeze `research_dataset_us_v2` (`freeze_asof=2026-07-15`), diagnostico de redundancia `z_m0` vs `z_ret` (v2), arms `Arm_Dedup1x1` e `Arm_DedupVolTilt` vs `Baseline_C4`, diagnostico late_rocket e `case_trace_repl_smwb_us_v1.csv`; vereditos finais `INCONCLUSIVO` para os dois arms e para late_rocket; sem alteracao de motor/blindados. Decision: D-135 (ref cruzada: SALA D-110).
- docs(backtest): T-SDC-POSWINNER-SELECTION-AUDIT-US-V1 — cria estudo read-only pre-registrado em `backtest/t_poswinner_selection_audit_us_v1/` (`decision_criterion_poswinner_selection_audit_us_v1.json`, `diagnostic_m3_redundancy_us_v1.py`, `run_poswinner_selection_audit_us_v1.py`, `report_poswinner_selection_audit_us_v1.md`) e gera artefatos em `results/` (`diagnostic_z_m0_z_ret_correlation.json`, `observations_poswinner_us_v1.csv`, `observations_ticker_late_rocket_us_v1.csv`, `bootstrap_diagnostics_us_v1.json`, `verdict_poswinner_us_v1.json`) com dataset congelado (`freeze_asof=2026-06-09`), sem alteracao de motor. Decision: D-134 (ref cruzada: SALA D-109).
- fix(analista-usa): T-SDC-ANALISTA-REAL-TEST-RANKING-FIX-US-V1 — `pipeline/analise_us.py` passa a detectar automaticamente o regime LIVE-REAL-TEST e ler caixa/posicoes do `data/live_real_test/ledger_real.jsonl` (em vez de `data/real/`) quando ativo; `scripts/live_real_cutover.py` corrige o `emit-abertura` para ordenar Top-20 por `m3_rank` (com fallback para `rank`); `pipeline/servidor.py` exibe coluna `M3 Rank` no `/painel`; adiciona testes `tests/test_analise_us.py` e amplia cobertura de `test_live_real_cutover.py`/`test_servidor_real_boletim.py`. Decision: D-133 (ref cruzada: SALA D-108).
- feat(scripts): T-SDC-LIVE-REAL-CUTOVER-ABERTURA-US-V1 — executa `init-cutover --confirm` em produção (APORTE C0 = US$ 20.008,72, `data/live_real_test/ledger_real.jsonl`); adiciona subcomando `emit-abertura` em `scripts/live_real_cutover.py` (boletim real de abertura: Top-20 operacional + caixa + carteira zerada, gravado em `data/live_real_test/abertura_<exec_day>.json`); `pipeline/servidor.py` (não blindado) passa a rotear `/painel` para esse boletim quando o regime LIVE-REAL-TEST estiver ativo, preservando `/painel/<data>` histórico do dry-run; porta 8788 reiniciada via `iniciar.sh`. Autosave do dry-run desmembrado para F-18 (SALA), pendente de decisão do Owner. Decision: D-132 (ref cruzada: SALA D-107).
- feat(scripts): T-SDC-LIVE-REAL-TEST-DAILY-LAUNCHERS-US-V1 — cria `scripts/lookup_shadow_price.py`, `scripts/registrar_ordem_real.sh` e `scripts/encerrar_dia_real.sh` com testes sintéticos, reutilizando `scripts/live_real_cutover.py`/`scripts/friction_ruler.py` (F-16/F-17); adiciona atalhos de desktop `USA_REGISTRAR_ORDEM` e `USA_ENCERRAR_DIA`; não executa corte real nem grava `data/live_real_test/` em produção. Decision: D-131 (ref cruzada: SALA D-106).
- docs(governanca): T-SDC-LIVE-REAL-TEST-PREREGISTRATION-F13-US-V1 — congela pre-registro de vigilancia F-13 em `analise_interfabricas/T-SDC-LIVE-REAL-TEST-PREREGISTRATION-F13-US-V1/preregistro.md` + `manifest.json` (SHA256), sem abrir `data/live_real_test/ledger_real.jsonl`, e registra o sinal explicito do Owner para o corte real do dia-D com os parametros confirmados (C0, ancora sombra e `exec_date` do APORTE). Decision: D-130 (ref cruzada: SALA D-105).

## 2026-07-15

- feat(scripts): T-SDC-FRICTION-RULER-F17-US-V1 — cria `scripts/friction_ruler.py` (`record-shadow-buy`, `emit-friction-report`) e `tests/test_friction_ruler.py` para autoria/teste da F-17 (régua de fricção Opção 1 dry-run paralelo + Opção 2 gêmeo sombra) com dados sintéticos; não executa corte real nem grava `data/live_real_test/` em produção. Decision: D-129 (ref cruzada: SALA D-104).
- feat(scripts): T-SDC-LIVE-REAL-CUTOVER-RUNBOOK-US-V1 — cria `scripts/live_real_cutover.py` (subcomandos `init-cutover`, `freeze-dryrun`, `record-buy`, `emit-boletim`) e `tests/test_live_real_cutover.py` para autoria/teste da F-16 com dados sintéticos e guard de idempotência; não executa corte real nem grava `data/live_real_test/` em produção. Decision: D-128 (ref cruzada: SALA D-103).

## 2026-07-14

- fix(analista-usa): T-SDC-ANALISE-US-CICLOS-ACESO-FIX-V1 — adiciona `ciclos_aceso` aos holdings do contexto canônico em `pipeline/analise_us.py` com semântica alinhada ao BR (pregões desde `purchase_date` até `market_day`), preservando escopo estrito sem ajuste de liquidez. Artefatos: `pipeline/analise_us.py`, `data/ssot/contexto_analista_us.json`, `DECISION_LOG.md`, `CHANGELOG.md`. Decision: D-127 (ref cruzada: SALA D-101).

## 2026-07-09

- feat(motor): T-PAINEL-SUBTOTAL-TICKER-US-V1 — adiciona linha de subtotal agregado por ticker (Layout B) acima dos lotes individuais nas tabelas `Carteira Comprada` e `Carteira Atual (D-1)` do painel US, preservando Total Geral, linhas de lote e logica operacional. Decision: D-126. Tag prevista: `v1.18.0-motor-us`.

## 2026-07-08

- feat(motor): T-SDC-RANKING-UNIFICATION-PHASE2-BRUS-V1 — unifica a fonte operacional de ranking US em `pipeline/09_decide.py`, `pipeline/painel_diario.py` e `pipeline/analise_us.py`, persistindo `operational_ranking` e mantendo `top20_by_score` apenas como dado tecnico legado. Artefatos: `pipeline/09_decide.py`, `pipeline/painel_diario.py`, `pipeline/analise_us.py`, `DECISION_LOG.md`, `REGRAS_OPERACIONAIS.md`. Decision: D-095. Tag prevista: `v1.17.0-motor-us`.
- docs(curadoria): T-SDC-EXIT-CONE-REBALANCE-US-V4 - fecha a rastreabilidade do replay read-only do cone de trajetria inter-rebalance no Forno US com Gate 1/2 reconciliados, 6 comparacoes executadas e relatorio final `INCONCLUSIVO` em todos os bracos. Artefatos: `analise_interfabricas/T-SDC-EXIT-CONE-REBALANCE-US-V4/preregistro.md`, `analise_interfabricas/T-SDC-EXIT-CONE-REBALANCE-US-V4/run_exit_cone_rebalance_v4.py`, `analise_interfabricas/T-SDC-EXIT-CONE-REBALANCE-US-V4/resultados_raw.json`, `analise_interfabricas/T-SDC-EXIT-CONE-REBALANCE-US-V4/resultados.md`, `analise_interfabricas/T-SDC-EXIT-CONE-REBALANCE-US-V4/output_bruto.txt`. Decision: D-092.
- docs(curadoria): T-SDC-EXIT-MECHANISMS-REPLAY-US-V3 - fecha a rastreabilidade do replay read-only dos mecanismos de saida no Forno US com Gate 1/2 reconciliados, 10 comparacoes executadas e relatorio final `INCONCLUSIVO` em todos os bracos. Artefatos: `analise_interfabricas/T-SDC-EXIT-MECHANISMS-REPLAY-US-V3/preregistro.md`, `analise_interfabricas/T-SDC-EXIT-MECHANISMS-REPLAY-US-V3/run_exit_mechanisms_replay_v3.py`, `analise_interfabricas/T-SDC-EXIT-MECHANISMS-REPLAY-US-V3/resultados_raw.json`, `analise_interfabricas/T-SDC-EXIT-MECHANISMS-REPLAY-US-V3/resultados.md`, `analise_interfabricas/T-SDC-EXIT-MECHANISMS-REPLAY-US-V3/output_bruto.txt`. Decision: D-091.

## 2026-07-03

- fix(motor): T-COTIZACAO-BASE1-US-BR-V1 — Base 1 passa a usar cotizacao plena no `pipeline/painel_diario.py`: aportes/retiradas externos alteram quantidade de cotas, proventos permanecem como retorno interno/caixa e a rentabilidade acumulada do painel passa a seguir o preco da cota. Decision: D-125 / SALA D-084 / USA D-124. Tag prevista: `v1.16.0-motor-us`.

## 2026-07-02

- docs(curadoria): T-SDC-USA-LIVE-REAL-TEST-PREP-V2 - espelha D-123/D-124, registra o regime `LIVE-REAL-TEST` e a cotizacao plena da Base 1 para a passagem ao real US. Artefatos: `DECISION_LOG.md`. Decision: D-123 / D-124.
- fix(T-USA-RESEED-WINNER-C4-V1): reseed de `data/daily/winner_curve_us.parquet` com base C4 canonica e re-selagem do hash de `backtest/results/curve_C4_K10.csv` em `config/winner_us.json`. Mantem 70 linhas `pipeline_step10` intactas e fecha a reconciliação em `PASS`. Decision: D-122 / SALA D-081. Tag: `v1.15.0-motor-us`.

## 2026-06-27

- feat(T-SDC-ANALISTA-USA-CANONICAL-LAYER-V1): cria `lib/spc.py` (portado de `RENDA_OPS/lib/spc.py`, com B+C consultivo) e `pipeline/analise_us.py` (camada canonica que produz `data/ssot/contexto_analista_us.json`). Skill `analista-usa` refatorada para Passo 0b obrigatorio. SALA D-077 / USA D-121.

## 2026-06-13

- feat(motor): T-REBALANCE-D0-RECONCILIATION-US-V1 — adiciona reconciliacao D0 de rebalance no painel US com `_build_rebalance_sell_suggestions` (venda total para holdings fora da lista travada e aparo acima de `max_weight_cap=6%`), mantendo tabela SPC defensiva separada de "Vendas de Rebalance D0". Skill `analista-usa` atualizada com Passo 2b e prioridade de vendas D0 no Passo 9. Rastreabilidade: SALA D-062 / USA D-120 / R-042. Tag de motor prevista: `v1.14.0-motor-us` (pos-auditoria).

## 2026-06-10

- feat(backtest): T-RESEARCH-DATASET-FREEZE-US-V1 — dataset de pesquisa congelado/versionado (manifest+SHA256), load_inputs com US_RESEARCH_DATASET_DIR retrocompatível, verificador de integridade e reconciliação C4 vs winner selado. Decision: D-119 / SALA D-058. R-041.
- feat(backtest): T-DPLUS1-EXTENDED-US-V1 — compara V0 (A1D D+1 + ranking vivo D+2..D+9) vs V1 (lista travada estendida D+1..D+9) para alocacao de caixa livre inter-rebalance; veredito pre-registrado `MELHORA_V0`. Decision: D-118 / SALA D-057.
- feat: T-SDC-FRED-API-ROBUSTNESS-STAGNATION-ALARM-V1 — FRED passa a usar API oficial com `FRED_API_KEY` como fonte primaria no macro US, mantendo CSV publico como fallback secundario e emitindo WARNING/notify-send para series stale acima de 3 pregoes. Decision: D-117 / SALA D-056.

## 2026-06-09

- fix(motor): T-SDC-CHART-BASE1-MARKETDAY-AXIS-V1 — eixo temporal do grafico Base 1 passa a ser indexado por `market_day/ref_day` (R-022), preservando corte ledger por `exec_day` e calculo patrimonial. Decision: D-116 / SALA D-055. Tag prevista: v1.13.0-motor-us.

## 2026-06-08

- docs(curadoria): T-SKILL-ANALISTA-USA-A1D-V1 - fecha a rastreabilidade da política A1D D+1 na skill `analista-usa` com D-115 registrado e skill consultiva atualizada com Passo 4c + gate do Passo 5. Artefatos: `.cursor/skills/analista-usa/SKILL.md`, `DECISION_LOG.md`. Decision: D-115
- feat: T-EXEC-COMPLETION-US-V2 - baseline por construcao em C4, validacao dual HOLDOUT+SW_RECENT e politica D+1 utilizavel. Artefatos: backtest/t_exec_completion_us/run_t_exec_completion_us_v2.py, backtest/t_exec_completion_us/results/verdict_v2.json, backtest/t_exec_completion_us/results/curve_v2_*.csv, DECISION_LOG.md (ref: D-114)

## 2026-03-07

- chore: initial commit — estrutura do repo operacional US (Russell 1000 + SmallCap 600)
- docs: criar trinca de governança (GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md)
- docs: criar PLANO_USA_OPS.md (plano de execução completo)
- docs: copiar CORPUS_FABRICA_BR.md como referência

## 2026-03-16

- chore: T-001 setup do repositório (venv, requirements.txt fixo, skeleton lib/ e pipeline/)
- ref: D-001
- feat: T-002 — portar engine/metrics/io + criar adapters US (PolygonAdapter + FredAdapter). Artefatos: lib/engine.py, lib/metrics.py, lib/io.py, lib/adapters.py (ref: D-001, D-003)
- docs: T-003 — criar MANIFESTO_ORIGEM.json (proveniência + SHA256). Artefatos: MANIFESTO_ORIGEM.json (ref: D-001)
- docs: T-004 — confirmar corpus BR como referência (cópia + SHA256 + manifesto). Artefatos: docs/CORPUS_FABRICA_BR.md, MANIFESTO_ORIGEM.json (ref: D-001)
- docs: T-005 — criar SPEC do pipeline US (schemas, fontes, riscos, anti-lookahead/anti-survivorship). Artefatos: docs/SPEC_PIPELINE_US.md (ref: D-003)
- feat: T-006 — gerar composição histórica (proxy ETF) R1000/IWB e SP600/IJR com gate rígido de cobertura + evidências. Artefatos: config/index_proxies_us.json, scripts/t006_build_index_compositions.py (ref: D-004)
- fix: T-006 — reescrever composição do universo via CSV público iShares (IWB/IJR) em modo snapshot + evidências. Artefatos: config/index_proxies_us.json, scripts/t006_build_index_compositions.py, docs/SPEC_PIPELINE_US.md (ref: D-005)
- fix: T-006 — filtrar tickers inválidos (ex.: '-') do snapshot iShares + evidenciar descartes/pesos nulos + corrigir SHA256 do DECISION_LOG no MANIFESTO_ORIGEM. Artefatos: scripts/t006_build_index_compositions.py, MANIFESTO_ORIGEM.json (ref: D-005)
- feat: T-007 — ingestão massiva OHLCV + dividendos + splits (Polygon) para universo do snapshot iShares (T-006), com dedupe (date,ticker) + evidências de cobertura. Artefatos: scripts/t007_ingest_us_market_data_raw.py, data/ssot/us_market_data_raw.parquet (2.87M rows), data/ssot/t007_ingestion_report.json, data/ssot/t007_failures.json (ref: D-003, D-005)
- feat: T-008a — Reference data US por ticker (active/delisted, list_date, ticker changes) via Polygon/Massive + evidências. Artefatos: scripts/t008a_ingest_ticker_reference_us.py, data/ssot/ticker_reference_us.parquet, data/ssot/t008a_reference_report.json (ref: D-006)
- feat: T-008 — Qualidade SPC por ticker + blacklist HARD/SOFT + universo operacional. Artefatos: scripts/t008_quality_spc_and_blacklist.py, config/blacklist_us.json, data/ssot/us_universe_operational.parquet, data/ssot/t008_quality_report.json (ref: D-002)
- feat: T-009 — Exclusão de tickers com BDR na B3 (anti-sobreposição) + evidências. Artefatos: scripts/t009_exclude_bdrs.py, data/ssot/bdr_exclusion_list.json, docs/SPEC_PIPELINE_US.md (ref: D-001)
- feat: T-010 — SSOT canônico US (canonical_us.parquet) consolidando raw+SPC+reference+exclusão BDR + evidências. Artefatos: scripts/t010_build_canonical_us.py, data/ssot/canonical_us.parquet, data/ssot/t010_canonical_report.json, docs/SPEC_PIPELINE_US.md (ref: D-001)
- feat: T-011 — Macro expandido US (FRED) + features sem lookahead (shift(1)) + evidências. Artefatos: scripts/t011_ingest_macro_us.py, data/ssot/macro_us.parquet, data/features/macro_features_us.parquet, data/ssot/t011_macro_report.json (ref: D-003)

## 2026-03-17

- chore: T-PURGE — purga física artefatos Phase 1 v1 + archive auditorias + reset SPEC/ROADMAP p/ v2 (ref: D-008)
- feat: T-006v2 — universo histórico anual via Polygon /v3/reference/tickers + evidências (ref: D-007)
- feat: T-007v2 — ingestão OHLCV+dividends+splits com adjusted=False + chunks retomáveis + report/failures (ref: D-007)
- feat: T-008av2 — reference data por ticker (details+events) no universo v2 com chunks+report+failures (ref: D-007)
- feat: T-008v2 — SPC Shewhart completo (I-MR + Xbar-R) + blacklist HARD/SOFT no universo v2 (ref: D-007)
- fix: T-008v2-FIX — remover outlier_rate da blacklist SOFT (SOFT apenas history_days<252) e alinhar SPEC ao RENDA_OPS (ref: D-009)
- feat: T-009v2 — exclusão de tickers com BDR na B3 (anti-sobreposição) no universo v2 + evidências. Artefatos: scripts/t009_exclude_bdrs_v2.py, data/ssot/bdr_exclusion_list.json, data/ssot/t009v2_bdr_exclusion_report.json (ref: D-007, D-001)
- feat: T-010v2 — SSOT canônico US v2 consolidando raw+SPC+reference+exclusão BDR + evidências. Artefatos: scripts/t010_build_canonical_us_v2.py, data/ssot/canonical_us.parquet, data/ssot/t010v2_canonical_report.json (ref: D-007, D-001)
- feat: T-011v2 — Macro US com `outer merge -> ffill -> filter` + features shift(1) + evidências. Artefatos: scripts/t011_ingest_macro_us_v2.py, data/ssot/macro_us.parquet, data/features/macro_features_us.parquet, data/ssot/t011v2_macro_report.json (ref: D-007, D-003)
- fix: T-012-FIX — remover gate quantitativo arbitrário (median_tickers_ge_3500) do scoring M3-US e manter métrica apenas como evidência. Artefatos: scripts/t012_compute_scores_m3_us.py, data/features/scores_m3_us.parquet, data/features/t012_scores_report.json (ref: D-011, D-012, D-010)
- feat: T-013 — feature engineering US (macro shiftado + SPC/M3 cross-section + equity proxy sem lookahead) + feature guard + evidências. Artefatos: scripts/t013_build_features_us.py, config/feature_guard_us.json, data/features/dataset_us.parquet, data/features/t013_features_report.json (ref: D-002, D-009, D-010, D-012)

## 2026-03-18

- feat: T-014 — labels de regime US (oracle drawdown-based no S&P 500 via FRED, threshold calibrado no TRAIN, walk-forward split) + dataset rotulado + evidências. Artefatos: scripts/t014_build_labels_us.py, data/features/labels_us.parquet, data/features/dataset_us_labeled.parquet, data/features/t014_labels_report.json (ref: D-002, D-009, D-010)
- fix: T-012-FIX2 — stale_tickers rolling por dia (elimina lookahead no backtest; preserva equivalência no último dia via gate) (ref: D-013)
- chore: T-012-FIX2-MANIFEST-SHA-ALIGN — atualização de SHA256 no MANIFESTO_ORIGEM.json após re-execução (t012, t013, t014). Artefatos: MANIFESTO_ORIGEM.json
- feat: T-015 — framework de backtest US (C1/C2/C3, T+1, custos, outputs CSV/JSON/HTML + report com gates). Artefatos: backtest/run_backtest_variants_us.py, backtest/plot_t015_plotly.py, backtest/results/summary_t015_variants.csv, backtest/results/summary_t015_variants.json, backtest/results/t015_backtest_report.json, backtest/results/plot_t015_equity_comparison.html (ref: D-002)
- docs: D-014 — registrar decisão do Owner de aceitar T-015 como PASS (finding do auditor era leitura incompleta do código; C1 colapsando é evidência de rotação alta no universo US). (ref: D-014)
- feat: T-016 — venda defensiva permanente no backtest US (camada 0 split adjustment + severity score 0–6 + vendas 25/50/100 + quarentena + events + plots). Artefatos: backtest/run_backtest_variants_us.py, backtest/results/t016_backtest_report.json, backtest/results/events_defensive_sells.csv, backtest/results/events_split_adjustments.csv, backtest/plot_t015_plotly.py (ref: D-002)
- fix: T-016-FIX — backtest US: split event-based (sf_D/sf_{D-1}) para camada 0 + equity_base100 nas curvas + métricas no report. Artefatos: backtest/run_backtest_variants_us.py, backtest/plot_t015_plotly.py, backtest/results/t016_backtest_report.json (ref: D-015)
- feat: T-017 — ablação TopN × Cadence × K no backtest US (grade de parâmetros + summary CSV/JSON + report com gates). Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_report.json (ref: D-002)
- fix: T-017-FIX — ablação T-017 com filtro min_market_cap (>=300M USD) no universo operacional (aplicado no date do score, anti-lookahead) + novos summaries/report. Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_summary.csv, backtest/results/t017_ablation_summary.json, backtest/results/t017_ablation_report.json (ref: D-016, D-017)
- fix: T-017-FIX2 — ablação ampliada (TopN=[10,15,20,25], Cad=[5,10,21], K=[10,15,20,30]) com filtro min_market_cap=300M (anti-lookahead no date do score) + novos summaries/report. Artefatos: backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_summary.csv, backtest/results/t017_ablation_summary.json, backtest/results/t017_ablation_report.json, MANIFESTO_ORIGEM.json (ref: D-018, D-016)
- fix: T-016-FIX2 — corrigir split_event_wide no backtest: derivar ratio do preço raw (px_{D-1}/px_D) ao invés de sf_D/sf_{D-1} para preservar valor econômico por construção. 27 eventos divergentes corrigidos (15 no holdout). Re-executada ablação completa (72 combos). Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t017_ablation_us.py, backtest/results/t017_ablation_summary.csv, backtest/results/t017_ablation_summary.json, backtest/results/t017_ablation_report.json, MANIFESTO_ORIGEM.json (ref: D-019, D-015)
- feat: T-018 — variante C4 (score-weighted com dampening + cap de concentração + trims no rebalanceamento) + ablação dedicada T-018 + plots Plotly. Artefatos: backtest/run_backtest_variants_us.py, backtest/run_t018_ablation_us.py, backtest/plot_t018_plotly.py, backtest/results/t018_ablation_summary.csv, backtest/results/t018_ablation_summary.json, backtest/results/t018_ablation_report.json, backtest/results/plot_t018_*.html, backtest/results/events_concentration_trims.csv, MANIFESTO_ORIGEM.json (ref: D-019)
- feat: T-021 — análise de concentração + drawdown por ticker (decomposição MDD, série temporal concentração, efetividade do cap, plots Plotly). Artefatos: backtest/run_t021_concentration_analysis.py, backtest/results/t021_concentration_report.json, backtest/results/t021_daily_concentration.csv, backtest/results/t021_drawdown_decomposition.csv, backtest/results/plot_t021_*.html, MANIFESTO_ORIGEM.json (ref: D-002)
- feat: T-022 — dual acid window US (pior drawdown HOLDOUT em SP500 + proxy Russell1000 via FRED, min 6 meses) + métricas do motor nessas janelas + plots Plotly. Artefatos: backtest/run_t022_dual_acid_window_us.py, backtest/results/acid_analysis_us.json, backtest/results/plot_t022_*.html, MANIFESTO_ORIGEM.json (ref: D-002)
- feat: T-024 — declaração canônica do winner US (C4 TopN=20 Cad=10 K=10 cap=6% min_market_cap=300M) + métricas HOLDOUT + acid test + evidências e SHA256. Artefatos: config/winner_us.json, MANIFESTO_ORIGEM.json (ref: D-021, D-020)
- docs: CORPUS_FABRICA_US.md — consolidação de toda a experiência Phases 0-3, lições certas/erradas, divergências justificadas, lições cruzadas BR↔US, checklist Phase 4+. Artefatos: docs/CORPUS_FABRICA_US.md, MANIFESTO_ORIGEM.json, GOVERNANCE.md §8 atualizado
- docs: HANDOFF_PHASE4.md — documento de continuidade para novo chat (onde estamos, artefatos, regras, riscos, como iniciar). Artefatos: docs/HANDOFF_PHASE4.md, MANIFESTO_ORIGEM.json
- feat: T-025 — treinar XGBoost US (TRAIN-only) e gerar y_proba_cash + config/ml_model_us.json + report com gates (ref: D-002)
- feat: T-026 — ablação thr/h_in/h_out do ML trigger US (seleção TRAIN-only) + config/ml_trigger_us.json + report com gates (ref: D-002)
- feat: T-025v2 — retreinar ML trigger US com features estacionárias (sem _level) + report consolidado (ref: D-022)
- feat: T-027 — comparar C4 puro vs C4+ML trigger (histerese em y_proba_cash) + report consolidado (ref: D-023)
- fix: T-027v2 — corrigir reconciliação do baseline (z_wide 1:1) + adicionar Plotly comparação C4 puro vs C4+trigger (ref: D-023)
- feat: T-029 — pipeline operacional US (steps 01–12) + orquestrador `pipeline/run_daily.py` (ref: D-024)
- feat: T-030 — painel diário HTML (USD/NYSE) com Plotly (252 pregões + Base100) + resumo DFC/Balanço mínimo (ref: D-024)
- feat: T-031 — servidor/lançador (porta 8788) + catch-up automático de pregões NYSE + atalho Desktop (ref: D-024)
- feat: T-032 — duplo-caixa US (T+1) no painel + salvamento de boletim (data/real) via lançador (ref: D-024)

## 2026-03-19

- feat: T-034 — blindagem do motor + operational_window + ingestão incremental diária (ref: D-025, D-026). Artefatos: `data/ssot/operational_window.parquet`, `data/ssot/operational_market_data_raw.parquet`, `pipeline/00_incremental_ingest.py`, `pipeline/rebuild_operational_window.py`, `.git/hooks/pre-commit`, `tools/pre_commit_motor_guard.sh`, `tools/install_git_hooks.sh`, `docs/OPERACAO_DADOS.md`, `docs/T034_REEXECUCAO_E_HIGIENE.md`. Tag: `v1.0.0-motor-us`
- feat: T-037 — painel diário US reescrito no formato exato do painel BR (RENDA_OPS): CSS/JS/estrutura idêntica, adaptações US (USD, NYSE, T+1, Top-20, Drawdown%, Base1 sem CDI), seções adicionadas (Carteira Comprada, Carteira Atual D-1, Top-20, Card de Venda, Duplo-Caixa + Balanço + DFC), seções removidas (Resumo, Reconciliação, Proventos), servidor `/salvar` retornando `paths[]`. Artefatos: `pipeline/painel_diario.py`, `pipeline/servidor.py`, `data/daily/painel_2026-03-19.html`, `data/cycles/2026-03-19/painel.html` (ref: D-027)

## 2026-03-20

- fix: D-028 — corrigir fórmula de Patrimônio Inicial no Balanço Simplificado: eliminar CAIXA_ORIGINAL (valor de mercado dinâmico), usar Aportes acumulados - Retiradas acumuladas (capital líquido injetado). Label renomeado para "Capital Líquido Aportado". Artefatos: `pipeline/painel_diario.py` (ref: D-028)

## 2026-03-22

- fix: T-038 — separar Top-20 informativo (top20_by_score) da carteira ativa no decision JSON e no painel. Artefatos: `pipeline/09_decide.py`, `pipeline/painel_diario.py` (ref: D-029)

## 2026-03-26

- fix: T-039 — mover venda defensiva do step 09 para o painel usando carteira real (`build_lot_ledger`) e eliminar sugestões fantasmas para posições zeradas. Artefatos: `pipeline/09_decide.py`, `pipeline/painel_diario.py` (ref: D-033, D-032)

## 2026-04-01

- 2026-04-01 | T-040 | feat: desacoplar semântica temporal nos artefatos operacionais (exec_day, market_day, trade_day) — D-040, R-022. Toca painel_diario.py (blindado). MOTOR_OVERRIDE.

## 2026-04-02

- 2026-04-02 | T-041 | fix: resiliência FRED no step 05 — fallback para macro_us existente + gate D-2 (D-041, paridade D-027 BR)
- 2026-04-02 | T-042 | fix: rebalance por contagem relativa de pregões desde último rebalance, com fallback bootstrap e persistência em last_rebalance.json (D-043, R-018, D-032). Toca 09_decide.py (blindado). MOTOR_OVERRIDE.
- 2026-04-02 | T-042r | fix: cura documental do rebalance relativo após validação do Auditor; decision_2026-04-02.json e last_rebalance.json alinhados ao estado operacional. Artefatos: data/daily/decision_2026-04-02.json, data/daily/last_rebalance.json (ref: D-043).
- 2026-04-02 | T-043 | feat: parâmetro dry_run em run() de 09_decide.py para isolar escrita de disco em testes/auditorias (D-044, D-032). Toca 09_decide.py (blindado). MOTOR_OVERRIDE.

## 2026-04-03

- feat: T-045 — SSOT ledger imutável (D-045). Novo pipeline/ledger.py com event sourcing financeiro. Migração de 9 boletins + gap 01/04. Painel e servidor refatorados para ler/escrever no ledger. Boletins preservados como artefato de exibição.
- fix: T-046 — Corrige injeção de caixa fantasma por SETTLEMENT sem ref_id e duplicação de eventos no servidor (D-045). Auditoria forense Gemini/Kimi.
- chore: T-047-HF — commit com MOTOR_OVERRIDE + tag v1.4.0-motor-us selando T-041/T-042/T-043/T-045/T-046 (D-041/D-043/D-044/D-045). Governança Git exigiu consolidação.

## 2026-04-04

- T-052: Separar pipeline em duas fases (--ingest-only / --decision-only) + --dry-run + orquestrador run_all.sh (D-052)
- audit: T-052 — auditoria retroativa pós-commit (Gemini PASS + Kimi PASS + Auditor Principal FAIL governança -> saneado em T-053, D-053)
- chore: T-053 — saneamento de governança pós-auditoria retroativa T-052. Tags v1.8.0-motor + v1.5.0-motor-us criadas. R-024 adicionada ao corpus. (D-053)

## 2026-04-07

- feat: T-054 — exchange_calendars como infraestrutura de pregões (B3/NYSE). lib/trading_calendar.py criado; run_daily_assert_ssot_fresh*, 01_ingest_macro, ledger_br, 05_build_macro_expanded, auto_simulate migrados para calendário real. Guard no iniciar.sh. (D-054)
- audit: T-054 curada com PASS — calendários reais de B3/NYSE validados, dry-run e ingest-only sem regressão, sem blindados tocados. Artefatos: ROADMAP.md, DECISION_LOG.md, CHANGELOG.md (D-054)
- feat: T-055 — deteccao automatica de corporate actions (split) no painel_diario.py:_detect_and_adjust_splits com filtro temporal as_of_day (fix H1 Gemini), alerta visual HTML, campo corporate_actions no JSON, base-1 com close_operational (D-055)
- audit: T-055 curada com PASS — split POWL ajustado automaticamente, alerta inline no boletim e painel validado end-to-end. Artefatos: pipeline/painel_diario.py, ROADMAP.md, DECISION_LOG.md, CHANGELOG.md (D-055)

## 2026-04-08

- feat: T-SC-001 — freshness guard no --ingest-only: skip automático quando SSOT date_max >= prev_session(run_date). Evita re-fetch desnecessário de Polygon após ingest do timer. (D-056)
- audit: T-SC-001 curada com PASS — guarda de frescura validada em runtime para US; ingest-only retorna SKIPPED com SSOT já fresco, --full e --decision-only sem regressão. Artefatos: pipeline/run_daily.py, CHANGELOG.md, ROADMAP.md (D-056)

## 2026-04-11

- T-036 (D-003): Redirecionar stdout/stderr de subprocess.run() em 00_incremental_ingest.py, rebuild_operational_window.py e 04_build_canonical.py para arquivos de log — imuniza pipeline contra stdout morto do servidor web
- fix: T-036 — Redirecionar stdout/stderr de subprocess.run() nos callers do pipeline para arquivos de log. Artefatos: pipeline/00_incremental_ingest.py, pipeline/rebuild_operational_window.py, pipeline/04_build_canonical.py, logs/t008_window_subprocess.log, logs/t009_window_subprocess.log, logs/t010_window_subprocess.log (D-003)
fix: T-036-MOTOR (D-004) — redirecionar stdout/stderr de subprocess.run() em pipeline/04_build_canonical.py (blindado) para arquivos de log via _run_logged_subprocess — ciclo de motor com MOTOR_OVERRIDE, Auditor duplo Gemini+Kimi, tag v1.7.0-motor-us
- fix: T-036-MOTOR — curar alteração blindada em `pipeline/04_build_canonical.py` com Auditor duplo e `MOTOR_OVERRIDE`. Artefatos: pipeline/04_build_canonical.py, CHANGELOG.md, ROADMAP.md (D-004)

## 2026-04-14

- fix[motor](MOTOR-OVERRIDE): T-061-BASE1-v2 — Base 1 US com denominador vetorizado por ponto (patrimônio cumulativo `aportes - retiradas`) em substituição à base fixa `total_ativo.iloc[0]`, preservando leitura do boletim real, eixo operacional e extensão até `market_day`. Artefatos: pipeline/painel_diario.py, DECISION_LOG.md (D-058).
- docs: T-CORPUS-US-V2 (D-059) — reescrita completa do `docs/CORPUS_FABRICA_US.md` para cobrir D-001..D-058, incluir Phase 7, atualizar arquitetura operacional (ledger imutável, pipeline bifásico, calendário real, `dry_run`, `last_rebalance`), consolidar novas lições/erros e alinhar blindagem para `v1.6.0-motor-us`.
- docs: T-CORPUS-US-V2 curada com PASS (D-059) — fechamento da rastreabilidade do corpus US após auditoria favorável. Artefatos: docs/CORPUS_FABRICA_US.md, DECISION_LOG.md, CHANGELOG.md.

## 2026-04-15

- feat(painel): T-PAINEL-GRAFICOS-US — reforma visual dos gráficos US com paridade ao padrão BR corrigido (T-PAINEL-GRAFICOS): layout compacto, Motor Status C4 em cards e Base 1 com eixo temporal real NYSE. (ref: D-060)
- docs: T-PAINEL-GRAFICOS-US curada com PASS — alinhamento de `GOVERNANCE.md` §6.6 à tag `v1.6.0-motor-us` e resolução de DT-007 no corpus US. Artefatos: `GOVERNANCE.md`, `docs/CORPUS_FABRICA_US.md` (ref: D-060)

## 2026-04-16

- docs: T-GOV-DOC-US — saneamento de governança documental do USA_OPS: §6.7 adicionado ao GOVERNANCE.md (regra de espelhamento SALA + convenção de prefixo), D-061 e D-062 registrados no DECISION_LOG.md, CORPUS_FABRICA_US.md atualizado para D-062, cópia stale docs/CORPUS_FABRICA_BR.md confirmada ausente no workspace. (D-061, D-062; ref: SALA D-010, SALA D-011)

## 2026-04-17

- fix(motor) [MOTOR-OVERRIDE]: T-063-PIPELINE-IDEMPOTENCY — idempotência do pipeline de decisão US: `decision_{date}.json` imutável após primeira execução do dia e `last_rebalance.json` atualizado somente após salvamento do boletim via `/salvar`. Artefatos: `pipeline/09_decide.py`, `pipeline/servidor.py`, `DECISION_LOG.md` (D-063).
- audit: T-063-PIPELINE-IDEMPOTENCY curada com PASS — idempotência validada, `decision_{date}.json` preservado na primeira execução válida e `last_rebalance.json` commitado apenas no `/salvar`. Artefatos: `pipeline/09_decide.py`, `pipeline/servidor.py`, `CHANGELOG.md`.

## 2026-04-20

- docs: T-GOV-D013-US — espelho documental de SALA D-013 no USA_OPS: USA D-064 registrado no DECISION_LOG.md (Protocolo de Entrada obrigatório no analista-usa), CORPUS_FABRICA_US.md corrigido para D-063/D-064, CHANGELOG atualizado. (ref: SALA D-013, §6.7)
- fix(motor) [MOTOR-OVERRIDE]: T-MOTOR-ANCHOR-US-V1 — porta `09_decide.py` para cadência absoluta ancorada: `is_rebalance_day` passa a usar `rebalance_anchor_date + rebalance_phase_offset + rebalance_cadence` de `winner_us.json`, eliminando leitura de `last_rebalance.json` como gatilho. `_load_last_rebalance_dt` / `_save_last_rebalance_dt` marcadas como dead code. Artefatos: `pipeline/09_decide.py`, `config/winner_us.json` (Step 1 pré-aplicado), `DECISION_LOG.md` (D-065), `GOVERNANCE.md` (tag v1.7.0-motor-us), `CHANGELOG.md`, `docs/CORPUS_FABRICA_US.md`. (D-065, D-063, R-030)

## 2026-04-21

- docs: T-D066-SPC-ALERTA-USA — espelho parcial de RENDA_OPS D-084 Frente 1 no USA_OPS: ALERTA DURO de candidato INSTAVEL por SPC inserido no Passo 5 do analista-usa; D-066 registrado no DECISION_LOG.md; L-US-39 e nota operacional §3.4 adicionadas ao CORPUS_FABRICA_US.md. Nenhum arquivo de motor blindado tocado. (ref: D-066, RENDA_OPS D-082/D-083/D-084, L-25 BR, R-001)

## 2026-04-29

- feat(skills): T-R034-SKILLS-SPC-LOCK-US — gate R-034 inserido em `analista-usa` (Passo 1 e Passo 5) e `analista-br` (Passo 1 e Passo 5); backlog `T-REBALANCE-WEAKNESS-US` e `T-SPC-BC-MOTOR-US` adicionados ao ROADMAP. (ref: D-068, D-070, R-034)
- fix(motor): T-PAINEL-APORTE-LEDGER-US — Base 1 do painel US lê aportes/retiradas do ledger SSOT via `read_all_events()` com fallback legado para `cash_movements`; tag `v1.8.0-motor-us` registrada na blindagem. Artefatos: `pipeline/painel_diario.py`, `DECISION_LOG.md`, `GOVERNANCE.md`, `ROADMAP.md`. Decision: D-069.

## 2026-04-30

- fix(motor) [MOTOR-OVERRIDE]: T-BASE1-LEDGER-CUTOFF-FIX-US — corrigir semântica temporal do corte ledger-first em `_build_real_base1_series`: usar `exec_day` do boletim (não `market_day`) como data de corte na filtragem de APORTE/RETIRADA/DIVIDENDO do ledger SSOT. Resolve regressão "Base 1 indisponível" introduzida por D-069. Ref: D-071, D-069, D-040, D-045.
- audit: T-BASE1-LEDGER-CUTOFF-FIX-US curada com PASS — Base 1 validada em 17 registros, placeholder ausente e tag `v1.9.0-motor-us` confirmada. Artefatos: `pipeline/painel_diario.py`, `DECISION_LOG.md`, `CHANGELOG.md`, `GOVERNANCE.md`, `docs/CORPUS_FABRICA_US.md` (ref: D-071).

## 2026-05-06

- docs(curadoria): T-CURATION-088C-CLOSE - fechar T-088C-SPC-ENRICHED-ABLATION-US-PARITY como DONE com veredito INCONCLUSIVO; D-079 registrado; T-SPC-BC-MOTOR-US permanece BLOCKED; L-US-41 adicionada ao corpus US. (ref: D-079, D-078, D-070)
- feat(backtest): T-088C-SPC-ENRICHED-ABLATION-US-PARITY — rerun da ablacao SPC com paridade estrita BR (blocked_b=Regra1+runs_value, blocked_bc=blocked_b+runs_disp), preservando filtro market_cap >= 300M do T-088B; runs_xbar e runs_r removidos; D-078 registrado. (ref: D-078, D-077, D-076, D-070)
- feat(backtest): T-088B-SPC-ENRICHED-ABLATION-US-FIXED — rerun da ablacao SPC enriquecida (3 bracos Baseline/B/B+C, phase sweep 10 fases) com filtro market_cap >= 300M por d_prev, lido de winner_us.json (paridade com pipeline/09_decide.py); veredito de T-088 suspenso por gap metodologico; D-077 registrado. (ref: D-077, D-076, D-070)
- feat(backtest): T-088-SPC-ENRICHED-ABLATION-US — ablacao de 3 bracos SPC enriquecido (Baseline/B/B+C, phase sweep 10 fases, criterio pre-registrado em CVaR5/Sharpe/recidiva) sobre motor C4 US; veredito final em phase_sweep_stats_t088_us.json; D-076 registrado; nenhum arquivo de motor blindado tocado. (ref: D-076, D-075, D-070, RENDA_OPS D-087/D-088/D-090)
- research(backtest): T-REBALANCE-WEAKNESS-US-FASE-B-PREP — paridade metodologica BRxUS: magnitude log_ret_3 (BR -1.33 p.p. vs US -1.01 p.p.), tautologia became_instavel_K identica em ambas fabricas, gate motor BR usa blocked_bc (Regra 1 + Nelson/WE B+C) e nao spc_status isolado, sem threshold numerico de log_ret no motor BR; D-075 registrado; parity_with_renda_ops.md e .json gerados. (ref: D-075, D-074, D-073, D-070, RENDA_OPS D-082/D-088/D-090)
- chore(backtest): T-REBALANCE-WEAKNESS-US-FASE-A — reanalise honesta dos dados existentes: matriz spc_status x rank_trend x lookback x split; demonstracao formal de bug semantico em compute_verdict e tautologia em became_instavel_K; relatorio reanalysis_report.md e reanalysis_report.json em results/; observations_*.csv excluidos do git (regeneraveis); D-074 registrado. (ref: D-074, D-073, D-070)
- feat(backtest): T-REBALANCE-WEAKNESS-US — backtest isolado de rank-decay + spc_status pre-rebalance Top-20+Top-30 (paridade RENDA_OPS D-082/D-083); veredito pre-registrado em verdict.json; D-073 registrado no DECISION_LOG.md; ROADMAP atualizado IN_PROGRESS. (ref: D-073, D-070, D-066)
- docs(skills): T-NELSON-WE-AVISO-ANALISTA-USA — aviso consultivo duro Nelson/WE fora Regra 1 inserido em Passo 4 (holdings ativos) e Passo 5 (candidatos) da skill `analista-usa`; D-072 registrado em `DECISION_LOG.md`. Nenhum arquivo de motor blindado tocado. (ref: D-072, RENDA_OPS D-101, L-27, R-020)

## 2026-05-13

- docs(gov): T-096-SKILLS-SPC-CANVAS-60D - espelha decisao RENDA_OPS D-106 na skill `analista-usa` (Passo 4), exigindo canvas de 60 pregoes para ativos instaveis. D-081 registrada.

## 2026-05-16

- docs(curadoria): T-PAINEL-SETTLEMENT-VIEW-US - adiciona secao informativa de vendas em liquidacao no painel US com reconciliacao de Caixa Contabil; nova funcao `sells_in_settlement()` no ledger e teste de reconciliacao em `tests/test_ledger.py`. (ref: D-082)

## 2026-05-20

- fix(ledger): T-USA-LEDGER-RESTORE-V1 — Restaura 28 linhas de operacoes reais (2026-05-18 a 2026-05-20) revertidas indevidamente de `data/ssot/ledger.jsonl` pelo Auditor-Gemini (R-025). Ref: SALA D-035, USA D-083.
- fix(gov): T-AUDITOR-HARDENING-V1 — Estende pre-commit hook para bloquear rollback/truncamento de `data/ssot/ledger.jsonl`. Espelha revisao das skills de auditoria (principio funcional, R-025). Ref: SALA D-036, USA D-084.
- fix(ledger): T-LEDGER-AUTOCOMMIT-V1 — Auto-commit+push do ledger SSOT em `pipeline/servidor.py` apos `/salvar`. Elimina janela de appends nao-commitados. Ref: SALA D-037, USA D-085.
- docs(gov): T-SDC-VENV-RUNTIME-ARCHITECT-HARDENING-V1 — Espelho SALA D-038: hotfix venv SALA, R-039 interfabricas, endurecimento skill architect. Ref: SALA D-038, USA D-086.
- docs(gov): T-SDC-INTERLOCUTOR-DIRECT-MODE-V1 — Espelho SALA D-039: modo direto na skill interlocutor-tecnico. Ref: SALA D-039, USA D-087.

## 2026-05-27

- fix: T-SSOT-FRESHNESS-GATE-US-V1 — gate SSOT D-1 estrito em `--decision-only`, catch-up OPW-first no servidor e validacao de cache em `09_decide`. Artefatos: `pipeline/run_daily.py`, `pipeline/servidor.py`, `pipeline/09_decide.py`, `DECISION_LOG.md` (ref: D-088). Tag: `v1.11.0-motor-us`.

## 2026-06-03

- fix: T-SDC-AUTOMACAO-SSOT-ROBUSTEZ-US-V1 — espelho de SALA D-048: `pipeline/01_ingest_macro.py` passa a usar fallback operacional quando o Step 01 falha por indisponibilidade do FRED (reaproveita `macro_us.parquet`, recompõe `macro_features_us.parquet`, notifica o Owner e permite continuidade do ingest). Ref: SALA D-048, USA D-107.
- fix: T-USA-MACRO-FALLBACK-DATE-LOCK-FIX-V1 — errata de D-107: fallback de `pipeline/01_ingest_macro.py` passa a usar `prev_session(XNYS)` para evitar data civil futura em `macro_us`; `pipeline/run_daily.py` ganha lockfile PID para impedir ingest-only concorrente. Ref: SALA D-049, SALA D-048, USA D-108.
- fix: (T-USA-DEFENSIVE-SELL-DOWNSIDE-V1) Venda defensiva SPC restrita a downside (portabilidade conceitual BR). Em `pipeline/painel_diario.py`, `_compute_defensive_actions_from_holdings` mantém `any_rule` para alerta visual e adiciona gate `downside` antes de gerar ordem de venda (`D-USA-DOWNSIDE-GATE`). Ref: USA D-109, SALA DECISION_LOG (decisão do Owner em 2026-06-03).

## 2026-06-04

- docs(curadoria): T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1 — fecha a rastreabilidade da calibracao preventiva de SATL/FCEL com pre-registro, execucao e relatorio auditado. Artefatos: `analise_interfabricas/T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1/preregistro.md`, `analise_interfabricas/T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1/run_calibration.py`, `analise_interfabricas/T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1/resultados.md`. Decision: D-050
- docs(curadoria): T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1 — fecha a rastreabilidade da calibracao de saida por forma da curva com pre-registro, execucao e relatorio auditado. Artefatos: `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1/preregistro.md`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1/run_exit_calibration.py`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1/resultados.md`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1/resultados_raw.json`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1/output_bruto.txt`. Decision: D-051
- docs(curadoria): T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2 — fecha a rastreabilidade da calibracao V2 de saida condicional (forma da curva + Nelson/WE downside + deterioracao de rank) com pre-registro, execucao e relatorio auditado. Artefatos: `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2/preregistro.md`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2/run_exit_calibration_v2.py`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2/resultados.md`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2/resultados_raw.json`, `analise_interfabricas/T-SDC-EXIT-SIGNAL-CALIBRATION-US-V2/output_bruto.txt`. Decision: D-052

## 2026-06-05

- docs(curadoria): T-LATE-ROCKET-ENTRY-US-V1 — fecha a rastreabilidade da ablation read-only do gate de entrada por foguete tardio no C4. Auditoria PASS; melhor braço `Arm_A_1.00`; veredito `CONFIRMA_SINAL_US`. Artefatos: `backtest/t_late_rocket_entry_us/decision_criterion_late_rocket_us.json`, `backtest/t_late_rocket_entry_us/run_t_late_rocket_entry_us.py`, `backtest/t_late_rocket_entry_us/results/observations_late_rocket_us.csv`, `backtest/t_late_rocket_entry_us/results/summary_TRAIN_late_rocket_us.csv`, `backtest/t_late_rocket_entry_us/results/summary_HOLDOUT_late_rocket_us.csv`, `backtest/t_late_rocket_entry_us/results/phase_sweep_stats_late_rocket_us.json`, `backtest/t_late_rocket_entry_us/results/verdict_late_rocket_us.json`. Decision: D-110
- docs(saneamento): T-DOC-SANEAR-V2-FOGUETE-TARDIO — corrige documentacao da V2 do gate foguete tardio: D-112 registrado, R-037 atualizada (V2 PARCIAL), ROADMAP com V2 DONE e V3 BLOCKED, CORPUS com E-US-23 e novo padrao de falha. Decision: D-112

## 2026-07-20

- docs(curadoria): T-SDC-RECONCILIADOR-BTG-AUTONOMO-US-V1 — fecha a rastreabilidade da reconciliacao autonoma BTG no LIVE-REAL-TEST com ledger real versionado em git, checkpoint forward-only e skill dedicada `reconciliador-btg`. Artefatos: `scripts/reconcile_and_apply.py`, `GOVERNANCE.md`, `DECISION_LOG.md`, `data/live_real_test/ledger_real.jsonl`, `data/live_real_test/reconciliation_log.jsonl`, `data/live_real_test/reconciliation_checkpoint.json`. Decision: D-145
