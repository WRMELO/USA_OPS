# Pre-registro de Vigilancia - F-13 (LIVE-REAL-TEST, Fabrica US)

**Task**: T-SDC-LIVE-REAL-TEST-PREREGISTRATION-F13-US-V1  
**Regime**: `LIVE-REAL-TEST` (Fabrica US), conforme `SALA D-083`, `SALA D-103`, `R-049`.  
**Congelado em**: 2026-07-16T11:35:45Z

IMPORTANTE: este documento deve ser escrito e seu hash SHA256 calculado (ver manifest.json) antes do primeiro fill real do dia-D. Alterar qualquer parametro abaixo apos o primeiro evento BUY em ledger_real.jsonl invalida o pre-registro e exige novo ciclo formal com nova decisao do Owner.

## ENQUADRAMENTO

Este NAO e um pre-registro de estudo de decisao (nao decide GO_LIVE para operacao plena). E um pre-registro de vigilancia operacional continua durante a fase LIVE-REAL-TEST, definindo antecipadamente os parametros do kill-switch monitorado de R-049 item 3, para que nenhum limiar seja ajustado a posteriori com base no resultado observado.

## VARIAVEL DE DECISAO

A serie usada para toda avaliacao desta vigilancia e exclusivamente a serie REAL (ledger_real.jsonl, contador n_live_real), conforme confirmado pelo Owner em SALA D-103. Esta serie nunca e fundida com o dry-run paralelo (Opcao 1) nem com o gemeo sombra (Opcao 2, ledger_shadow.jsonl) - ambos permanecem controles diagnosticos, nunca substitutos da serie REAL nesta vigilancia.

Fonte de dados: boletins reais em USA_OPS/data/live_real_test/, contados pela funcao _count_n_live_real_us() ja implementada em dryrun_decision_b.py/dryrun_decision_c.py (SALA D-090, F-14).

## DEFINICAO DO KILL-SWITCH (R-049 item 3, citacao literal)

> "o Trilho C, apos o primeiro aporte real, deixa de ser gate de entrada e passa a operar como kill-switch monitorado (P(Sharpe_real>0) < 0.50 por 3 semanas), sem abortar por perda de granularidade de tamanho"

## OPERACIONALIZACAO (parametros fixados agora, nao ajustaveis apos 1o fill)

1. Metrica: P(Sharpe_real > 0), probabilidade posterior calculada pelo Trilho C (dryrun_decision_c.py, bloco Bayesiano hierarquico), aplicada exclusivamente a serie REAL isolada (n_live_real observacoes).
2. Janela de avaliacao: 3 semanas de pregoes NYSE ~= 15 pregoes (cadencia de 5 pregoes/semana).
3. Clausula de ruido por granularidade (pre-condicao de ativacao): o kill-switch so passa a ser avaliado como gatilho de acao a partir do momento em que n_live_real >= 15 (primeira janela completa). Antes disso, qualquer valor de P(Sharpe_real>0) e publicado apenas como leitura informativa nos relatorios do Trilho C, nunca como gatilho de acao - evita falso-alarme por ruido de amostra pequena (motivacao registrada em R-049/SALA D-083).
4. Cadencia de revisao: semanal, a cada emissao dos trilhos B/C (dryrun_decision_b.py/dryrun_decision_c.py, tipicamente via atualizar_tracker_dryrun.sh), com leitura explicita dos campos n_live_real e real_test_status do bloco US.
5. Efeito do disparo (P(Sharpe_real>0) < 0.50 na janela avaliada, com n_live_real >= 15): NAO aborta automaticamente o teste - conforme o texto literal de R-049 item 3 ("sem abortar por perda de granularidade de tamanho"). O unico efeito automatico e a emissao de um alerta explicito no relatorio do Trilho C. O disparo aciona um checkpoint obrigatorio de revisao com o Owner via interlocutor-tecnico, que reavalia continuidade, reducao de exposicao ou ajuste. A decisao final e sempre explicita do Owner (R-020).
6. Criterio de promocao a operacao plena: fora do escopo desta vigilancia. Segue o gate formal B+C de D-064/R-049 item 5, aplicado a serie REAL, quando n_live_real for suficiente. Este pre-registro nao antecipa nem substitui esse veredito.

## PARAMETROS CONFIRMADOS DO CORTE ASSOCIADO (contexto - decididos pelo Owner em SALA D-103 e nesta sessao, nao redecididos por este documento)

- C0 (aporte inicial real, evento APORTE): US$ 20.008,72.
- Ancora de preco do gemeo sombra (Opcao 2, F-17, record-shadow-buy): fechamento de mercado de 2026-07-15.
- exec_date do evento APORTE inicial (init-cutover): 2026-07-16.

## REFERENCIAS

SALA D-083, SALA D-090, SALA D-103, SALA D-104, R-018, R-020, R-049, USA D-123, USA D-128, USA D-129, USA GOVERNANCE secao 6.7.
