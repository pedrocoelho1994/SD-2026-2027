# COMINDIR04 · Respostas ao ponto 11

Os valores vêm de [RESULTADOS.md](RESULTADOS.md) e das pastas de cada ensaio. O que foi deduzido, e não medido, está assinalado.

## Lote e latência

**Poupança.** Na rajada de 8(b), os 331 registos seguiram em 15 lotes em vez de 331 `REG`, menos 95 % das mensagens. O custo por registo desceu de 55,3 para 38,5 bytes, menos 30 %. O valor da COMINDIR03 é calculado, não medido. A poupança depende de haver tráfego: com o produtor parado, a COMINDIR03 não publicaria nada, enquanto aqui foram publicados 39 lotes vazios em cerca de 62 s, com cerca de 40 bytes cada. Esse é o custo do sinal de vida.

**Δ/2.** Em 8(b), a média de 2520 ms aproxima-se de Δ/2 = 2500 ms, e o máximo de 5004 ms mostra que a espera chega a Δ. A aproximação não deve ser generalizada: em 8(e) obtiveram-se 393, 2882 e 5694 ms, contra 500, 2500 e 5000 ms previstos. Δ/2 pressupõe chegadas uniformes, mas cada rajada dura cerca de 1 s, por isso todos os registos dependem do ponto do período em que a rajada calha.

**Aplicações e alternativa.** Esperar até Δ seria inaceitável quando a reação tem de ser imediata, como alarmes de segurança ou controlo de um equipamento. Aqui, um alarme pode demorar até 2Δ = 10 s, somando a espera nos dois líderes. Para desacoplar, o líder pode publicar os dados de imediato, ou por tamanho de lote, e manter o heartbeat num temporizador próprio, enviado só para assinalar vida.

## Escolha do `TIMEOUT`

**Compromisso.** Um `TIMEOUT` curto deteta mais depressa, mas tolera menos atrasos. Em 8(e), com o fator 3, o tempo de deteção médio foi de 3,57 s, 10,76 s e 25,74 s para Δ de 1, 5 e 10 s, sempre dentro do intervalo previsto. Não houve falsas suspeitas porque estes ensaios não tinham perturbações, logo não mostram a diferença de exatidão. Esta aparece em 8(d): a pausa de 20 s excedeu o `TIMEOUT` de 15 s e gerou 4 falsas suspeitas. Na falha 5, um atraso de 7 s causou 4 falsas suspeitas com fator 1 e nenhuma com fator 3. Deduz-se, sem ter sido medido, que uma pausa de 20 s não geraria suspeita com `TIMEOUT` = 30 s, à custa de uma deteção mais lenta, como os 25,74 s medidos com Δ = 10 s. Pelo intervalo do Enquadramento, reduzir Δ mantendo o `TIMEOUT` estreita a variação do tempo de deteção e tolera a perda de mais heartbeats, mas aumenta as mensagens: 63 lotes por minuto com Δ = 1 s, contra 7 com Δ = 10 s.

**Propriedade violada.** Durante a pausa de 8(d) deixa de se verificar a **exatidão**, porque um líder correto é suspeito. As 4 `RETOMA` trazem o mesmo `inc`, o que prova que o processo não reiniciou. Num sistema assíncrono não há limite para o atraso das mensagens nem para a velocidade dos processos, por isso nenhum valor de `TIMEOUT` a garante. O participante observa o mesmo nos dois casos: 15,6 s de silêncio numa falha real em 8(a) e 15,1 a 15,5 s na pausa de 8(d). Só o que acontece depois os distingue.

## `ultimoSeq` e lacunas

**O que acrescenta.** O mecanismo de lacunas da COMINDIR02 só reage a registos que chegam. Se nada chegar depois de uma perda, não há nada que a revele. O `ultimoSeq` diz à réplica até onde vai a sequência, mesmo sem dados. Em 8(a), o registo 3 nunca foi publicado e foi recuperado por TCP depois do primeiro lote vazio da nova incarnação (`ultimoSeq` = 3, *n* = 0), nas réplicas 1 e 3.

**Quarta falha.** Com `ultimoSeq` = 0 e um em cada dez lotes descartado, as réplicas ficaram com 15 de 20 registos em p0, 20 de 20 em p1 e 9 de 20 em p2. Nesta implementação, herdada da COMINDIR03, uma lacuna só é pedida quando há mais de 10 registos retidos. Em p1, a lacuna do `seq` 4 foi recuperada ao chegar o 15. Em p0 faltou o 16 e ficaram retidos 4 registos (17 a 20); em p2 faltou o 10 e ficaram retidos 10 (11 a 20). Em ambos os casos o limite não foi ultrapassado e o pedido nunca foi feito. Os 48 `HB_PERDIDO` mostram que se perderam lotes, mas não quais os registos em falta. Com o `ultimoSeq` correto, o lote seguinte, mesmo vazio, despoletaria a recuperação.

**Porque basta nos lotes vazios.** Sem dados novos, a lacuna deixa de se revelar sozinha, e é aí que o campo faz falta. Os lotes vazios são emitidos a cada Δ enquanto o líder estiver vivo. Como o `ultimoSeq` é cumulativo, perder um lote não obriga a reenviá-lo: qualquer lote seguinte volta a anunciar o valor. Isto cobre a perda em silêncio. A condição é o líder estar vivo, porque só ele emite lotes e responde ao pedido TCP.

## Heartbeats e relógios lógicos

Os relógios lógicos ordenam eventos relevantes para o estado da aplicação. Um heartbeat não altera esse estado nem cria uma relação causal com os registos, e o seu ritmo depende do tempo físico. Contá-lo faria *L* e *V* avançarem só com a passagem do tempo e quebraria a igualdade *V*[*k*] = `seq`, que permite ler a posição de um registo na partição.

Quem o demonstra é a verificação *V*[*k*] = `seq` do `CausalCheck`, herdada da COMINDIR03. Nos ensaios corretos, assinalou 0 registos inválidos. Na segunda falha, com a guarda do líder ativa, o próprio líder rejeitou as 30 leituras. Sem a guarda, o `CausalCheck` assinalou 32 violações em 32 registos. As 0 violações de Lamport nesse ensaio mostram que a verificação de *L* não bastaria para o detetar.

## Detetar não é recuperar

**Leituras enviadas a p1.** Perdem-se sem deixar vestígio, porque não há líder a subscrever o tópico nem o IPFS as retém. Em 8(c) foram publicadas 283 leituras para p1 e registadas 153: **perderam-se 130 (46 %)**, além de 34 alarmes dirigidos a p1. A suspeita não evita a perda, porque só surge 10,6 a 12,7 s depois da falha e o produtor limita-se a avisar. Nos ensaios, esse aviso nem chegou a ocorrer, porque as leituras foram enviadas antes da suspeita. Os registos já atribuídos mas ainda pendentes foram recuperados depois do reinício, porque estavam no ficheiro do líder.

**O que seria necessário para outro processo assumir p1:**

- **Acordo sobre um único líder.** Uma eleição ou consenso, já que as suspeitas surgem em instantes diferentes (em 8(c), 10,62 a 11,56 s na primeira repetição) e podem ser falsas. Em 8(d), um líder dado como falhado voltou a funcionar.
- **Exclusão do líder antigo.** Um número de época ou mandato crescente, com rejeição das mensagens de épocas anteriores. Sem isto, o líder de 8(d) continuaria a atribuir `seq` a par do novo.
- **Estado a partir de um quórum.** O novo líder precisa de conhecer o último `seq` confirmado. Hoje, os registos pendentes só existem no ficheiro do líder: em 8(a), nenhuma réplica tinha o registo 3, e uma réplica promovida reutilizaria esse `seq`. Seria preciso replicar antes de confirmar.
- **Retenção pelo produtor.** Para não perder leituras, o produtor teria de as guardar e reenviar até receber confirmação, com tratamento de duplicados.

Este mecanismo **não cabe no âmbito desta ficha**, que exclui eleição, consenso, *failover* e reenvio pelo produtor. A solução desta ficha deteta o problema, mas não o resolve.

## Medição e relógios físicos

A latência de lote subtrai ao instante de `RECEBIDO` no participante o `instante` atribuído pelo líder. O tempo de deteção subtrai ao instante de `SUSPEITA` o instante anotado em `falhas.txt`. A hipótese que os torna válidos é a de **todos os processos correrem na mesma máquina e lerem o mesmo relógio**, como a ficha exige e como foi feito nestes ensaios. Em máquinas diferentes, o desvio entre relógios somar-se-ia diretamente a estes valores, e a ficha exclui a sua sincronização.

O detetor não depende desta hipótese porque nunca compara instantes de processos diferentes. Mede, com `System.nanoTime()` no próprio participante, o tempo decorrido desde a última receção. Esse relógio é monótono e não é afetado por acertos à hora do sistema, ao contrário de `System.currentTimeMillis()`. O valor absoluto não tem significado entre processos, mas o detetor só usa diferenças locais.
