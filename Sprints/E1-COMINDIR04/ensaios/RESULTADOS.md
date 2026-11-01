# COMINDIR04 · Resultados dos ensaios

Valores medidos a 2026-11-01. Os ficheiros de cada ensaio estão na pasta com o mesmo nome.

## Condições de execução

- Todos os processos na mesma máquina (Windows 11, Java 27), com um único nó Kubo 0.39.0 sem ligação à rede pública.
- Arranque pela ordem da ficha: réplicas, líderes, processador e produtor. Réplica 1 com p0 e p1, réplica 2 com p1 e p2, réplica 3 com p2 e p0.
- N = 3, Δ = 5000 ms, `FATOR_TIMEOUT` = 3 e `VERIFICACAO_MS` = 1000 ms, salvo indicação em contrário.
- `kill -9` substituído por `taskkill /F` e `kill -STOP`/`-CONT` substituídos por PsSuspend, aplicado ao processo da JVM.
- `rajada 300 10`: 300 leituras de 10 sensores, o mais depressa possível, conforme a definição da COMINDIR02. O CA3 fala em «intervalo de 10 ms»; seguiu-se o comando da secção 4 e cada rajada demorou entre 0,6 e 1,4 s.
- «Produtor parado»: processo em execução, sem lhe introduzir leituras.

## 8 · Ensaios

### (a) Figura 1 · pasta `8a`

- Os registos 1 e 2 seguiram no `LOTE` hb=3. O líder de p0 terminou depois de atribuir o seq 3 e o líder seguinte arrancou já depois da suspeita.
- `SUSPEITA` em todos os participantes, com tempo de deteção entre 15,12 e 15,38 s, dentro de ]10 s, 16 s]. O valor é alto porque a falha foi provocada logo após um heartbeat.
- Depois do arranque: `REINICIO`, `RETOMA` e `RECUPERACAO;p0;de=3;ate=3` nas réplicas 1 e 3, ambas com os 3 registos e ficheiros iguais aos do líder.

### (b) Sem falhas · pastas `8b-rajada` e `8b-parado`

Ver a tabela do ponto 10. Sem suspeitas, `HB_PERDIDO` ou recuperações; réplicas iguais aos líderes; `CausalCheck` sem violações de V[k] = seq.

### (c) Falha do líder de p1 durante a rajada · pasta `8c`

| Repetição | Tempo de deteção (s) | Registos recuperados por TCP |
| --- | --- | --- |
| 1 | 10,62 a 11,56 | 1 a 53 |
| 2 | 12,29 a 12,53 | 54 a 105 |
| 3 | 12,49 a 12,74 | 106 a 153 |

Todos os tempos cabem em ]10 s, 16 s]. Foram publicadas 283 leituras para p1 e o líder só registou 153: **perderam-se 130 leituras (46 %) sem deixar vestígio**. Perderam-se também os 34 alarmes dirigidos a p1. Os registos atribuídos mas ainda pendentes foram todos recuperados, e os lotes de p1 ficaram todos vazios porque a falha ocorreu antes do lote seguinte.

### (d) Suspensão do líder de p2 durante cerca de 20 s · pasta `8d`

- **Suspeitas:** 4, uma por participante, todas **falsas**, com 15,1 a 15,5 s de silêncio. As 4 `RETOMA` trazem o mesmo `inc`.
- **`hb-p2.txt` após a retoma:** intervalo de 25 557 ms, seguido de 4 heartbeats em 6 a 9 ms cada (recuperação do atraso pelo `scheduleAtFixedRate`), 4414 ms até realinhar e depois cerca de 5000 ms.
- **Leituras introduzidas:** as 5 leituras não se perderam. Ficaram por tratar durante a pausa, foram registadas na retoma (seq 1 a 5) e seguiram no `LOTE` hb=4. O campo `instante` indica o tratamento pelo líder, não o envio.

### (e) Variação de Δ · pastas `8e-*`

Ver a segunda tabela do ponto 10.

## 9 · Falhas deliberadas

Introduzidas numa cópia temporária do código, sem alterar o repositório.

| Falha introduzida | Lado onde surgiu | Detetada por `HeartbeatCheck` / `CausalCheck`? | O que permitiu concluir |
| --- | --- | --- | --- |
| Líder que só publica o `LOTE` quando `pendentes` não está vazia (`9-f1`) | Líder; observada nos participantes | `HeartbeatCheck`: sim, 1 lote por partição e 24 suspeitas (12 antes do primeiro heartbeat, 12 «reais»). `CausalCheck`: não | O silêncio volta a ser ambíguo. O líder estava vivo, mas sem novo heartbeat a suspeita é classificada como real |
| Líder que incrementa *L* e *V*[*k*] em cada heartbeat (`9-f2`, `9-f2b-sem-guarda`) | Líder | Com a guarda *V*[*k*] = `seq`: o próprio líder rejeita as 30 leituras e nenhuma verificação encontra registos. Sem a guarda: `CausalCheck` assinala 32 violações em 32 registos. `HeartbeatCheck`: não | Heartbeats não são eventos: contá-los quebra a igualdade *V*[*k*] = `seq` |
| Líder que lê `ultimoSeq` fora da secção sincronizada (`9-f3`, `9-f3b-janela-50ms`) | Líder; observada nas réplicas | Tal como pedido: não detetada em 1800 leituras. Com a janela alargada para 50 ms: `HeartbeatCheck` mostra 20 `RECUPERACAO` sem perdas. `CausalCheck`: não | É uma corrida rara: o `ultimoSeq` pode anunciar um registo que ainda não vai no lote e causar pedidos TCP desnecessários. Os ficheiros convergem |
| Líder que envia sempre `ultimoSeq` = 0 e descarta um em cada dez lotes (`9-f4`) | Líder; observada nas réplicas | `HeartbeatCheck`: sim, 48 `HB_PERDIDO`. `CausalCheck`: não, porque lê os ficheiros do líder | As réplicas divergem: 15 de 20 registos em ficheiro em p0, 20 de 20 em p1 e 9 de 20 em p2. Sem `ultimoSeq`, uma lacuna só é pedida com mais de 10 registos retidos: em p1 o `seq` 4 foi recuperado, mas em p0 e p2 ficaram retidos 4 e 10 registos e o pedido nunca foi feito |
| Participantes executados com `rep04.fator` = 1 (`9-f5`, `9-f5-atraso-7s-fator1`, `9-f5-atraso-7s-fator3`) | Participantes | `HeartbeatCheck`: 0 suspeitas em cerca de 5 min sem perturbação (intervalo máximo de 5016 ms). Com um atraso de 7 s no líder de p0: 4 falsas suspeitas com fator 1 e 0 com fator 3. `CausalCheck`: não | Com `TIMEOUT` = Δ não há margem: qualquer atraso superior a Δ gera falsa suspeita. O efeito é o preço de uma deteção mais rápida |

## 10 · Tabelas

### Δ = 5000 ms (ensaios 8(b) e 8(c))

| Métrica | Valor |
| --- | --- |
| Lotes publicados por partição (p0 / p1 / p2) | Rajada, cerca de 20 s: 5 / 5 / 5. Produtor parado, cerca de 62 s: 13 / 13 / 13 |
| Registos por lote (média / máximo) | 22,1 / 66 |
| Proporção de lotes vazios (rajada / produtor parado) | 40 % (janela de cerca de 20 s) / 100 % |
| Intervalo entre heartbeats (média / máximo, ms) | Rajada: 4927 / 4999. Produtor parado: 4976 / 5006 |
| Latência de lote (média / máximo, ms) | 2520 / 5004 |
| Tempo de deteção (mínimo / médio / máximo, s) | 10,62 / 11,99 / 12,74 |
| Falsas suspeitas | 0 em (b) e (c); 4 em (d), resultantes de uma única pausa |
| Mensagens no tópico de dados: COMINDIR03 / COMINDIR04 | 331 / 15 |
| Bytes por registo: `REG` individual / em `LOTE` | 55,3 / 38,5 |

A proporção de lotes vazios depende da duração da janela observada. A média do intervalo fica abaixo de Δ porque inclui o intervalo entre o primeiro e o segundo heartbeat, encurtado pelo arranque do processo. Os valores da COMINDIR03 são calculados, não medidos: um `REG` por registo, com o formato dessa ficha, aplicado aos mesmos registos.

### Variação de Δ (ensaio 8(e), uma execução por valor)

| Δ (ms) | Lotes por minuto e partição | Proporção de lotes vazios | Latência de lote média (ms) | Tempo de deteção (s) | Falsas suspeitas |
| --- | --- | --- | --- | --- | --- |
| 1000 | 63 | Rajada: 79,5 %. Parado: 100 % | 393 | 3,31 a 3,74 (média 3,57); previsto ]2, 4] | 0 |
| 5000 | 13 | Rajada: 40,0 %. Parado: 100 % | 2882 | 10,51 a 10,88 (média 10,76); previsto ]10, 16] | 0 |
| 10000 | 7 | Rajada: 45,5 %. Parado: 100 % | 5694 | 25,67 a 25,77 (média 25,74); previsto ]20, 31] | 0 |

Os lotes por minuto foram contados numa janela de cerca de 62 s, por isso incluem lotes além dos 60, 12 e 6 esperados por minuto. A janela da rajada aumenta com Δ, o que torna as proporções de lotes vazios pouco comparáveis entre linhas. Como cada rajada dura cerca de 1 s, a latência depende do momento em que cai dentro do período.

## Limites destas medições

- O aviso `AVISO: líder ... suspeito` não ocorreu em nenhum ensaio: as leituras foram todas enviadas antes de surgir a suspeita. O código existe, mas estes ensaios não o exercitam.
- A falha `9-f3`, tal como pedida, não se manifestou nesta amostra, o que não prova que a corrida não possa ocorrer.
- 8(c) tem três repetições e 8(e) uma por valor de Δ: os resultados ilustram o comportamento, sem valor estatístico.
