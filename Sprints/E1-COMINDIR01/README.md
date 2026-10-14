# E1-COMINDIR01 — Comunicação Indireta

## 1. Objetivo

Implementar e testar a replicação de registos de sensores entre um processo líder e vários processos réplica, utilizando dois mecanismos de comunicação indireta:

- **Parte A:** IP Multicast
- **Parte B:** IPFS PubSub

O líder recebe leituras de sensores, atribui-lhes um número de sequência e guarda-as num ficheiro local. As réplicas recebem os registos e guardam-nos pela ordem correta.

## 2. Estrutura dos projetos

A implementação está organizada em dois projetos Java independentes:

**`rep01-lider`**
- `SensorRecord.java` — representação dos registos dos sensores.
- `RecordFile.java` — operações de leitura e escrita do ficheiro.
- `MulticastLeader.java` — líder com comunicação IP Multicast.
- `IpfsPubSub.java` — comunicação com a API HTTP do IPFS.
- `PubSubLeader.java` — líder com comunicação IPFS PubSub.

**`rep01-replica`**
- `SensorRecord.java` — representação dos registos.
- `RecordFile.java` — operações de persistência.
- `MulticastReplica.java` — réplica IP Multicast.
- `IpfsPubSub.java` — comunicação com a API HTTP do IPFS.
- `PubSubReplica.java` — réplica IPFS PubSub.

## 3. Funcionamento

Cada registo contém um número de sequência, o identificador do sensor, a temperatura e a data/hora.

Exemplo:

`1;S01;22.5;2026-10-14T03:20:29`

O líder guarda cada registo no ficheiro `lider.txt` e publica-o através do mecanismo de comunicação escolhido.

Cada réplica guarda os registos recebidos no ficheiro `replica-<id>.txt`.

Para preservar a ordem, as réplicas verificam os números de sequência. Quando recebem um registo posterior ao esperado, mantêm-no pendente até receberem o registo em falta.

Não foi implementado um mecanismo automático de recuperação de mensagens perdidas.

## 4. Compilação

É necessário ter o Java instalado.

Para compilar o projeto líder:

```bash
cd rep01-lider
mkdir -p out
javac -d out src/rep01/*.java
```

Para compilar o projeto réplica:

```bash
cd rep01-replica
mkdir -p out
javac -d out src/rep01/*.java
```

## 5. Parte A — IP Multicast

O IP Multicast permite ao líder enviar mensagens para um grupo multicast, ao qual as réplicas aderem.

As mensagens incluem números de sequência para que as réplicas possam verificar a ordem dos registos.

O comando `rajada <n>` permite gerar e enviar várias leituras consecutivas, sendo útil para os testes de desempenho.

### 5.1. Execução

O líder utiliza o grupo multicast `230.0.0.1` e a porta `6789`.

Para iniciar as três réplicas, executar em três terminais diferentes, a partir da pasta `rep01-replica`:

```bash
java -cp out rep01.MulticastReplica 1
java -cp out rep01.MulticastReplica 2
java -cp out rep01.MulticastReplica 3
```

Para iniciar o líder, a partir da pasta `rep01-lider`:

```bash
java -cp out rep01.MulticastLeader
```

O líder permite introduzir leituras manualmente ou gerar várias leituras através do comando `rajada <n>`.

### 5.2. Testes com três réplicas

Foram realizadas três rajadas sucessivas de 1000 registos, mantendo o líder e as três réplicas em execução.

Antes dos testes, foram terminados os processos Multicast antigos e os ficheiros de registos foram reinicializados, para evitar interferências de execuções anteriores.

| Rajada | Duração no líder | Total acumulado por ficheiro | Resultado                     |
|--------|-----------------:|-----------------------------:|-------------------------------|
| 1      | 552 ms           | 1000                         | Três réplicas iguais ao líder |
| 2      | 344 ms           | 2000                         | Três réplicas iguais ao líder |
| 3      | 442 ms           | 3000                         | Três réplicas iguais ao líder |

**Tempo médio:** 446 ms por rajada de 1000 registos.

Após cada rajada, foi verificada a quantidade de linhas dos ficheiros e realizada uma comparação do seu conteúdo.

No final, o líder e as três réplicas continham exatamente 3000 registos cada. As comparações confirmaram que os ficheiros eram idênticos.

Não foram detetadas perdas de mensagens nestas três experiências. No entanto, o IP Multicast não garante, por si só, a entrega das mensagens, pelo que este resultado não significa que não possam ocorrer perdas noutras condições.

Os resultados foram guardados nas pastas `resultados-testes-multicast/teste1`, `teste2` e `teste3`.

## 6. Parte B — IPFS PubSub

Foi utilizado o IPFS Kubo, com comunicação através da API HTTP RPC (`/api/v0`), utilizando `HttpClient` em Java.

### Execução

Antes de executar os programas, é necessário iniciar o daemon IPFS.

Num terminal, executar:

```bash
ipfs daemon --enable-pubsub-experiment
```

Para iniciar uma réplica:

```bash
cd rep01-replica
java -cp out rep01.PubSubReplica 1 5001
```

Para iniciar o líder:

```bash
cd rep01-lider
java -cp out rep01.PubSubLeader
```

Exemplos de comandos do líder:

```text
S01 22.5
rajada 1000
sair
```

### 6.1. Testes com um daemon e três réplicas

Foram realizadas três rajadas sucessivas de 1000 registos, com três réplicas ligadas ao mesmo daemon IPFS.

| Rajada | Duração no líder | Total acumulado por ficheiro |
|--------|-----------------:|-----------------------------:|
| 1      | 4610 ms          | 1000                         |
| 2      | 4255 ms          | 2000                         |
| 3      | 4162 ms          | 3000                         |

**Tempo médio:** 4342,33 ms por rajada.

Após as três rajadas, os ficheiros do líder e das três réplicas continham 3000 registos. A comparação dos ficheiros não apresentou diferenças.

### 6.2. Testes com dois daemons IPFS

Foi configurado um segundo daemon IPFS na porta API `5002`, enquanto o primeiro utilizava a porta `5001`.

Os dois daemons foram ligados através da rede swarm IPFS.

Configuração:
- Líder: daemon na porta 5001.
- Réplica 1: daemon na porta 5001.
- Réplicas 2 e 3: daemon na porta 5002.

Após um registo inicial, foram realizadas três rajadas sucessivas de 1000 registos.

| Rajada | Duração no líder | Total acumulado por ficheiro |
|--------|-----------------:|-----------------------------:|
| 1      | 7740 ms          | 1001                         |
| 2      | 7544 ms          | 2001                         |
| 3      | 7409 ms          | 3001                           |

**Tempo médio:** 7564,33 ms por rajada.

No final, os ficheiros do líder e das três réplicas continham 3001 registos e não apresentavam diferenças.

### 6.3. Validação final dos projetos reorganizados

Após integrar as classes IPFS nos dois projetos finais, foi realizado um teste adicional.

O líder publicou:

```text
1;S01;22.5;2026-10-14T03:20:29
```

A réplica confirmou:

```text
APLICADO: 1;S01;22.5;2026-10-14T03:20:29
```

A comparação com `diff` confirmou que os ficheiros `lider.txt` e `replica-1.txt` eram idênticos, contendo um registo cada.

## 7. Observações

Os testes IPFS com três réplicas demonstraram que, nas condições experimentadas, os registos chegaram às réplicas pela ordem necessária para produzir ficheiros iguais ao do líder.

Os testes com dois daemons apresentaram um tempo médio de publicação superior ao dos testes com um daemon. Esta diferença foi observada neste ambiente de teste e não deve ser generalizada sem mais experiências.

Como não existe recuperação automática de mensagens, uma réplica que não receba determinado número de sequência pode ficar com registos posteriores pendentes.


## 8. Comparação e reflexão crítica
### 8.1. Desempenho

Nos testes realizados, o IP Multicast apresentou um tempo médio de envio de 446 ms por rajada de 1000 registos, enquanto o IPFS PubSub apresentou 4342,33 ms com um daemon e 7564,33 ms com dois daemons.

O débito médio calculado a partir destes tempos foi de aproximadamente 2242 registos/s no Multicast, 230 registos/s no PubSub com um daemon e 132 registos/s no PubSub com dois daemons.

O Multicast apresentou, portanto, maior débito de envio. Neste mecanismo, o líder envia datagramas UDP diretamente para o grupo multicast, sendo a distribuição efetuada pela rede.

No IPFS PubSub, cada publicação passa pela API HTTP do Kubo e pelo processamento do daemon. No cenário com dois nós, as mensagens também têm de ser propagadas entre os daemons através da rede IPFS, acrescentando processamento e comunicação.

Estes resultados representam os tempos de envio medidos no líder. Não permitem, por si só, determinar a latência completa entre o primeiro envio e a última receção numa réplica, que exige os instantes de receção registados nas réplicas.

### 8.2. Escalabilidade

No IP Multicast, o líder envia apenas um datagrama por registo para o endereço do grupo, independentemente do número de réplicas. Assim, passar de 3 para 300 réplicas não implica aumentar o número de envios realizados pelo líder.

A rede é responsável por distribuir as mensagens pelos membros do grupo. O tráfego total pode aumentar devido à distribuição pelas diferentes ligações, mas não é necessário que o líder envie uma cópia individual para cada réplica.

No IPFS PubSub, o líder publica cada registo no tópico e os nós IPFS propagam as mensagens através do protocolo gossip. Ao aumentar o número de réplicas e de nós, aumenta também o trabalho de propagação e o tráfego total da rede, embora cada nó comunique principalmente com um conjunto de vizinhos.

Quando as réplicas estão distribuídas pela Internet, o IP Multicast torna-se menos adequado, uma vez que os routers normalmente não encaminham tráfego multicast entre redes diferentes.

O IPFS PubSub é mais adequado a esse cenário, porque os nós comunicam através de ligações peer-to-peer que podem atravessar diferentes redes, desde que exista conectividade entre eles.

Nos nossos testes, o Multicast apresentou um tempo médio de envio de 446 ms, enquanto o IPFS PubSub apresentou 4342,33 ms com um nó e 7564,33 ms com dois nós. Estes resultados mostram o desempenho observado no ambiente testado, mas não permitem prever diretamente o comportamento com 300 réplicas, uma vez que essa escala não foi testada.

### 8.3. Fiabilidade

Tanto o IP Multicast como o IPFS PubSub permitem distribuir mensagens por várias réplicas, mas nenhum dos dois mecanismos garante, por si só, que todas as mensagens sejam recebidas e aplicadas.

No IP Multicast, a comunicação utiliza UDP, pelo que podem ocorrer perdas de datagramas, duplicações ou alterações na ordem de chegada. O protocolo não disponibiliza confirmações de receção nem retransmissões automáticas.

No IPFS PubSub, a propagação das mensagens depende da conectividade entre os nós, das subscrições ativas e do funcionamento dos daemons. Uma réplica que esteja desligada ou que ainda não tenha subscrito o tópico pode não receber mensagens publicadas anteriormente.

Na implementação desenvolvida, cada registo possui um número de sequência. As réplicas utilizam esse número para identificar registos duplicados ou fora de ordem e só escrevem no ficheiro quando recebem a sequência esperada.

Quando existe uma falha na sequência, os registos posteriores ficam pendentes em memória. Este mecanismo preserva a ordem dos registos aplicados, mas não recupera automaticamente mensagens perdidas.

Nos testes de desempenho realizados, os ficheiros das três réplicas ficaram idênticos ao do líder. Contudo, esse resultado não constitui uma garantia de entrega em todas as condições de funcionamento.

### 8.4. Recuperação de mensagens

Quando uma réplica perde uma mensagem, o número de sequência permite identificar a existência de uma falha, mas não permite recuperar automaticamente o registo em falta.

Por exemplo, se uma réplica receber os registos 1, 2, 4 e 5, consegue guardar os registos 1 e 2, mas mantém os registos 4 e 5 pendentes enquanto aguarda pelo registo 3.

Na implementação desenvolvida, não existem pedidos de retransmissão, confirmações de receção ou mecanismos de sincronização automática com o líder. Assim, se o registo 3 nunca chegar, os registos seguintes permanecem pendentes e não são escritos no ficheiro.

Uma possível melhoria seria implementar um mecanismo de recuperação baseado nos números de sequência, em que a réplica pudesse solicitar ao líder os registos em falta. Outra alternativa seria permitir a sincronização a partir do ficheiro persistente do líder quando uma réplica regressasse à rede.

Estas melhorias exigiriam um canal adicional de comunicação e controlo, aumentando a complexidade da solução. Não foram implementadas nesta ficha, de acordo com o âmbito definido para a experiência.

### 8.5. Dependências e alcance

O IP Multicast depende do suporte da infraestrutura de rede para a distribuição das mensagens. As réplicas têm de conseguir aderir ao grupo multicast e os equipamentos de rede devem permitir a circulação desse tráfego.

Numa rede local com suporte multicast, este mecanismo permite distribuir eficientemente a mesma mensagem por vários destinatários. Contudo, a sua utilização através da Internet é limitada, porque o encaminhamento multicast entre redes diferentes não está normalmente disponível.

O IPFS PubSub depende da execução dos daemons IPFS, da conectividade entre os nós e da subscrição do mesmo tópico. Para comunicar entre redes diferentes, os nós têm de conseguir estabelecer ligações peer-to-peer, diretamente ou através dos mecanismos de conectividade suportados pelo IPFS.

Ao contrário do IP Multicast, o IPFS PubSub não necessita de suporte multicast nos routers. Por isso, é mais adequado a cenários distribuídos por diferentes redes e localizações geográficas.

Nos testes realizados, o IP Multicast funcionou numa rede local, enquanto o IPFS PubSub foi validado com um e dois daemons ligados através da rede swarm. Não foram realizados testes entre máquinas fisicamente separadas pela Internet.

### 8.6. Ponto único de falha

Na solução implementada, existe apenas um líder responsável por receber novas leituras, atribuir números de sequência e guardar os registos no ficheiro local.

Se o líder deixar de funcionar, as réplicas deixam de receber novos registos. Os ficheiros existentes mantêm os dados anteriormente guardados, mas não são efetuadas novas escritas enquanto não existir um líder operacional.

Nem o IP Multicast nem o IPFS PubSub resolvem automaticamente este problema, porque ambos são mecanismos de comunicação e não de coordenação entre processos.

Se duas máquinas pudessem registar leituras em simultâneo, seria necessário coordenar a atribuição dos números de sequência e a ordem das escritas. Caso contrário, os dois processos poderiam atribuir o mesmo número a registos diferentes, provocando inconsistências entre as réplicas.

A escolha automática de um novo líder e a coordenação de múltiplos processos responsáveis pelas escritas exigiriam mecanismos adicionais, como eleição de líder e consenso distribuído, que estão fora do âmbito desta ficha.

### 8.7. Segurança

Na implementação desenvolvida, as réplicas verificam o formato dos registos e os respetivos números de sequência, mas não verificam a identidade de quem enviou cada mensagem.

No IP Multicast, qualquer processo com acesso à rede e ao grupo `230.0.0.1:6789` pode enviar um datagrama com o formato esperado. Se o registo tiver um número de sequência válido, a réplica poderá aceitá-lo, mesmo que não tenha sido enviado pelo líder legítimo.

No IPFS PubSub, também é possível publicar mensagens no tópico `rep01-temperaturas` sem ser o líder da aplicação. As mensagens recebidas incluem o campo `from`, que identifica o nó IPFS responsável pela publicação e poderia ajudar a distinguir publicadores conhecidos de desconhecidos.

Contudo, o campo `from` não é atualmente utilizado pela aplicação para autorizar mensagens, nem comprova, por si só, que uma leitura foi produzida pelo líder legítimo.

Para melhorar a segurança, seria necessário validar a origem das mensagens antes de as aplicar ao ficheiro, recorrendo a mecanismos adequados de autenticação e verificação de integridade.

Estes mecanismos adicionais não foram implementados, uma vez que o objetivo da ficha é estudar a comunicação indireta e a replicação de registos.

### 8.8. Testes de falha e comportamento das réplicas

Para além dos testes de desempenho, importa analisar o comportamento dos mecanismos quando existem interrupções na comunicação ou quando uma réplica não recebe todos os registos.

Nos testes realizados com IPFS PubSub, foi observado que uma réplica iniciada depois da publicação dos primeiros registos não recupera automaticamente as mensagens anteriores. Como a réplica aguarda pelo primeiro número de sequência em falta, os registos posteriores podem ficar pendentes.

| Situação                                                   | Mecanismo             | Observação                                        |
|------------------------------------------------------------|-----------------------|---------------------------------------------------|
| Três réplicas ativas durante três rajadas de 1000 registos | IP Multicast          | Não foram detetadas perdas; os ficheiros ficaram idênticos |
| Três réplicas ativas durante três rajadas de 1000 registos | IPFS PubSub, um daemon| Não foram detetadas diferenças entre os ficheiros |
| Três réplicas distribuídas por dois daemons                | IPFS PubSub           | Os ficheiros ficaram idênticos após as três rajadas |
| Réplica iniciada depois de existirem registos publicados   | IPFS PubSub           | Registos anteriores não recuperados automaticamente; possibilidade de registos posteriores pendentes |

Os testes efetuados não demonstram tolerância a todas as falhas possíveis. Para avaliar interrupções do líder, dos daemons e da conectividade, seriam necessárias experiências adicionais e o registo dos resultados observados em cada cenário.

## 9. Conclusão

Foi implementada uma solução de replicação de registos de sensores utilizando IP Multicast e IPFS PubSub, com persistência local e controlo da ordem através de números de sequência.

Na Parte A, foram realizadas três rajadas de 1000 registos através de IP Multicast, com um tempo médio de 446 ms por rajada. As três réplicas mantiveram ficheiros idênticos ao do líder, sem perdas detetadas.

Na Parte B, os testes com um e dois daemons IPFS permitiram validar a receção dos registos pelas três réplicas e a consistência dos ficheiros. Os tempos médios observados foram de 4342,33 ms e 7564,33 ms, respetivamente.

A solução não garante recuperação automática de mensagens perdidas, sendo essa uma limitação importante do modelo implementado.
