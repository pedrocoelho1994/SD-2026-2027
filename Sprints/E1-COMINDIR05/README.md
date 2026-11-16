
# E1-COMINDIR05 — Comunicação Indireta em Sistemas Distribuídos

## 1. Introdução

Este projeto implementa um sistema distribuído para receção, replicação e processamento de leituras de sensores de temperatura.

A solução utiliza comunicação indireta através do IPFS PubSub, permitindo que os componentes comuniquem por publicação e subscrição de mensagens.

O sistema está organizado em três partições, cada uma constituída por três nós. Em cada partição, os nós participam num mecanismo de eleição de líder inspirado no algoritmo Raft, assegurando a continuidade do funcionamento perante falhas de nós, desde que exista quórum.

A implementação inclui mecanismos de replicação de registos, recuperação de dados, monitorização de nós, relógios vetoriais e processamento de leituras para geração de alarmes.

## 2. Arquitetura do Sistema

O projeto encontra-se dividido em três módulos principais:

### 2.1. raft04-no

Responsável pela gestão das partições e dos respetivos nós.

Inclui:
- Eleição de líderes e gestão de termos.
- Comunicação entre nós e troca de mensagens de controlo.
- Replicação de leituras.
- Recuperação de registos em falta.
- Monitorização através de heartbeats.
- Persistência de registos.
- Verificação da consistência das réplicas.

### 2.2. raft04-produtor

Responsável pela produção e publicação de leituras de temperatura.

Inclui:
- Introdução de leituras de sensores.
- Determinação da partição de destino.
- Publicação de mensagens através do IPFS PubSub.
- Geração de rajadas de leituras para testes de carga.
- Monitorização do estado das partições.

### 2.3. raft04-processador

Responsável pelo processamento das leituras recebidas.

Inclui:
- Subscrição de mensagens através do IPFS PubSub.
- Processamento das leituras dos sensores.
- Deteção de condições de alarme.
- Publicação de eventos de alarme.
- Monitorização do funcionamento do sistema.

## 3. Organização das Partições

O sistema utiliza três partições, identificadas por:

- p0
- p1
- p2

Cada partição contém três nós:

- n1
- n2
- n3

No total, a configuração utilizada nos testes contém nove nós.

Em cada partição, a eleição de líder exige uma maioria de votos, correspondente a pelo menos dois dos três nós.

Se o líder deixar de estar disponível, os restantes nós podem iniciar uma nova eleição. Quando o quórum é restabelecido, o sistema pode voltar a eleger um líder e retomar o funcionamento.


## 4. Requisitos de Execução

Para compilar e executar o sistema, são necessários:

- Java JDK 21.
- IPFS Kubo com suporte para PubSub.
- Terminal para executar os diferentes componentes.
- Acesso à API local do IPFS.

Nos testes realizados, foi utilizado o Kubo IPFS 0.43.1.

A API do IPFS foi configurada no endereço:

`http://127.0.0.1:5001/api/v0`

### 4.1. Inicialização do IPFS

Antes de iniciar os componentes do sistema, é necessário garantir que o daemon IPFS está em execução.

Num terminal, executar:

```bash
ipfs daemon --enable-pubsub-experiment
```

O terminal do daemon deve permanecer aberto durante a execução do sistema.

## 5. Compilação

A compilação deve ser realizada a partir da pasta principal `E1-COMINDIR05`.

Os três módulos são compilados separadamente, uma vez que possuem classes com os mesmos nomes de pacote.

### 5.1. Preparação das pastas de saída

```bash
mkdir -p raft04-no/out raft04-produtor/out raft04-processador/out
```

### 5.2. Compilação dos nós

```bash
javac -encoding UTF-8 -d raft04-no/out \
  $(find raft04-no/src/raft04 -name "*.java" ! -name "PartitionLeader.java")
```

O ficheiro `PartitionLeader.java` corresponde a uma implementação independente que não é utilizada na configuração atual baseada em `PartitionNode`.

### 5.3. Compilação do produtor

```bash
javac -encoding UTF-8 -d raft04-produtor/out \
  raft04-produtor/src/raft04/*.java
```

### 5.4. Compilação do processador

```bash
javac -encoding UTF-8 -d raft04-processador/out \
  raft04-processador/src/raft04/*.java
```

Cada módulo gera os respetivos ficheiros compilados na sua pasta `out`.


## 6. Execução do Sistema

Antes de iniciar os componentes, deve confirmar-se que o daemon IPFS está em execução e que os três módulos foram compilados.

Todos os comandos seguintes devem ser executados a partir da pasta `E1-COMINDIR05`.

### 6.1. Inicialização dos nós

O sistema utiliza nove nós, distribuídos por três partições.

Cada nó deve ser executado num terminal independente.

#### Partição p0

Terminal 1:
```bash
java -cp raft04-no/out raft04.PartitionNode 0 1
```

Terminal 2:
```bash
java -cp raft04-no/out raft04.PartitionNode 0 2
```

Terminal 3:
```bash
java -cp raft04-no/out raft04.PartitionNode 0 3
```

#### Partição p1

Terminal 4:
```bash
java -cp raft04-no/out raft04.PartitionNode 1 1
```

Terminal 5:
```bash
java -cp raft04-no/out raft04.PartitionNode 1 2
```

Terminal 6:
```bash
java -cp raft04-no/out raft04.PartitionNode 1 3
```

#### Partição p2

Terminal 7:
```bash
java -cp raft04-no/out raft04.PartitionNode 2 1
```

Terminal 8:
```bash
java -cp raft04-no/out raft04.PartitionNode 2 2
```

Terminal 9:
```bash
java -cp raft04-no/out raft04.PartitionNode 2 3
```

Após a inicialização, os nós de cada partição participam no processo de eleição de líder.

Cada partição funciona de forma independente, mantendo o seu próprio líder e termo.

### 6.2. Inicialização do produtor

Num novo terminal, executar:

```bash
java -cp raft04-produtor/out raft04.Producer
```

O produtor permite introduzir leituras de sensores de temperatura.

Exemplo:

```text
S01 25.5
```

Neste exemplo:
- `S01` identifica o sensor.
- `25.5` representa a temperatura registada.

O produtor determina a partição de destino e publica a leitura através do IPFS PubSub.

### 6.3. Inicialização do processador

Num novo terminal, executar:

```bash
java -cp raft04-processador/out raft04.AlarmProcessor 3 http://127.0.0.1:5001/api/v0
```

O processador subscreve as mensagens necessárias ao processamento das leituras e à deteção de condições de alarme.

O primeiro argumento indica o número de partições e o segundo corresponde ao endereço da API IPFS.

### 6.4. Ordem recomendada de inicialização

A ordem de execução utilizada nos testes foi:

1. Iniciar o daemon IPFS.
2. Iniciar os nove nós das três partições.
3. Aguardar pela eleição dos líderes.
4. Iniciar o produtor.
5. Iniciar o processador.
6. Publicar leituras de temperatura.
7. Observar a replicação, o processamento e os eventos de alarme.


## 7. Testes e Validação

Foram realizados testes funcionais, de carga, de tolerância a falhas e de consistência dos dados.

### 7.1. Publicação e replicação de leituras

Foram publicadas leituras de temperatura através do produtor, utilizando o formato:

```text
S01 25.5
```

Foi observado o encaminhamento das mensagens para a partição correspondente e a replicação dos registos entre os nós.

### 7.2. Teste de carga — 300 leituras

Foi executada uma rajada de 300 leituras, utilizando o comando:

```bash
printf 'rajada 300 1\nsair\n' | java -cp raft04-produtor/out raft04.Producer
```

Resultados apresentados pelo produtor:

| Indicador | Resultado |
|---|---|
| Leituras publicadas | 300 |
| Partição p0 | 300 |
| Partição p1 | 0 |
| Partição p2 | 0 |
| Duração | 4039 ms |
| Taxa média aproximada | 74,3 leituras/s |

As leituras foram encaminhadas para a partição p0.

Os registos do líder permitiram observar a progressão sequencial até ao registo 318, incluindo os eventos da rajada.

### 7.3. Falha do líder

Foi simulada a interrupção do líder da partição p0, inicialmente identificado como n1.

Após a falha:
- Os nós restantes iniciaram um novo processo eleitoral.
- O nó n3 foi eleito líder no termo 113, com dois votos.
- Foi publicada uma nova leitura através do produtor.
- A leitura foi recebida e registada pelo novo líder, com sequência 319.
- O nó n2 recuperou o registo em falta.
- Após o reinício, o antigo líder n1 reconheceu o novo líder e recuperou o registo 319.

O teste demonstrou a continuidade do funcionamento com dois dos três nós disponíveis.

### 7.4. Perda de quórum

Foi simulada a indisponibilidade dos nós n2 e n3 da partição p0, mantendo apenas o nó n1 em funcionamento.

Durante o período observado:
- O nó n1 iniciou novas candidaturas.
- Não foi observada a eleição de um líder sem maioria.
- O sistema permaneceu sem quórum na partição p0.

Este comportamento é consistente com a exigência de uma maioria de dois votos numa partição de três nós.

### 7.5. Recuperação do quórum

Após a perda de quórum, o nó n2 foi reiniciado.

Com dois nós novamente disponíveis:
- O nó n2 reconheceu o termo 125.
- O nó n2 concedeu o seu voto ao candidato n1.
- O nó n1 passou a ser reconhecido como líder no termo 125.
- Foram retomados os heartbeats.

Posteriormente, o nó n3 foi reiniciado, reconhecendo o líder n1 e o termo 125.

### 7.6. Verificação da consistência

Após os testes, foi executado o verificador:

```bash
java -cp raft04-no/out raft04.RaftCheck 3 .
```

Resultados finais:

| Partição | Réplicas encontradas | Registos | Comparação |
|---|---|---:|---|
| p0 | 3/3 | 319 | Idêntico |
| p1 | 3/3 | 50 | Idêntico |
| p2 | 3/3 | 1 | Idêntico |

O verificador apresentou:

```text
RESUMO: 0 erro(s), 0 aviso(s)
Sem violacoes detetadas nos dados analisados (nao prova formal).
```

O produtor apresentou 316 publicações registadas e 316 correspondências aproximadas por sensor e temperatura, sem correspondências em falta.

Esta comparação é aproximada: leituras repetidas, execuções anteriores e alarmes impedem que o resultado constitua uma prova individual de entrega de todas as mensagens.

### 7.7. Conclusão dos testes

Nos cenários observados, o sistema demonstrou:
- Eleição e substituição de líderes.
- Publicação e replicação de leituras.
- Recuperação de registos em falta.
- Continuidade de funcionamento após a falha de um líder, mantendo o quórum.
- Ausência de eleição confirmada durante a perda de quórum.
- Recuperação do funcionamento após o restabelecimento da maioria.
- Consistência dos registos persistidos entre as três réplicas de cada partição.

Os resultados obtidos são favoráveis, embora não constituam uma prova formal de correção do protocolo.
