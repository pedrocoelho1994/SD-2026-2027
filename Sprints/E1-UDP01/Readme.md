## 1. Diagnóstico Inicial e Enquadramento

### (a) O que é um datagrama e como é enviado e recebido?
Um datagrama é uma unidade autónoma e independente de informação que viaja pela rede. Contém dados e toda a informação de encaminhamento necessária (endereço IP e porto de origem e destino).
* **Envio:** O emissor encapsula os dados num objeto, define o destino e envia-o através de um socket. O sistema operativo despacha o pacote para a rede de forma isolada.
* **Receção:** O recetor abre um socket num porto específico e fica à escuta. Quando um datagrama chega a esse porto, o sistema operativo copia os dados para um buffer de memória disponibilizado pela aplicação.

### (b) Que garantias o UDP dá e, sobretudo, quais não dá?
* **Garantias que dá:** O UDP garante a integridade básica dos dados contidos no datagrama através de um mecanismo de *checksum*. Se o pacote chegar corrompido, o sistema operativo descarta-o silenciosamente. O UDP garante também a delimitação exata de mensagens (preservação das fronteiras do datagrama).
* **Garantias que NÃO dá:** Não garante a **entrega** (omissão), não garante a **ordenação** (mensagens podem chegar trocadas), não garante a **unicidade** (pode haver duplicação por retransmissões da rede) e não garante o **controlo de fluxo** (pode ocorrer *buffer overflow* se o recetor for mais lento que o emissor).

### (c) O que significa «resolver um problema ao nível da aplicação»?
Significa que, como a camada de transporte (UDP) não fornece nativamente mecanismos de fiabilidade, ordenação ou controlo de erros, a lógica do programa de software (o código que escrevemos no Cliente e Servidor) assume a responsabilidade de inspecionar os dados, manter variáveis de estado e coordenar a retransmissão ou rejeição de pacotes para obter o comportamento desejado.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## 2. Critérios de Aceitação e Respostas de Verificação

### CA1 · Modelo de Falhas e Desordenação

#### Origem da desordenação e exemplo prático
A desordenação nasce porque o UDP é um protocolo **sem ligação** (*connectionless*). Cada datagrama é tratado de forma totalmente independente pelos routers da rede. 
* **Exemplo:** Se o cliente enviar a Mensagem A e logo a seguir a Mensagem B, o Router 1 pode encaminhar a Mensagem A por um caminho congestionado ou mais longo, enquanto a Mensagem B apanha um caminho alternativo mais rápido. Como resultado, a Mensagem B chega ao destino antes da Mensagem A.

#### Respostas às perguntas de verificação:
* **Onde nasce a desordenação?** Nasce nos nós intermédios da rede (routers) que comutam os pacotes de forma independente. A característica do UDP que a torna possível é a **ausência de circuito/estado virtual** (falta de uma rota fixa estabelecida entre emissor e recetor).
* **Como detetar a desordenação?** O recetor não tem como saber que uma mensagem chegou fora de ordem a menos que a própria aplicação adicione metadados. Falta-lhe um **número de sequência**, informação que só o **emissor (cliente)** lhe pode dar ao embutir esse número nos dados enviados.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

### CA2 · Decisões da API de Datagramas

#### Respostas às perguntas de verificação:
* **Decisão de Portos:** O servidor precisa de um porto fixo e bem conhecido (`6789`) para que qualquer cliente saiba para onde direcionar o primeiro contacto. O cliente não precisa de um porto fixo; o sistema operativo atribui-lhe um porto aleatório livre (*ephemeral port*). Se o cliente for executado com o servidor desligado, ele enviará os pacotes com sucesso para a rede (uma vez que o UDP não faz validação de ligação), mas não receberá qualquer resposta e ficará eternamente bloqueado na instrução `receive()`.
* **Proteção contra lixo no Buffer do Cliente:** Se uma resposta curta chegar após uma longa, os bytes novos cobrem apenas o início do array. Imprimir `new String(reply.getData())` vai mostrar os dados novos colados aos restos da mensagem antiga. Para nos protegermos, usamos o construtor limitador: `new String(reply.getData(), 0, reply.getLength())`, garantindo que apenas os bytes realmente recebidos são convertidos.
* **Prevenção de lixo no Servidor:** No servidor, o problema é evitado ao instanciar um array de bytes limpo (`byte[] buffer = new byte[1000]`) **dentro** do ciclo `while(true)`, mesmo antes da chamada ao `receive()`. Além disso, ao responder, o servidor constrói o `DatagramPacket` usando explicitamente o comprimento real da string de resposta (`replyBuffer.length`), nunca enviando lixo residual.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

### CA3 · Protocolo, Estado e Regra de Decisão

#### Definição do Protocolo e Regra do Servidor
* **Regra de decisão:** "Se o número de sequência $N$ recebido for estritamente igual a $L+1$, o servidor aceita o pacote, atualiza o estado $L = N$ e devolve um *echo* da mensagem; caso contrário, rejeita a mensagem e responde `waitingfor,<L+1>`."
* **Estado mínimo:** O servidor guarda apenas a variável inteira **`L`** (Last ordered), inicializada a **`0`**. Significa que, antes de o programa arrancar, a última mensagem processada em ordem foi a "zero" e que a primeira mensagem válida esperada terá de ser a número `1`.

#### Respostas às perguntas de verificação:
* **Onde viaja o número de sequência?** Viaja **dentro** da mensagem, como parte da carga útil (*payload*), no formato de texto `<N>,<Mensagem>`. A consequência é que quem lê do outro lado é obrigado a fazer o *parsing* (separação de strings e conversão para inteiro) antes de processar o conteúdo.
* **Quantidade mínima de informação:** O servidor só precisa de recordar o valor de **`L`** (um único número inteiro). Como o protocolo exige uma sequência linear estrita ($1, 2, 3...$), saber o último número aceite é suficiente para deduzir matematicamente qual deve ser o próximo.
* **Estratégia para mensagens adiantadas (Descarte vs Armazenamento):** O nosso servidor adota a estratégia de **descartar** a mensagem adiantada e enviar imediatamente um pedido de retransmissão (`waitingfor`). A consequência para o cliente é que ele terá de reenviar essa mensagem mais tarde. *Armazenar temporariamente em memória exigiria gerir tabelas de buffers (janelas de receção) no servidor, aumentando a complexidade.*
* **Distinção de respostas no Cliente:** O cliente faz a distinção inspecionando o início do texto da resposta com `.startsWith("waitingfor,")`. Isto impõe a restrição de que o utilizador normal não deve enviar mensagens de texto que comecem exatamente com a palavra-chave reservada do protocolo, sob pena de gerar uma falsa interpretação visual.
* **Resiliência contra dados malformados:** O código que faz o processamento da mensagem está protegido por um bloco `try-catch` genérico dentro do ciclo. Se o cliente enviar dados sem vírgula ou lixo alfanumérico no lugar do número, ocorre uma exceção capturada pelo `catch`. O servidor responde com o aviso `waitingfor` atual e executa o `continue`, garantindo que o ciclo nunca quebra e o processo continua estável para o pedido seguinte.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

### CA4 · Demonstração e Interpretação

#### Execução de Cenários de Teste

##### Cenário 1: Sequência Normal (1, 2, 3)
1. Cliente envia `1,Mensagem A` $\rightarrow$ Servidor processa ($1 == 0+1$), define $L=1$, responde `Mensagem A`. Cliente exibe `[ECHO]`.
2. Cliente envia `2,Mensagem B` $\rightarrow$ Servidor processa ($2 == 1+1$), define $L=2$, responde `Mensagem B`. Cliente exibe `[ECHO]`.
3. Cliente envia `3,Mensagem C` $\rightarrow$ Servidor processa ($3 == 2+1$), define $L=3$, responde `Mensagem C`. Cliente exibe `[ECHO]`.

##### Cenário 2: Sequência Desordenada Provocada (1, 3, 2, 3)
1. Cliente envia `1,Primeira` $\rightarrow$ Servidor aceita ($1 == 0+1$), $L=1$. **Echo com sucesso**.
2. Cliente envia `3,Terceira` (Salto provocado no modo manual) $\rightarrow$ Servidor analisa ($3 \neq 1+1$). **Rejeita e envia** `waitingfor,2`. O cliente exibe `[AVISO SERVIDOR]`. O estado no servidor mantém-se $L=1$.
3. Cliente envia `2,Segunda` (Retransmissão do pacote em falta) $\rightarrow$ Servidor analisa ($2 == 1+1$). **Aceita**, atualiza o estado para $L=2$ e faz **Echo da mensagem**. *(Esta é a mensagem que prova a recuperação do sistema, pois demonstra o destrancar do protocolo).*
4. Cliente envia `3,Terceira` (Reenvio da mensagem adiantada) $\rightarrow$ Servidor analisa ($3 == 2+1$). **Aceita**, atualiza o estado para $L=3$ e faz **Echo da mensagem**. O fluxo voltou à normalidade.

#### Respostas às perguntas de verificação:
* **Qual mensagem prova a recuperação?** A mensagem **`2`** da segunda sequência. É ela que satisfaz a condição pendente do servidor ($L+1$), permitindo que o estado interno avance e o servidor volte a aceitar pacotes sequenciais.
* **Por que razão a desordenação teve de ser provocada?** Porque os testes foram efetuados em ambiente local (*localhost* / loopback). Em redes locais virtuais, o atraso de propagação é quase nulo e não existem caminhos alternativos nem perdas físicas, pelo que os datagramas chegam mecanicamente na mesma ordem em que foram enviados.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

### CA5 · Limites da Solução

#### Respostas às perguntas de verificação:

#### 1. Duplicação de Pacotes
Se o mesmo datagrama com número $N$ chegar duas vezes seguidas:
* Se for um duplicado da mensagem atual correta ($N == L+1$), o servidor processa a primeira, atualiza $L$. Quando a segunda cópia idêntica chegar imediamente a seguir, o servidor verá que $N \neq L+1$ (pois $L$ já avançou) e irá tratá-la erradamente como uma mensagem fora de ordem, respondendo com um `waitingfor` desnecessário.
* **Solução:** O servidor deve verificar se $N \le L$. Se o número for menor ou igual ao estado atual, o servidor identifica-o como um duplicado antigo e simplesmente descarta o pacote ou repete o *echo* sem alterar o estado.

#### 2. Perda Silenciosa de Pacotes
Se o cliente enviar a última mensagem e ela se perder na rede, o servidor nunca recebe nada e fica calado. Como o UDP não tem confirmações (ACKs) nem temporizadores, o cliente assume que correu tudo bem e a perda torna-se permanente e invisível para a aplicação.
* **Solução:** Implementar um mecanismo de Timeout no lado do cliente utilizando aSocket.setSoTimeout(tempo). Se o cliente não receber resposta em $X$ milissegundos, assume que houve perda e reenvia o pacote.

### 3. Múltiplos Clientes em Simultâneo
O design atual guarda apenas uma única variável global L no servidor. Se o Cliente X e o Cliente Y comunicarem ao mesmo tempo, as sequências numéricas vão misturar-se. O Cliente Y enviará o seu número 1, baralhando o contador que já ia a 4 para o Cliente X. O estado atual perde todo o sentido.
* **Solução:** O servidor tem de manter uma tabela de estados (um mapa em memória do tipo HashMap<String, Integer>). A chave do mapa será a combinação única do Endereço IP + Porto de cada cliente. Assim que um datagrama chega, o servidor extrai a origem, localiza o estado L específico daquele cliente e aplica a regra de decisão isoladamente.
