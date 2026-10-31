# TCP01 · Comunicação Baseada em Streams e Serialização de Objetos

Trabalho de Sistemas Distribuídos (SD 2026/2027) sobre comunicação TCP com sockets em Java e serialização automática de objetos.

## 1. Estrutura do trabalho

Dois projetos separados simulam duas máquinas distintas. Todas as classes estão no pacote `tcp01` e o servidor arranca sempre primeiro.

```
tcp01-servidor/
  src/tcp01/TCPServer.java
  src/tcp01/Connection.java
  src/tcp01/Person.java
  src/tcp01/Place.java
tcp01-cliente/
  src/tcp01/TCPClient.java
  src/tcp01/Person.java    (idêntica à do servidor)
  src/tcp01/Place.java     (idêntica à do servidor)
```

`Person` e `Place` têm de ser idênticas nos dois projetos: mesmo pacote, mesmo nome e mesmo `serialVersionUID`.

## 2. Como compilar e executar

Em cada projeto, apagar a pasta `out` e compilar de raiz. Não pode haver ficheiros de cópia (por exemplo `Person - OLD.java`) dentro de `src/tcp01`, porque o `javac` compila tudo o que termina em `.java`.

```
rmdir /s /q out
javac -d out src/tcp01/*.java
```

1. No projeto servidor: `java -cp out tcp01.TCPServer` (fica parado, bloqueado no `accept()`).
2. No projeto cliente: `java -cp out tcp01.TCPClient`.

Resultado esperado no cliente: `Received: Localidade: Figueira da Foz`.

## 3. Implementação

### 4.1 Strings com Data streams
Comunicação base com `DataInputStream` e `DataOutputStream` no porto 7896. O servidor devolve a mesma string (eco).

### 4.2 Enviar um objeto
A `Person` (`name`, `year`) implementa `Serializable` com `serialVersionUID` explícito e existe nos dois projetos. O cliente envia o objeto com `ObjectOutputStream`; o servidor lê com `ObjectInputStream`, faz o cast para `Person` e trata `ClassNotFoundException` e `ClassCastException`.

Regra essencial: criar o `ObjectOutputStream`, fazer `flush()` e só depois criar o `ObjectInputStream`. O construtor do `ObjectInputStream` bloqueia até receber o cabeçalho do outro lado, e esse cabeçalho só sai do `ObjectOutputStream` com `flush()`. Sem isto, os dois lados ficam em deadlock.

### 4.3 Objeto com dependências
Criação do `Place` (`postalCode`, `locality`) e da referência `private Place place;` na `Person`, com o construtor `Person(String name, Place place, int year)`. O cliente escreve apenas a `Person`; o `Place` viaja automaticamente porque é `Serializable` e existe nos dois projetos. O servidor devolve a localidade, o que prova a serialização automática.

Código-chave do cliente:

```java
out.writeObject(new Person("Ana", new Place("3080-000", "Figueira da Foz"), 2004));
out.flush();
String data = in.readUTF();
```

Código-chave da `Connection.run()`:

```java
Person p = (Person) in.readObject();
out.writeUTF("Localidade: " + p.getPlace().getLocality());
out.flush();
```

### 4.4 Variantes

| Variante | Alteração | Resultado | Lado da exceção |
|---|---|---|---|
| A | Método novo `describe()` na `Person` | Continua a funcionar | Nenhuma |
| B | `serialVersionUID` 2L no servidor e 1L no cliente | `InvalidClassException` (local class incompatible) | Servidor (no cliente, `EOFException` como consequência) |

Com o `serialVersionUID` declarado explicitamente, acrescentar métodos não altera a versão da classe. Sem ele, o Java calcularia o valor a partir da estrutura da classe e um método novo mudaria esse valor.

## 4. Pontos que bloqueiam

| Linha | À espera de |
|---|---|
| `listenSocket.accept()` (servidor) | Que um cliente complete a ligação TCP |
| `new Socket(...)` (cliente) | Que a ligação seja estabelecida (ou falha com `ConnectException`) |
| `new ObjectInputStream(...)` (ambos) | Do cabeçalho de stream do outro lado |
| `in.readObject()` (servidor) | Que o cliente envie um objeto completo |
| `in.readUTF()` (cliente) | Da resposta do servidor |

`writeObject` e `writeUTF` normalmente não bloqueiam, porque só copiam para o buffer do TCP.

## 5. Ponto 13 · Falhas introduzidas

| Falha introduzida | Lado | Exceção | Momento | O que permitiu concluir |
|---|---|---|---|---|
| Arrancar o cliente sem o servidor | Cliente | `ConnectException: Connection refused` | Ligação (`new Socket`) | Ninguém está à escuta no porto. É um problema de rede e de ordem de arranque, não de serialização. |
| Retirar `implements Serializable` do `Place` | Cliente | `NotSerializableException: tcp01.Place` | Escrita (`writeObject`) | A ligação estava boa. O Java percorre o grafo e falha no emissor. O servidor só vê `EOFException` como consequência. |
| `serialVersionUID` da `Person` diferente nos dois lados | Servidor | `InvalidClassException` (local class incompatible) | Leitura (`readObject`) | A versão é comparada no recetor. Os dois lados têm de declarar o mesmo valor. |
| `Person` num pacote diferente no servidor | Servidor | `ClassNotFoundException: tcp01.Person` | Leitura (`readObject`) | O pacote faz parte da identidade da classe. Só os dados viajam, não o código. |

### Como reproduzir a falha do pacote diferente
1. No projeto servidor, criar `src/tcp02` e mover a `Person.java` para lá, sem deixar cópia em `tcp01`.
2. Escrever `package tcp02;` e acrescentar `import tcp01.Place;` na `Person`.
3. Na `Connection`, acrescentar `import tcp02.Person;`.
4. Apagar `out`, compilar `src/tcp01/*.java src/tcp02/*.java` e arrancar servidor e cliente.

Resultado: o servidor mostra `Class: tcp01.Person` e o cliente `EOF: null`. Depois, repor o código original.

## 6. Ponto 14 · Construções usadas

| Construção | Onde a usou | O que ficou garantido | O que continua a não ser garantido |
|---|---|---|---|
| `ServerSocket` / `accept()` | `TCPServer` | Porto reservado; `accept()` só devolve com uma ligação TCP estabelecida e um `Socket` próprio. | Que o cliente envie dados, que sejam válidos ou que a ligação se mantenha. |
| `Connection extends Thread` | `Connection` | Cada cliente é atendido numa thread própria; um cliente lento não bloqueia os outros. | Ordem de execução entre threads; que o `run()` termine (o `readObject()` pode ficar bloqueado sem timeout). |
| `implements Serializable` | `Person`, `Place` | O objeto pode ser convertido em bytes, com os atributos serializáveis. | Que o recetor tenha a classe e a versão certas; campos `transient` não viajam. |
| `ObjectOutputStream` / `ObjectInputStream` | `TCPClient`, `Connection` | Objetos completos, na ordem de escrita, com o tipo preservado. | Ausência de deadlock se a ordem ou o `flush()` falharem; chega uma cópia, e o cast pode falhar. |
| `serialVersionUID` | `Person`, `Place` | Versão identificada explicitamente; alterações compatíveis não quebram a comunicação. | Que a estrutura seja realmente compatível, pois o número igual não garante campos iguais. |
| Referência para `Place` | Atributo `place` da `Person` | O `Place` é serializado automaticamente com a `Person`. | Que o `Place` seja `Serializable` e exista nos dois projetos. |

## 7. Problemas comuns encontrados

- **`class X is public, should be declared in a file named X.java`:** o nome do ficheiro tem de coincidir com o da classe pública (por exemplo `TCPCliente.java` contra `TCPClient`).
- **`duplicate class`:** cópias de segurança `- OLD.java` dentro de `src/tcp01` são compiladas também. Mover para uma pasta fora de `src`.
- **`invalid stream header: 000F6D65`:** o cliente antigo (que usa `writeUTF`) ligou-se ao servidor novo (que usa `ObjectInputStream`). `00 0F` é o tamanho da string e `6D 65` são as letras "me". Recompilar e correr a versão certa do cliente.
- **`Address already in use`:** há um servidor antigo ainda a correr no porto 7896.

## 8. Reflexão crítica (4.5)

### 8.1 Problemas que não eram de transmissão
O TCP garante bytes intactos e por ordem, mas quase tudo o que correu mal estava acima dele:
- o deadlock por falta de `flush()` entre os dois streams de objetos;
- nomes de ficheiros que não coincidiam com as classes e cópias duplicadas;
- cliente e servidor a falarem protocolos diferentes (`writeUTF` contra `ObjectInputStream`);
- classes diferentes, `serialVersionUID` diferente e pacote diferente;
- arrancar o cliente sem o servidor.

Estes problemas são de acordo entre as duas aplicações (protocolo, formato e versões) e de operação. Quem os resolve é o programador, ou a equipa que mantém as duas pontas. O TCP não sabe o que os bytes significam.

### 8.2 O que acontece quando o servidor muda a Person
Numa aplicação real, os clientes instalados noutras máquinas não são atualizados todos ao mesmo tempo. Se o servidor mudar a `Person` e o `serialVersionUID`, os clientes antigos deixam de comunicar e recebem ou provocam uma `InvalidClassException`. Se o servidor mudar a classe mas mantiver o mesmo `serialVersionUID`, a comunicação continua: campos novos ficam com o valor por omissão (`null` ou `0`) e campos removidos são ignorados.

O `serialVersionUID` funciona como contrato de versão. Mantém-se igual para alterações compatíveis (acrescentar campos ou métodos) e incrementa-se numa alteração que quebra a compatibilidade, para que a falha seja explícita em vez de produzir dados errados sem aviso. Mesmo assim, é preciso uma estratégia de atualização dos clientes.

### 8.3 Riscos de enviar o grafo completo
- **Volume:** um atributo que aponta para uma lista grande ou para uma estrutura com muitas referências arrasta tudo atrás de si, e o tamanho da mensagem pode crescer muito sem que o programador repare.
- **Informação enviada sem querer:** todos os atributos não `transient` viajam, incluindo dados sensíveis (palavras-passe, identificadores internos). Estes campos devem ser marcados `transient` ou excluídos com um objeto de transferência próprio.
- **Segurança:** desserializar dados de uma origem não confiável pode executar código indesejado. Convém restringir as classes aceites, por exemplo com `ObjectInputFilter`.

### 8.4 Uma thread por ligação contra um cliente de cada vez
Com uma thread por ligação, o servidor ganha concorrência: vários clientes são atendidos em paralelo, o `accept()` fica sempre livre e um cliente lento ou parado não bloqueia os outros. Passa a gastar memória (cada thread tem a sua pilha), tempo de CPU na criação e na troca entre threads, e precisa de sincronização se partilhar dados. Sem limite de threads, muitos clientes podem esgotar os recursos. Atender um cliente de cada vez é mais simples e barato, mas os restantes ficam em fila. Um meio-termo é um conjunto fixo de threads (*thread pool*).

### 8.5 Quando preferir datagramas (UDP)
Os datagramas são preferíveis quando a informação perde valor depressa e retransmitir seria pior do que perder. Um exemplo é um sensor que envia a temperatura todos os segundos: se uma leitura se perder, a seguinte já a substitui, e esperar pela retransmissão atrasaria as leituras novas. Outros casos são pedidos curtos de pergunta e resposta, como as consultas DNS, e mensagens de descoberta na rede local. Perde-se a entrega garantida, a ordem e a deteção de duplicados, que a aplicação tem de tratar se precisar delas.

## 9. Entrega

- Compilar os dois projetos de raiz (pasta `out` apagada).
- Não incluir as pastas `out` e `bin`.
- `git add .`, `git commit -m "TCP01"` e `git push`.
