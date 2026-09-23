# Servidor UDP com Controlo de Ordenação

Este projeto implementa um servidor de rede baseado no protocolo **UDP (User Datagram Protocol)** em Java. Além de atuar como um serviço de *echo* (devolver a mensagem recebida), possui um mecanismo básico para verificar se as mensagens estão a chegar na **ordem correta**.

---
### Pré-requisitos
* **Java SDK** instalado (versão 8 ou superior).

### Passos para Correr
1. Abra o terminal na pasta onde guardou o ficheiro `UDPServer.java`.
2. Compile o ficheiro com o comando:
   ```bash
   javac UDPServer.java
   ```

3. Execute o programa:
   ```bash
   java -cp . udpserver.UDPServer na pasta anterior do projeto !!!!
   ```
   *(O servidor ficará ativo na porta **6789** à espera de conexões).*


---

## 🛠️ Regra de Decisão (Como Funciona?)

O servidor mantém internamente uma variável **`L`** (número da última mensagem recebida em ordem, que começa em `0`). Ao receber um pacote no formato `<N>,<Mensagem>`, ele avalia o número `N`:

* **Se `N` for igual a `L + 1`:** A mensagem está na ordem correta! O servidor atualiza o seu estado (`L = N`) e devolve a mensagem original como *echo*.
* **Se `N` for diferente de `L + 1`:** O pacote chegou fora de ordem (ou perdeu-se um pacote anterior). O servidor rejeita o texto e responde ao cliente com o aviso: `waitingfor,<L+1>`.

### 🛡️ Proteção contra Erros
O código inclui um bloco de proteção (`try-catch` interno). Se receber mensagens malformadas, vazias ou sem o formato correto, o servidor **não vai crashar nem desligar**. Ele envia um aviso de erro ao cliente e continua ativo a servir os pedidos seguintes.

---

## 🛑 Como Desligar
Como o servidor corre num ciclo infinito (`while (true)`), para terminar a sua execução basta pressionar as teclas **`Ctrl + C`** no terminal onde ele está a ser executado.
