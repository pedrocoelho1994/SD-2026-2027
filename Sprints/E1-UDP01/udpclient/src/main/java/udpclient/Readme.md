# Cliente UDP em Java

Este projeto implementa um cliente de rede baseado no protocolo **UDP (User Datagram Protocol)**. O cliente permite enviar mensagens em tempo real para um servidor local, suportando tanto o envio contínuo através do teclado como um mecanismo de controlo de ordenação de mensagens.
---

### Pré-requisitos
* **Java SDK** instalado (versão 8 ou superior).
* O **`UDPServer`** deve estar em execução primeiro para poder responder às mensagens.

### Passos para Correr
1. Abra o terminal na pasta onde guardou o ficheiro `UDPClient.java`.
2. Compile o ficheiro com o comando:
   ```bash
   javac UDPClient.java
   ```
3. Execute o programa:
   ```bash
   java -cp . udpclient.UDPClient na pasta anterior do projeto !!!!
   ```
---

## 🛠️ Como Funciona?

Assim que o programa inicia, ele pede para escolher o **Modo de Numeração**:

1. **Modo Automático:** O programa numera as mensagens sozinho de forma sequencial (1, 2, 3...). É o comportamento normal do dia a dia.
2. **Modo Manual:** O programa pede-lhe para digitar o número de sequência antes de cada mensagem. Ideal para testar falhas e enviar mensagens fora de ordem de propósito.

### Comandos Especiais
* Digite **`sair`** a qualquer momento (seja a pedir o número ou a mensagem) para fechar a aplicação em segurança.
---

## 📬 Formato das Mensagens

Para que o servidor consiga processar o controlo de erros, as mensagens são enviadas automaticamente em rede no formato:
`<Número_Sequência>,<Texto_da_Mensagem>`

### Respostas do Servidor
O cliente interpreta e traduz visualmente a resposta que vem do servidor:
* **`[ECHO SERVIDOR]`** Significa que a mensagem chegou na ordem certa e o servidor devolveu o texto com sucesso.
* **`[AVISO SERVIDOR]`** Significa que enviou um número fora de ordem e o servidor avisou que ficou bloqueado à espera do número correto.
