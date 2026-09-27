package udpclient;
import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDPClient {

  public static void main(String args[]) {
    DatagramSocket aSocket = null;
    Scanner scanner = new Scanner(System.in);
    
    int proxAutomatico = 1; // Contador para o modo automático

    try {
      aSocket = new DatagramSocket();
      InetAddress aHost = InetAddress.getByName("localhost");
      int serverPort = 6789;

      System.out.println("Escolhe o modo de numeracao inicial:");
      System.out.println("1 - Automatico (1, 2, 3...)");
      System.out.println("2 - Manual (Define o numero a cada mensagem)");
      System.out.print("Opcao: ");
      int modo = Integer.parseInt(scanner.nextLine());

      System.out.println("\nCliente pronto. Escreve 'sair' para terminar.");

      while (true) {
        int numeroSequencia = 0;

        if (modo == 2) {
          System.out.print("Indica o numero de sequência (N): ");
          String nInput = scanner.nextLine();
          if (nInput.equalsIgnoreCase("sair")) break;
          numeroSequencia = Integer.parseInt(nInput);
        } else {
          numeroSequencia = proxAutomatico;
        }

        System.out.print("Escreve a mensagem: ");
        String mensagem = scanner.nextLine();
        if (mensagem.equalsIgnoreCase("sair")) break;

        // Formatação obrigatória: <N>,<Mensagem>
        String mensagemFormatada = numeroSequencia + "," + mensagem;

        byte[] m = mensagemFormatada.getBytes();
        DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);
        aSocket.send(request);

        // Se correu bem no modo automático, avança o contador
        if (modo == 1) {
          proxAutomatico++;
        }

        // Receber resposta
        byte[] buffer = new byte[1000];
        DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
        aSocket.receive(reply);

        String resposta = new String(reply.getData(), 0, reply.getLength());

        // Reconhecer respostas do tipo waitingfor
        if (resposta.startsWith("waitingfor,")) {
          String proximoEsperado = resposta.split(",")[1];
          System.out.println("[AVISO SERVIDOR] O servidor perdeu mensagens! Esta a espera do numero: " + proximoEsperado);
        } else {
          System.out.println("[ECHO SERVIDOR] Mapeado com sucesso: " + resposta);
        }
        System.out.println("------------------------------------");
      }

    } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
    } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
    } catch (NumberFormatException e) { System.out.println("Erro: Entrada numerica invalida. A fechar.");
    } finally { 
      if (aSocket != null) aSocket.close(); 
      scanner.close();
    }
  }
}
