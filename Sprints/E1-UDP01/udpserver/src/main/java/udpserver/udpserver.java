package udpserver;
import java.net.*;
import java.io.*;

public class UDPServer{

  public static void main(String args[]) {
    DatagramSocket aSocket = null;
    // Estado do servidor: L é a última mensagem recebida em ordem (começa em 0)
    int L = 0; 

    try {
      aSocket = new DatagramSocket(6789);
      byte[] buffer = new byte[1000];
      System.out.println("Servidor UDP com controlo de ordenacao ativo na porta 6789...");

      while (true) {
        DatagramPacket request = new DatagramPacket(buffer, buffer.length);
        aSocket.receive(request);

        String dadosRecebidos = new String(request.getData(), 0, request.getLength());
        
        try {
          // Processar formato <N>,<Mensagem>
          int indexVirgula = dadosRecebidos.indexOf(",");
          if (indexVirgula == -1) {
            throw new IllegalArgumentException("Formato invalido");
          }

          int N = Integer.parseInt(dadosRecebidos.substring(0, indexVirgula));
          String mensagemCliente = dadosRecebidos.substring(indexVirgula + 1);

          // 👇 ESTA LINHA ADICIONADA MOSTRA O QUE CHEGOU NO TERMINAL DO SERVIDOR
          System.out.println(" Recebido pacote N=" + N + " | Mensagem: [" + mensagemCliente + "]");

          String respostaTexto;
          // Regra de decisão do servidor
          if (N != L + 1) {
            respostaTexto = "waitingfor," + (L + 1);
            System.out.println("Fora de ordem! Rejeitado. Enviado aviso: waitingfor," + (L + 1));
          } else {
            // Mensagem correta: atualiza o estado L e faz echo da mensagem original
            L = N;
            respostaTexto = mensagemCliente;
            System.out.println("Em ordem! Estado L atualizado para: " + L);
          }
          System.out.println("------------------------------------------------");

          byte[] replyBuffer = respostaTexto.getBytes();
          DatagramPacket reply = new DatagramPacket(replyBuffer, replyBuffer.length,
              request.getAddress(), request.getPort());
          aSocket.send(reply);

        } catch (Exception e) {
          System.out.println("Recebida mensagem errada ou inválida!");
          // Proteção contra mensagens malformadas para garantir que o servidor nunca pare
          String erroTexto = "Erro: Mensagem errada detetada pelo servidor.";
          byte[] errorBuffer = erroTexto.getBytes();
          DatagramPacket reply = new DatagramPacket(errorBuffer, errorBuffer.length,
              request.getAddress(), request.getPort());
          aSocket.send(reply);
        }
      }
    } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
    } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
    } finally { if (aSocket != null) aSocket.close(); }
  }
}
