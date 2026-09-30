package udpserver;

import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class udpserver {

    // Lista com as mensagens entregues na ordem correta
    private static final ArrayList<String> listaRececao = new ArrayList<>();
    // Buffer temporário para armazenar mensagens que chegaram adiantadas
    private static final HashMap<Integer, String> mensagensTemporarias = new HashMap<>();

    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {

        // 1. Duplicado (já entregue ou já retido em buffer)
        if (nCurrentMessage <= nLastMessageInOrder || mensagensTemporarias.containsKey(nCurrentMessage)) {
            System.out.println("Pacote duplicado ignorado: N=" + nCurrentMessage);
            return nLastMessageInOrder;
        }

        // 2. Mensagem em ordem: entrega atual + cascata
        if (nCurrentMessage == nLastMessageInOrder + 1) {
            listaRececao.add(currentMessage);
            nLastMessageInOrder = nCurrentMessage;
            System.out.println("Mensagem entregue em ordem: N=" + nCurrentMessage);

            // Cascata: esvaziar elementos consecutivos já retidos
            while (mensagensTemporarias.containsKey(nLastMessageInOrder + 1)) {
                int proximoN = nLastMessageInOrder + 1;
                String msgBuf = mensagensTemporarias.remove(proximoN);
                listaRececao.add(msgBuf);
                nLastMessageInOrder = proximoN;
                System.out.println("Cascata ativada: N=" + proximoN + " retirado do buffer.");
            }

            return nLastMessageInOrder;
        }

        // 3. Fora de ordem: reter no mapa sem avançar L
        mensagensTemporarias.put(nCurrentMessage, currentMessage);
        System.out.println("Fora de ordem: N=" + nCurrentMessage + " guardado. Esperando N=" + (nLastMessageInOrder + 1));
        return nLastMessageInOrder;
    }

    public static void main(String[] args) {
        DatagramSocket aSocket = null;
        int nLastMessageInOrder = 0; // Último número de sequência entregue em ordem (L)

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];
            System.out.println("Servidor UDP com reordenação ativo na porta 6789...");

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);

                String dadosRecebidos = new String(request.getData(), 0, request.getLength());

                try {
                    // Espera o formato <N>,<Mensagem>
                    int indexVirgula = dadosRecebidos.indexOf(",");
                    if (indexVirgula == -1) {
                        throw new IllegalArgumentException("Formato inválido");
                    }

                    int nCurrentMessage = Integer.parseInt(dadosRecebidos.substring(0, indexVirgula));
                    String currentMessage = dadosRecebidos.substring(indexVirgula + 1);

                    System.out.println("Recebido pacote N=" + nCurrentMessage + " | Conteúdo: [" + currentMessage + "]");

                    // Guarda o L anterior para aferir se houve entrega
                    int previousL = nLastMessageInOrder;

                    // Executa a lógica de entrega e cascata
                    nLastMessageInOrder = processDeliveredMessages(nLastMessageInOrder, nCurrentMessage, currentMessage);

                    // Construção da resposta segundo o protocolo:
                    // - Se L avançou (houve entrega): echo da mensagem recebida
                    // - Se L não se alterou (sem entrega: duplicado ou retido): waitingfor,<L+1>
                    String respostaTexto;
                    if (nLastMessageInOrder > previousL) {
                        respostaTexto = dadosRecebidos;
                    } else {
                        respostaTexto = "waitingfor," + (nLastMessageInOrder + 1);
                    }

                    System.out.println("Estado atual (L): " + nLastMessageInOrder);
                    System.out.println("Total em ordem entregue: " + listaRececao.size() + " | Em buffer: " + mensagensTemporarias.size());
                    System.out.println("Resposta enviada: [" + respostaTexto + "]");
                    System.out.println("------------------------------------------------");

                    // Resposta ao cliente
                    byte[] replyBuffer = respostaTexto.getBytes();
                    DatagramPacket reply = new DatagramPacket(
                            replyBuffer,
                            replyBuffer.length,
                            request.getAddress(),
                            request.getPort()
                    );
                    aSocket.send(reply);

                } catch (Exception e) {
                    System.out.println("Erro ao processar pacote recebido: " + e.getMessage());
                    String erroTexto = "ERRO: formato incorreto";
                    byte[] errBytes = erroTexto.getBytes();
                    DatagramPacket reply = new DatagramPacket(
                            errBytes,
                            errBytes.length,
                            request.getAddress(),
                            request.getPort()
                    );
                    aSocket.send(reply);
                }
            }

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }
}