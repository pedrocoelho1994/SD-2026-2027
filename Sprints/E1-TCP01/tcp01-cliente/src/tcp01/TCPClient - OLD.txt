package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;                              // porto do servidor
            s = new Socket("localhost", serverPort);            // falha com ConnectException se o servidor não estiver a correr / bloqueia
            DataInputStream in = new DataInputStream(s.getInputStream());   // não bloqueia
            DataOutputStream out = new DataOutputStream(s.getOutputStream());      // não bloqueia
            out.writeUTF("mensagem em UTF");                    // envia os dados ao servidor / normalmente não bloqueia - apenas enche o buffer do TCP
            String data = in.readUTF();                         // bloqueia à espera da resposta 
            System.out.println("Received: " + data);            //não bloqueia
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}