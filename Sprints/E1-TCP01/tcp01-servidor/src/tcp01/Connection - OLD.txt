package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    DataInputStream in;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            in = new DataInputStream(clientSocket.getInputStream());        // não bloqueia
            out = new DataOutputStream(clientSocket.getOutputStream());     //não bloqueia
            this.start();                                       // executa run() numa thread separada / não bloqueia
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            String data = in.readUTF();                         // lê os dados do cliente / bloqueia - só esta thread
            System.out.println("Received: " + data);            // EXTRA!!!!!! - mostra mensagem no servidor / não bloqueia
            out.writeUTF(data);                                 // envia a resposta ao cliente / normalmente não bloqueia - copia para o buffer do TCP
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();                           // não bloqueia - termina a ligação
            } catch (IOException e) {
                System.out.println("close: " + e.getMessage()); // EXTRA!!!!! - mostra qual o erro ao fechar o socket
                /* falha ao fechar */
            }
        }
    }
}