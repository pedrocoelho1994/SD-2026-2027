package tcp01;

import java.io.*;
import java.net.*;

public class TCPServer {
    public static void main(String[] args) {
        try {
            int serverPort = 7896;
            ServerSocket listenSocket = new ServerSocket(serverPort);   //não bloqueia, apenas cria o socket do servidor/reserva o porto 
            while (true) {
                Socket clientSocket = listenSocket.accept();    // bloqueia o servidor até chegar um cliente. Por isso o servidor arranca primeiro.
                Connection c = new Connection(clientSocket);    // processa o pedido noutra thread / não bloqueia, lança a thread e volta ao accept() para esperar por outro cliente
            }
        } catch (IOException e) {
            System.out.println("Listen: " + e.getMessage());
        }
    }
}