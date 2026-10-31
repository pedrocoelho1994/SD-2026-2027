package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream in;                                               // ALTERADO: era DataInputStream
    ObjectOutputStream out;                                             // ALTERADO: era DataOutputStream
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            out = new ObjectOutputStream(clientSocket.getOutputStream()); // ALTERADO: o output é criado primeiro / não bloqueia
            out.flush();                                                // NOVO: envia o cabeçalho antes de criar o input / não bloqueia
            in = new ObjectInputStream(clientSocket.getInputStream());  // ALTERADO: BLOQUEIA até chegar o cabeçalho do cliente
            this.start();                                       // executa run() numa thread separada / não bloqueia
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            Person p = (Person) in.readObject();
        System.out.println("Received: " + p.getName());
        out.writeUTF("Localidade: " + p.getPlace().getLocality());   // ALTERADO: era "Recebi a pessoa: " + p.getName() / normalmente não bloqueia - copia para o buffer do TCP
            out.flush();                                        // NOVO: garante que a resposta é enviada
        } catch (ClassNotFoundException e) {                    // NOVO: obrigatório, a classe Person não existe no servidor
            System.out.println("Class: " + e.getMessage());
        } catch (ClassCastException e) {                        // NOVO: o objeto recebido não é uma Person
            System.out.println("Cast: " + e.getMessage());
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