package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;
            s = new Socket("localhost", serverPort);                 // BLOQUEIA: espera a ligação
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            out.flush();                                             // envia o cabeçalho ANTES de criar o input
            ObjectInputStream in = new ObjectInputStream(s.getInputStream()); // BLOQUEIA: espera o cabeçalho do servidor
            out.writeObject(new Person("Ana", new Place("3500-001", "Viseu"), 1990));                // envia o objeto
            out.flush();
            String data = in.readUTF();                              // BLOQUEIA: espera a resposta em texto
            System.out.println("Received: " + data);
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try { s.close(); }
                catch (IOException e) { System.out.println("close: " + e.getMessage()); }
            }
        }
    }
}