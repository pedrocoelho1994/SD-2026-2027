package tpf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Chunk {

    private final List<Registo> registos = new ArrayList<>();
    private int tamanhoBruto = Integer.BYTES; // 4 bytes iniciais para numRegistos
    private boolean selado = false;

    public synchronized void adicionar(Registo registo) {
        if (selado) {
            throw new IllegalStateException("Chunk já foi selado e é imutável.");
        }
        byte[] autorBytes = registo.autor().getBytes(StandardCharsets.UTF_8);
        byte[] canalBytes = registo.canal().getBytes(StandardCharsets.UTF_8);
        byte[] textoBytes = registo.texto().getBytes(StandardCharsets.UTF_8);

        registos.add(registo);
        tamanhoBruto += Long.BYTES // lsn
                + Long.BYTES // instante (epoch millis)
                + Integer.BYTES + autorBytes.length
                + Integer.BYTES + canalBytes.length
                + Integer.BYTES + textoBytes.length;
    }

    public synchronized void selar() {
        this.selado = true;
    }

    public synchronized boolean isSelado() {
        return selado;
    }

    public synchronized boolean estaVazio() {
        return registos.isEmpty();
    }

    public synchronized int getTamanhoBruto() {
        return tamanhoBruto;
    }

    public synchronized long getLsnInicial() {
        if (registos.isEmpty()) {
            throw new IllegalStateException("Chunk vazio não tem LSN inicial.");
        }
        return registos.get(0).lsn();
    }

    public synchronized long getLsnFinal() {
        if (registos.isEmpty()) {
            throw new IllegalStateException("Chunk vazio não tem LSN final.");
        }
        return registos.get(registos.size() - 1).lsn();
    }

    public synchronized List<Registo> getRegistos() {
        return List.copyOf(registos);
    }

    public synchronized byte[] serializar() throws IOException {
        return serializar(Wire.PAD_BYTES);
    }

    public synchronized byte[] serializar(int padBytes) throws IOException {
        if (padBytes <= 0) {
            throw new IllegalArgumentException("padBytes tem de ser > 0.");
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(tamanhoBruto);
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(registos.size());
            for (Registo r : registos) {
                out.writeLong(r.lsn());
                out.writeLong(r.instante().toEpochMilli());
                escreverString(out, r.autor());
                escreverString(out, r.canal());
                escreverString(out, r.texto());
            }
        }

        byte[] bruto = baos.toByteArray();
        int resto = bruto.length % padBytes;
        int padding = (resto == 0) ? 0 : (padBytes - resto);
        if (padding == 0) {
            return bruto;
        }

        byte[] comPadding = new byte[bruto.length + padding];
        System.arraycopy(bruto, 0, comPadding, 0, bruto.length);
        return comPadding;
    }

    public static List<Registo> desserializar(byte[] dados) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(dados))) {
            int numRegistos = in.readInt();
            if (numRegistos < 0) {
                throw new IOException("Número de registos inválido no chunk: " + numRegistos);
            }
            List<Registo> resultado = new ArrayList<>(numRegistos);
            for (int i = 0; i < numRegistos; i++) {
                long lsn = in.readLong();
                long epochMillis = in.readLong();
                Instant instante = Instant.ofEpochMilli(epochMillis);
                String autor = lerString(in);
                String canal = lerString(in);
                String texto = lerString(in);
                resultado.add(new Registo(lsn, instante, autor, canal, texto));
            }
            return resultado;
        }
    }

    private static void escreverString(DataOutputStream out, String valor) throws IOException {
        byte[] bytes = valor.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String lerString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > in.available()) {
            throw new IOException("Comprimento de string inválido no chunk: " + len);
        }
        byte[] bytes = in.readNBytes(len);
        if (bytes.length != len) {
            throw new IOException("Fim inesperado ao ler string do chunk.");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
