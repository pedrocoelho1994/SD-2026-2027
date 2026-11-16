
package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

/** Configuração e contrato de comunicação da COMINDIR05. */
public final class Wire {

    public static final double LIMIAR_ALARME = 28.0;
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;

    public static final int GRUPO = 3;
    public static final int PORTO_BASE = 7000;

    public static final long PERIODO_HB_MS =
            Long.getLong("raft04.periodo", 2000L);

    public static final int FATOR_TIMEOUT =
            Integer.getInteger("raft04.fator", 3);

    public static final long VERIFICACAO_MS = 200L;

    public static final long ELEICAO_MIN_MS =
            Long.getLong("raft04.eleicaoMin",
                    3L * PERIODO_HB_MS);

    public static final long ELEICAO_MAX_MS =
            Long.getLong("raft04.eleicaoMax",
                    6L * PERIODO_HB_MS);

    private Wire() { }

    /** Tópico de leituras enviadas pelo produtor. */
    public static String topicIn(int k) {
        return "raft04-in-p" + k;
    }

    /** Tópico de lotes e heartbeats da partição. */
    public static String topicData(int k) {
        return "raft04-p" + k;
    }

    /** Tópico dos pedidos de voto e respetivas respostas. */
    public static String topicElection(int k) {
        return "raft04-eleicao-p" + k;
    }

    /** Porta TCP exclusiva de cada nó. */
    public static int nodePort(int k, int i) {
        if (k < 0 || k >= 3 || i < 1 || i > GRUPO) {
            throw new IllegalArgumentException(
                    "Particao ou no invalido");
        }

        return PORTO_BASE + k * GRUPO + i;
    }

    /** Verifica se o nó está isolado da comunicação entre nós. */
    public static boolean isolated(int k, int i) {
        return Files.exists(
                Path.of("isolar-p" + k + "-n" + i)
        );
    }

    /** Mensagem de leitura enviada pelo produtor. */
    public static String in(String sensor, double temp,
                            long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";"
                + lamport + ";" + vector;
    }

    /** Obtém o tipo de uma mensagem textual. */
    public static String type(String msg) {
        String m = msg.trim();
        int pos = m.indexOf(';');

        if (pos <= 0) {
            throw new IllegalArgumentException(
                    "Mensagem sem tipo: " + msg);
        }

        return m.substring(0, pos);
    }

    /** Obtém o conteúdo após o primeiro ponto e vírgula. */
    public static String body(String msg) {
        String m = msg.trim();
        int pos = m.indexOf(';');

        if (pos < 0) {
            throw new IllegalArgumentException(
                    "Mensagem sem separador: " + msg);
        }

        return m.substring(pos + 1);
    }
}
