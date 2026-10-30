package rep04;

/** Contrato textual de tópicos e mensagens entre os processos. */
public final class Wire {
    public static final double LIMIAR_ALARME = 28.0;
    public static final int LIMITE_RETIDOS = 10;   // acima deste numero de retidos, a replica recupera
    public static final int T_TCP_MS = 5000;       // tempo maximo de ligacao e de espera por cada linha
    public static final int PORTO_BASE = 7000;     // porto de recuperacao do lider da particao k: 7000 + k
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    public static final long VERIFICACAO_MS = 1000L;

    private Wire() { }

    /** Tópico de entradas destinado à partição k. */
    public static String topicIn(int k)   { return "rep04-in-p" + k; }
    /** Tópico de LOTEs e heartbeats emitidos pelo líder da partição k. */
    public static String topicData(int k) { return "rep04-p" + k; }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    /** Obtém e valida o tipo textual da mensagem. */
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    /** Obtém o corpo depois do primeiro ponto e vírgula. */
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }

}