package rep03;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;   // acima deste numero de retidos, a replica recupera
    public static final int T_TCP_MS = 5000;       // tempo maximo de ligacao e de espera por cada linha
    public static final int PORTO_BASE = 7000;     // porto de recuperacao do lider da particao k: 7000 + k

    private Wire() { }

    public static String topicIn(int k)   { return "rep03-in-p" + k; }
    public static String topicData(int k) { return "rep03-p" + k; }

    public static String in(String sensor, double temp) { return "IN;" + sensor + ";" + temp; }

    // REG;<endereco>;<porto>;<seq>;<sensor>;<temperatura>;<instante>
    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
    }

    // tipo da mensagem (IN, REG); lanca IllegalArgumentException se nao houver tipo
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    // conteudo da mensagem sem o tipo
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }

    // divide uma mensagem REG em { endereco, porto, linha do registo }
    public static String[] regParts(String msg) {
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG")) {
            throw new IllegalArgumentException("REG mal formado: " + msg);
        }
        return new String[] { f[1], f[2], f[3] };
    }
}
