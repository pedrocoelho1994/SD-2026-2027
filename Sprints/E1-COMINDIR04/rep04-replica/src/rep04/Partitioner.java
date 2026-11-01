package rep04;

/** Mapeia cada sensor para uma partição de forma determinística. */
public final class Partitioner {
    private final int n;

    /** Cria o mapeador e rejeita uma configuração sem partições. */
    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("numero de particoes invalido: " + n);
        }
        this.n = n;
    }

    /**
     * Calcula a partição do sensor. {@code floorMod} evita índices negativos
     * mesmo quando o hash Java do identificador é negativo.
     */
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    /** Devolve o número de partições, usado para dimensionar vetores. */
    public int count() {
        return n;
    }
}