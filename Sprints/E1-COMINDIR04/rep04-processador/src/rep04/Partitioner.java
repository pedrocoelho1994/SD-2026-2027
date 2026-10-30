package rep04;

/** Mapeia sensores para partições de forma determinística. */
public final class Partitioner {
    private final int n;

    /** Valida o número de partições usado para dimensionar os vetores. */
    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("numero de particoes invalido: " + n);
        }
        this.n = n;
    }

    /** Usa o hash do sensor, normalizado para nunca produzir índice negativo. */
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    public int count() {
        return n;
    }
}