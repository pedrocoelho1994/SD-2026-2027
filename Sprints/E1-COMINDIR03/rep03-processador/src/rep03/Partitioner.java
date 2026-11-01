package rep03;

public final class Partitioner {
    private final int n;

    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("numero de particoes invalido: " + n);
        }
        this.n = n;
    }

    // particao de um sensor: deterministica e igual em qualquer JVM
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    public int count() {
        return n;
    }
}