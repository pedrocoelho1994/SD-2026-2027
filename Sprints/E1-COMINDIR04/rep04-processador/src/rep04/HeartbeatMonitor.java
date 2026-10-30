package rep04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }
    private final int partition; private final long timeoutNanos; private final Consumer<String> log;
    private State state = State.DESCONHECIDO; private long lastNanos = System.nanoTime();
    private long inc = -1; private long hb;
    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition; timeoutNanos = periodMs * factor * 1_000_000L; this.log = log;
    }
    public synchronized void onHeartbeat(long i, long h) {
        if (i < inc || (i == inc && h <= hb)) return;
        if (inc != -1 && i != inc) log.accept("REINICIO;p" + partition + ";inc=" + inc + "->" + i);
        else if (i == inc && h != hb + 1) log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (hb + 1) + ";recebido=" + h);
        if (state == State.SUSPEITO) log.accept("RETOMA;p" + partition + ";inc=" + i + ";hb=" + h);
        inc = i; hb = h; lastNanos = System.nanoTime(); state = State.ATIVO;
    }
    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) { state = State.SUSPEITO; log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + elapsed / 1_000_000); }
    }
    public synchronized State state() { return state; }
}