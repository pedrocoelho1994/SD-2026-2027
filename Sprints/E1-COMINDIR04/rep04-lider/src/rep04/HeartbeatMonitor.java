package rep04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }
    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;
    private State state = State.DESCONHECIDO;
    private long lastNanos = System.nanoTime();
    private long inc = -1;
    private long hb;

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
    }

    public synchronized void onHeartbeat(long newInc, long newHb) {
        if (newInc < inc || (newInc == inc && newHb <= hb)) return;
        if (inc != -1 && newInc != inc) log.accept("REINICIO;p" + partition + ";inc=" + inc + "->" + newInc);
        else if (newInc == inc && newHb != hb + 1)
            log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (hb + 1) + ";recebido=" + newHb);
        if (state == State.SUSPEITO) log.accept("RETOMA;p" + partition + ";inc=" + newInc + ";hb=" + newHb);
        inc = newInc; hb = newHb; lastNanos = System.nanoTime(); state = State.ATIVO;
    }

    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + elapsed / 1_000_000);
        }
    }

    public synchronized State state() { return state; }
}