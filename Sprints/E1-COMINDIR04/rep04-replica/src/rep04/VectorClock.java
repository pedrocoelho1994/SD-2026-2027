package rep04;

import java.util.Arrays;

/**
 * Relógio vetorial usado pelos líderes para representar dependências causais.
 *
 * Cada posição corresponde a uma partição. Um líder incrementa apenas a sua
 * posição; ao receber contexto de outro processo, fica com o máximo de cada
 * componente. Esta classe não cria eventos por si só: apenas manipula o
 * carimbo que acompanha cada {@link SensorRecord}.
 */
public final class VectorClock {
    public enum Order { BEFORE, AFTER, EQUAL, CONCURRENT }

    private final long[] v;

    /** Cria um vetor de dimensão {@code n}, inicialmente preenchido com zeros. */
    public VectorClock(int n) {
        this.v = new long[n];
    }

    /** Construtor interno usado para preservar cópias independentes do vetor. */
    private VectorClock(long[] v) {
        this.v = v;
    }

    /**
     * Funde conhecimento causal: cada componente passa a ser o máximo local
     * e recebido. É a operação usada pelo líder e pelo processador.
     */
    public void merge(VectorClock other) {
        for (int i = 0; i < v.length; i++) {
            v[i] = Math.max(v[i], other.v[i]);
        }
    }

    /** Regista um evento local, incrementando a componente da partição {@code k}. */
    public void tick(int k) {
        v[k]++;
    }

    /** Lê a componente associada à partição {@code k}. */
    public long get(int k) {
        return v[k];
    }

    /** Devolve um snapshot imutável por partilha: alterações futuras não o afetam. */
    public VectorClock copy() {
        return new VectorClock(v.clone());
    }

    /**
     * Compara os vetores componente a componente.
     *
     * BEFORE/AFTER significam uma relação causal; CONCURRENT significa que
     * cada vetor domina pelo menos uma componente diferente.
     */
    public Order compare(VectorClock other) {
        boolean menor = false, maior = false;
        for (int i = 0; i < v.length; i++) {
            if (v[i] < other.v[i]) menor = true;
            if (v[i] > other.v[i]) maior = true;
        }
        if (menor && maior) return Order.CONCURRENT;
        if (menor) return Order.BEFORE;
        if (maior) return Order.AFTER;
        return Order.EQUAL;
    }

    /** Serializa o vetor no formato simples usado nos ficheiros e mensagens. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.toString();
    }

    /**
     * Lê um vetor serializado e valida a dimensão esperada.
     *
     * A validação torna explícita a limitação de mudar N sem migrar os
     * registos persistidos.
     */
    public static VectorClock parse(String s, int n) {
        String[] f = s.trim().split(",");
        if (f.length != n) {
            throw new IllegalArgumentException("Vetor com dimensão " + f.length + ", esperado " + n + ": " + s);
        }
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = Long.parseLong(f[i]);
        }
        return new VectorClock(v);
    }

    /** Compara valores, e não identidade, para permitir testes e classificações. */
    @Override
    public boolean equals(Object o) {
        return o instanceof VectorClock && Arrays.equals(v, ((VectorClock) o).v);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(v);
    }
}