package org.group17;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

public class VectorClock implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Map<Integer, Integer> clock = new HashMap<>();

    public int get(int id) {
        return clock.getOrDefault(id, 0);
    }

    public void set(int id, int value) {
        clock.put(id, value);
    }

    public void tick(int self) {
        clock.put(self, get(self) + 1);
    }

    public void merge(VectorClock other) {
        for (var e : other.clock.entrySet()) {
            clock.put(e.getKey(), Math.max(get(e.getKey()), e.getValue()));
        }
    }

    // use for message timestamps
    public VectorClock snapshot() {
        VectorClock c = new VectorClock();
        c.clock.putAll(this.clock);
        return c;
    }

    public Set<Map.Entry<Integer, Integer>> entries() {
        return clock.entrySet();
    }

    public boolean contains(int id) {
        return clock.containsKey(id);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        Iterator<Map.Entry<Integer, Integer>> it = clock.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Integer> e = it.next();
            sb.append(e.getKey()).append(": ").append(e.getValue());
            if (it.hasNext()) sb.append(", ");
        }
        sb.append("}");
        return sb.toString();
    }
}