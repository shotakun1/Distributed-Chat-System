package org.group17;

import java.io.Serial;
import java.io.Serializable;

public record ServerInfo(
        int id,
        String host,
        int port
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}