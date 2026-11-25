package org.group17;

import java.io.Serial;
import java.io.Serializable;

public record HeartbeatMessage(
        int serverId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
