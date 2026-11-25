package org.group17;

import java.io.Serial;
import java.io.Serializable;

public record DirectMessage(
        String text,
        int senderId,
        VectorClock timestamp,
        int targetId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
