package org.group17;

import java.io.Serial;
import java.io.Serializable;

public record GroupMessage(
        String text,
        int senderId,
        VectorClock timestamp,
        int targetId,
        int groupId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}