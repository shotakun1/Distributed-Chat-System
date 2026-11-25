package org.group17;

import java.io.Serial;
import java.io.Serializable;

public record GroupMessageAck(
        int roomId,
        long sequenceNumber,
        int clientId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}