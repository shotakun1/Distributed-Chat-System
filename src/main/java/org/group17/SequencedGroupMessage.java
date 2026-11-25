package org.group17;

import java.io.Serial;
import java.io.Serializable;

public record SequencedGroupMessage(
        GroupMessage message,
        long sequenceNumber,
        int roomId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}