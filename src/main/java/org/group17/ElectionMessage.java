package org.group17;

import java.io.Serial;
import java.io.Serializable;

/**
 * Message used for the Bully Algorithm leader election.
 */
public record ElectionMessage(
        Type type,
        int senderId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public enum Type {
        ELECTION,       // "I want to be leader"
        ANSWER,         // "I have a higher ID, stop"
        COORDINATOR     // "I am the new leader"
    }
}