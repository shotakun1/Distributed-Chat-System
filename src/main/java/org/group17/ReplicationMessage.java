package org.group17;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

public record ReplicationMessage(
        Map<Integer, ChatServer.ClientReplica> clientData,
        Map<Integer, ChatServer.ChatRoomReplica> roomData
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
