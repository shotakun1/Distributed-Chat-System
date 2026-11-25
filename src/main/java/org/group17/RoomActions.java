package org.group17;

import java.io.Serial;
import java.io.Serializable;
import java.util.Set;

public class RoomActions {

    public record CreateRoomRequest(String requestId, int creatorId, String roomName) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record CreateRoomResponse(String requestId, boolean success, Integer roomId, String message, VectorClock clock, long startingSequence) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record JoinRoomRequest(int clientId, int roomId, int requestingServerId) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record JoinRoomResponse(int clientId, boolean success, int roomId, String message, Set<Integer> currentMembers, VectorClock currentClock, long startingSequence) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record LeaveRoomRequest(int clientId, int roomId) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record LeaveRoomResponse(int clientId, boolean success, String message) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record AddMemberRequest(int requesterId, int roomId, int targetClientId) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record AddMemberResponse(boolean success, String message) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record RemoveMemberRequest(int requesterId, int roomId, int targetClientId) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record RemoveMemberResponse(boolean success, String message) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record RoomMemberNotification(int roomId, int memberId, boolean joined, VectorClock updatedClock) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    public record RoomStateUpdateMessage(int roomId, VectorClock clock, long sequenceNumber, Set<Integer> members) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}