package org.group17;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class ChatRoom {

    private final int roomId;
    private final String roomName;
    private final int creatorId;
    private final ChatServer server;
    private final Map<Integer, ChatClientHandler> localMembers = new HashMap<>();
    private final Set<Integer> allMemberIds = new HashSet<>(); // includes remote members
    private final List<GroupMessage> groupMessageLog = new ArrayList<>();

    public ChatRoom(int roomId, String roomName, int creatorId, ChatServer server) {
        this.roomId = roomId;
        this.roomName = roomName;
        this.creatorId = creatorId;
        this.server = server;
    }

    public void addMember(int clientId) {
        allMemberIds.add(clientId);

        // If client is local, add handler reference
        ChatClientHandler handler = server.getLocalClient(clientId);
        if (handler != null) {
            localMembers.put(clientId, handler);
            System.out.println("Client " + clientId + " added to local members of room " + roomId);
        } else {
            System.out.println("Client " + clientId + " added to room " + roomId + " (not local)");
        }
    }

    public void addLocalMember(ChatClientHandler handler) {
        localMembers.put(handler.getClientId(), handler);
    }

    public void removeMember(int clientId) {
        allMemberIds.remove(clientId);
        localMembers.remove(clientId);
        System.out.println("Client " + clientId + " removed from room " + roomId);
    }

    public List<GroupMessage> getMessageHistory() {
        return groupMessageLog;
    }

    public int getRoomId() {
        return roomId;
    }

    public String getRoomName() {
        return roomName;
    }

    public int getCreatorId() {
        return creatorId;
    }

    public Set<Integer> getAllMemberIds() {
        return new HashSet<>(allMemberIds);
    }

    public Map<Integer, ChatClientHandler> getLocalMembers() {
        return new HashMap<>(localMembers);
    }
}