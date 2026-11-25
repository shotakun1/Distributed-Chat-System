package org.group17;

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ChatClient {

    private final int clientId;
    private ServerInfo connectedServer;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private final List<Integer> joinedRooms = new ArrayList<>();
    private volatile boolean running = false;

    private ServerSocket pushSocket;
    public static final int PUSH_PORT_OFFSET = 2000;

    private final Map<Integer, VectorClock> conversationClocks = new HashMap<>();
    private final Map<Integer, List<GroupMessage>> groupMessagesBuffers = new HashMap<>();
    private final Map<Integer, VectorClock> roomClocks = new HashMap<>();
    private final Map<Integer, Set<Integer>> roomMembers = new HashMap<>();

    private final Map<Integer, Long> nextExpectedRoomSeq = new HashMap<>();
    private final Map<Integer, TreeMap<Long, SequencedGroupMessage>> roomOutOfOrderBuffer = new HashMap<>();

    private final Map<String, CompletableFuture<Integer>> pendingRoomCreations = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Startup
    // -------------------------------------------------------------------------

    public ChatClient() throws IOException, ClassNotFoundException, InterruptedException {
        this.clientId = queryNextClientId();
        startPushListener();
        System.out.println("Client Initialized with ID: " + clientId);
    }

    public List<ServerInfo> discoverServers() {
        try {
            return HttpRegistryClient.getActiveServers();
        } catch (Exception e) {
            System.out.println("Error discovering servers: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public int getClientId() { return clientId; }

    public boolean isConnected() {
        return running && socket != null && !socket.isClosed();
    }

    // -------------------------------------------------------------------------
    // Connect / disconnect
    // -------------------------------------------------------------------------

    public void connect(ServerInfo server) {
        try {
            socket = new Socket(server.host(), server.port());
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(socket.getInputStream());
            in.readObject();            // WELCOME
            out.writeObject(clientId);  // identify ourselves
            out.flush();
            connectedServer = server;
            running = true;
            new Thread(this::listen, "ClientListener-" + clientId).start();
            System.out.println("Successfully connected to Server " + server.id());
        } catch (Exception e) {
            connectedServer = null;
            System.out.println("[Connection] Failed to connect to Server " + server.id() + ": " + e.getMessage());
        }
    }

    public void disconnect() {
        if (!running) return;

        for (int roomId : new ArrayList<>(joinedRooms)) {
            leaveRoom(roomId);
        }

        running = false;
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        connectedServer = null;
        System.out.println("ChatClient: Client " + clientId + " disconnected.");
    }

    /**
     * Silently close the current connection without sending leave messages.
     * Used during server reassignment so we don't spam a dead server.
     */
    private void closeSilently() {
        running = false;
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
        } catch (IOException ignored) {}
        connectedServer = null;
    }

    // -------------------------------------------------------------------------
    // Room / messaging API
    // -------------------------------------------------------------------------

    public CompletableFuture<Integer> createRoom(String roomName) {
        if (!isConnected()) {
            System.out.println("ChatClient: Not connected.");
            return CompletableFuture.completedFuture(null);
        }
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<Integer> future = new CompletableFuture<>();
        pendingRoomCreations.put(requestId, future);
        try {
            out.writeObject(new RoomActions.CreateRoomRequest(requestId, clientId, roomName));
            out.flush();
            System.out.println("ChatClient: Client " + clientId + " requested to create room: " + roomName);
        } catch (IOException e) {
            System.out.println("ChatClient: Failed to send create room request");
            future.complete(null);
            pendingRoomCreations.remove(requestId);
        }
        return future;
    }

    public void joinRoom(int roomId) {
        if (!isConnected()) { System.out.println("ChatClient: Not connected."); return; }
        try {
            out.writeObject(new RoomActions.JoinRoomRequest(clientId, roomId, -1));
            out.flush();
            System.out.println("ChatClient: Client " + clientId + " requested to join room " + roomId);
        } catch (IOException e) {
            System.out.println("ChatClient: Failed to send join room request");
        }
    }

    public void leaveRoom(int roomId) {
        if (!isConnected()) { System.out.println("ChatClient: Not connected."); return; }
        try {
            out.writeObject(new RoomActions.LeaveRoomRequest(clientId, roomId));
            out.flush();
            joinedRooms.remove(Integer.valueOf(roomId));
            roomMembers.remove(roomId);
            roomClocks.remove(roomId);
            nextExpectedRoomSeq.remove(roomId);
            roomOutOfOrderBuffer.remove(roomId);
            System.out.println("ChatClient: Client " + clientId + " left room " + roomId);
        } catch (IOException e) {
            System.out.println("ChatClient: Failed to send leave room request");
        }
    }

    public void sendMessageToRoom(int targetRoomId, String text) {
        if (!isConnected()) { System.out.println("ChatClient: Not connected."); return; }
        if (!joinedRooms.contains(targetRoomId)) { System.out.println("ChatClient: Not member of the group!"); return; }
        VectorClock clock = roomClockFor(targetRoomId);
        clock.tick(clientId);
        GroupMessage gm = new GroupMessage(text, clientId, clock.snapshot(), -1, targetRoomId);
        try {
            out.writeObject(gm);
            out.flush();
            System.out.println("Sent RM to " + targetRoomId + ": " + text);
        } catch (IOException e) {
            System.out.println("ChatClient: Failed to send group message");
        }
    }

    public void sendDirectMessage(int targetClientId, String text) {
        if (!isConnected()) { System.out.println("ChatClient: Not connected."); return; }
        VectorClock clock = conversationClocks.computeIfAbsent(targetClientId, _ -> new VectorClock());
        clock.tick(clientId);
        DirectMessage directMessage = new DirectMessage(text, clientId, clock.snapshot(), targetClientId);
        try {
            out.writeObject(directMessage);
            out.flush();
            System.out.println("Sent DM to " + targetClientId + ": " + text);
        } catch (IOException e) {
            System.out.println("ChatClient: Failed to send direct message");
        }
    }

    // -------------------------------------------------------------------------
    // Incoming message listener (runs on its own thread per connection)
    // -------------------------------------------------------------------------

    private void listen() {
        try {
            while (running) {
                Object obj = in.readObject();
                dispatchIncoming(obj);
            }
        } catch (Exception e) {
            if (running) {
                running = false;
                System.out.println("[Connection] Lost connection to Server " + (connectedServer != null ? connectedServer.id() : "?") + ". Waiting for reassignment from the cluster...");
            }
        }
    }

    private void startPushListener() {
        try {
            pushSocket = new ServerSocket(clientId + PUSH_PORT_OFFSET); // deterministic port from clientId
            new Thread(() -> {
                while (!pushSocket.isClosed()) {
                    try {
                        Socket s = pushSocket.accept();
                        ObjectInputStream oin = new ObjectInputStream(s.getInputStream());
                        try {
                            while (true) {
                                Object msg = oin.readObject();
                                dispatchIncoming(msg);
                            }
                        } catch (EOFException ignored) {} // socket closed after all messages sent
                        s.close();
                    } catch (Exception ignored) {}
                }
            }, "PushListener-" + clientId).start();
        } catch (IOException e) {
            System.out.println("[PushListener] Could not start: " + e.getMessage());
        }
    }

    private void dispatchIncoming(Object obj) {
        if (obj instanceof ChatServer.ServerReassignmentMessage(ServerInfo newServer)) {
            handleServerReassignment(newServer);

        } else if (obj instanceof ChatServer.DeliveryFailureMessage dfm) {
            System.out.println("\n[!] Message to " + dfm.targetId() + " could not be delivered: " + dfm.reason());
            System.out.print("> ");

        } else if (obj instanceof SequencedGroupMessage sm) {
            handleSequencedGroupMessage(sm);

        } else if (obj instanceof GroupMessage gm) {
            receiveGroupMessage(gm);

        } else if (obj instanceof DirectMessage dm) {
            receiveDirectMessage(dm);

        } else if (obj instanceof RoomActions.JoinRoomResponse res) {
            handleJoinRoomResponse(res);

        } else if (obj instanceof RoomActions.CreateRoomResponse res) {
            handleCreateRoomResponse(res);
            System.out.println(">> Create Room: " + res.message() + " (ID: " + res.roomId() + ")");

        } else if (obj instanceof RoomActions.LeaveRoomResponse res) {
            System.out.println(">> Leave Room: " + res.message());

        } else if (obj instanceof RoomActions.RoomMemberNotification n) {
            handleRoomMemberNotification(n);
        } else if (obj instanceof RoomActions.RoomStateUpdateMessage rum) {
            handleRoomStateUpdate(rum);
        }
    }

    // -------------------------------------------------------------------------
    // Server reassignment — seamless reconnect without disrupting listen() thread
    // -------------------------------------------------------------------------

    /**
     * Called when the cluster has reassigned this client to a new server.
     * Strategy: close the old connection silently (setting running=false so
     * the current listen() thread exits cleanly after this call returns),
     * then open a fresh connection and re-join all rooms.
     * This is intentionally NOT called from a separate thread because it is
     * already dispatched from listen() — we just need to close, reconnect,
     * and return; the new listen() thread spawned by connect() takes over.
     */
    private void handleServerReassignment(ServerInfo newServer) {
        System.out.println("[Reassignment] Client " + clientId + " reassigned from Server " + (connectedServer != null ? connectedServer.id() : "?") + " -> Server " + newServer.id() + " (" + newServer.host() + ":" + newServer.port() + ")");
        List<Integer> roomsToRejoin = new ArrayList<>(joinedRooms);

        // Close the current socket without sending leave messages to the (dead) server
        closeSilently();

        // Reconnect — connect() also starts a fresh listen() thread
        connect(newServer);

        if (running) {
            System.out.println("[Reassignment] Connected to new server. Waiting for room state update...");
        } else {
            System.out.println("[Reassignment] Reconnection to new server failed. Trying any server...");
            joinedRooms.addAll(roomsToRejoin);   // keep the list so attemptReconnection can use it
            attemptReconnection();
        }
    }

    // -------------------------------------------------------------------------
    // Direct messages
    // -------------------------------------------------------------------------

    private void receiveDirectMessage(DirectMessage dm) {
        VectorClock local = conversationClocks.computeIfAbsent(dm.senderId(), _ -> new VectorClock());
        local.merge(dm.timestamp());
        System.out.println("\n[DM] From " + dm.senderId() + ": " + dm.text());
        System.out.print("> ");
    }

    // -------------------------------------------------------------------------
    // Group messages (sequenced + causal ordering)
    // -------------------------------------------------------------------------

    private void handleSequencedGroupMessage(SequencedGroupMessage seqMsg) {
        int rId = seqMsg.roomId();
        long expected = nextExpectedRoomSeq.getOrDefault(rId, 1L);
        if (seqMsg.sequenceNumber() == expected) {
            if (seqMsg.message().senderId() != clientId) {
                deliverSequencedGroupMessage(seqMsg);
            }
            nextExpectedRoomSeq.put(rId, expected + 1);
            sendAck(rId, seqMsg.sequenceNumber());
            deliverBufferedSequencedMessages(rId);
        } else if (seqMsg.sequenceNumber() > expected) {
            roomOutOfOrderBuffer.computeIfAbsent(rId, _ -> new TreeMap<>())
                    .put(seqMsg.sequenceNumber(), seqMsg);
            sendAck(rId, seqMsg.sequenceNumber());
        }
        // seqNum < expected → duplicate, ignore
    }

    private void deliverBufferedSequencedMessages(int rId) {
        TreeMap<Long, SequencedGroupMessage> buffer = roomOutOfOrderBuffer.get(rId);
        if (buffer == null) return;
        long expected = nextExpectedRoomSeq.get(rId);
        while (buffer.containsKey(expected)) {
            SequencedGroupMessage next = buffer.remove(expected);
            if (next.message().senderId() != clientId) deliverSequencedGroupMessage(next);
            expected++;
            nextExpectedRoomSeq.put(rId, expected);
        }
    }

    private void deliverSequencedGroupMessage(SequencedGroupMessage seqMsg) {
        GroupMessage gm = seqMsg.message();
        VectorClock localClock = roomClockFor(gm.groupId());
        if (isDeliverable(gm, localClock)) {
            deliver(gm, localClock);
            tryDeliverBufferedGroupMessages(gm.senderId());
        } else {
            groupMessagesBuffers.computeIfAbsent(gm.senderId(), _ -> new ArrayList<>()).add(gm);
        }
    }

    private boolean isDeliverable(GroupMessage gm, VectorClock localClock) {
        int senderId = gm.senderId();
        VectorClock msgClock = gm.timestamp();

        int localSenderVal = localClock.get(senderId);
        int receivedSenderVal = msgClock.get(senderId);

        // Sender reset their clock (rejoined) — accept any reset to a lower sequence
        // not just the == 1 case, since we may have missed the notification
        if (receivedSenderVal < localSenderVal) {
            localClock.set(senderId, 0);
            localSenderVal = 0;
        }

        if (receivedSenderVal != localSenderVal + 1) return false;

        for (var entry : msgClock.entries()) {
            int id = entry.getKey();
            if (id == senderId) continue;
            if (entry.getValue() > localClock.get(id)) return false;
        }

        return true;
    }

    private void deliver(GroupMessage gm, VectorClock localClock) {
        localClock.merge(gm.timestamp());
        System.out.println("Room " + gm.groupId() + " | Client " + gm.senderId() + ": " + gm.text());
        System.out.print("> ");
    }

    private void tryDeliverBufferedGroupMessages(int senderId) {
        List<GroupMessage> buffer = groupMessagesBuffers.get(senderId);
        if (buffer == null || buffer.isEmpty()) return;
        buffer.sort(Comparator.comparingInt(m -> m.timestamp().get(senderId)));
        Iterator<GroupMessage> it = buffer.iterator();
        while (it.hasNext()) {
            GroupMessage gm = it.next();
            VectorClock localClock = roomClockFor(gm.groupId());
            if (isDeliverable(gm, localClock)) {
                deliver(gm, localClock);
                it.remove();
            } else break;
        }
    }

    private void sendAck(int rId, long seq) {
        try {
            out.writeObject(new GroupMessageAck(rId, seq, clientId));
            out.flush();
        } catch (IOException ignored) {}
    }

    private VectorClock roomClockFor(int rId) {
        return roomClocks.computeIfAbsent(rId, _ -> {
            VectorClock vc = new VectorClock();
            vc.set(clientId, 0);
            return vc;
        });
    }

    // -------------------------------------------------------------------------
    // Room response handlers
    // -------------------------------------------------------------------------

    private void handleCreateRoomResponse(RoomActions.CreateRoomResponse response) {
        CompletableFuture<Integer> future = pendingRoomCreations.remove(response.requestId());
        if (response.success()) {
            // no need to explicitly join, creation already takes care of that
            System.out.println("ChatClient: Room created successfully with ID " + response.roomId() + " and joined.");
            joinedRooms.add(response.roomId());
            roomMembers.put(response.roomId(), new HashSet<>(Set.of(clientId)));
            if (response.clock() != null) roomClockFor(response.roomId()).merge(response.clock());
            nextExpectedRoomSeq.put(response.roomId(), response.startingSequence());
            if (future != null) future.complete(response.roomId());
        } else {
            System.out.println("ChatClient: Failed to create room - " + response.message());
            if (future != null) future.complete(null);
        }
    }

    private void handleJoinRoomResponse(RoomActions.JoinRoomResponse response) {
        if (response.success()) {
            if (!joinedRooms.contains(response.roomId())) joinedRooms.add(response.roomId());
            roomMembers.put(response.roomId(), new HashSet<>(response.currentMembers()));

            // Replace the clock entirely instead of merging — ensures we're in sync
            // with the server's current state, discarding any stale pre-leave/pre-failover state
            if (response.currentClock() != null) {
                roomClocks.put(response.roomId(), response.currentClock());
            } else {
                roomClocks.remove(response.roomId()); // start fresh
            }

            nextExpectedRoomSeq.put(response.roomId(), response.startingSequence());
            // Clear any stale out-of-order buffers from the previous session
            roomOutOfOrderBuffer.remove(response.roomId());
            Set<Integer> membersOfRoom = roomMembers.getOrDefault(response.roomId(), Set.of());
            membersOfRoom.forEach(groupMessagesBuffers::remove);

            System.out.println(">> Joined Room " + response.roomId() + ". Members: " + response.currentMembers());
        } else {
            System.out.println(">> Failed to join room: " + response.message());
        }
        System.out.print("> ");
    }

    private void handleRoomMemberNotification(RoomActions.RoomMemberNotification notification) {
        String action = notification.joined() ? "joined" : "left";
        System.out.println("\n[Room " + notification.roomId() + "] Member " + notification.memberId() + " " + action);
        Set<Integer> members = roomMembers.computeIfAbsent(notification.roomId(), _ -> new HashSet<>());
        if (notification.joined()) members.add(notification.memberId());
        else members.remove(notification.memberId());

        if (notification.joined() && notification.memberId() == clientId && notification.updatedClock() != null) {
            // I am the one rejoining — reset clock entirely to avoid stale state
            roomClocks.put(notification.roomId(), notification.updatedClock());
            roomOutOfOrderBuffer.remove(notification.roomId());
            groupMessagesBuffers.clear();
        } else if (notification.joined() && notification.updatedClock() != null) {
            // Someone else rejoined — reset only their entry in our local clock so we expect their messages to start from 1 again
            VectorClock local = roomClockFor(notification.roomId());
            local.set(notification.memberId(), 0);
        }
        System.out.print("> ");
    }

    private void handleRoomStateUpdate(RoomActions.RoomStateUpdateMessage rum) {
        // Replace clock and sequence entirely — same logic as rejoining
        if (rum.clock() != null) {
            roomClocks.put(rum.roomId(), rum.clock());
        }
        nextExpectedRoomSeq.put(rum.roomId(), rum.sequenceNumber());
        roomMembers.put(rum.roomId(), new HashSet<>(rum.members()));
        roomOutOfOrderBuffer.remove(rum.roomId());
        groupMessagesBuffers.clear();
        System.out.println("[Room] State updated for room " + rum.roomId()
                + " after server failover. Next seq: " + rum.sequenceNumber());
    }

    // -------------------------------------------------------------------------
    // Reconnection logic
    // -------------------------------------------------------------------------

    private void attemptReconnection() {
        running = false;
        System.out.println("Connection lost. Attempting recovery...");
        List<Integer> roomsToRejoin = new ArrayList<>(joinedRooms);
        int attempts = 0;
        while (!running && attempts < 5) {
            try {
                List<ServerInfo> servers = HttpRegistryClient.getActiveServers();
                if (!servers.isEmpty()) {
                    Collections.shuffle(servers);   // avoid thundering herd
                    connect(servers.getFirst());
                    if (running) {
                        System.out.println("Reconnected to Server " + connectedServer.id());
                        rejoinRooms(roomsToRejoin);
                        return;
                    }
                }
                attempts++;
                Thread.sleep(2000);
            } catch (Exception ignored) {}
        }
        System.out.println("Recovery failed after " + attempts + " attempts.");
    }

    private void rejoinRooms(List<Integer> rooms) {
        for (int roomId : rooms) {
            try {
                out.writeObject(new RoomActions.JoinRoomRequest(clientId, roomId, -1));
                out.flush();
            } catch (IOException ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Registry
    // -------------------------------------------------------------------------

    private int queryNextClientId() throws IOException, InterruptedException, ClassNotFoundException {
        HttpClient c = HttpClient.newHttpClient();
        HttpRequest r = HttpRequest.newBuilder()
                .uri(URI.create("http://" + RegistryService.HOST + ":" + RegistryService.PORT + "/nextClientId"))
                .GET().build();
        return (int) new ObjectInputStream(c.send(r, HttpResponse.BodyHandlers.ofInputStream()).body()).readObject();
    }

    // -------------------------------------------------------------------------
    // Legacy (raw GroupMessage, not sequenced)
    // -------------------------------------------------------------------------

    private void receiveGroupMessage(GroupMessage gm) {
        System.out.println("[RM] Received raw message for Room " + gm.groupId() + " from Client " + gm.senderId());
        VectorClock localClock = roomClockFor(gm.groupId());
        if (isDeliverable(gm, localClock)) {
            deliver(gm, localClock);
        } else {
            System.out.println("[RM Buffer] Message for Room " + gm.groupId() + " buffered (causal order).");
            groupMessagesBuffers.computeIfAbsent(gm.senderId(), _ -> new ArrayList<>()).add(gm);
        }
    }

    // -------------------------------------------------------------------------
    // State accessors
    // -------------------------------------------------------------------------

    public List<Integer> getJoinedRooms() { return new ArrayList<>(joinedRooms); }

    public Set<Integer> getRoomMembers(int roomId) {
        return new HashSet<>(roomMembers.getOrDefault(roomId, Collections.emptySet()));
    }
}