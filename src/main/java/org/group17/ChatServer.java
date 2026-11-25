package org.group17;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.group17.ChatClient.PUSH_PORT_OFFSET;

/**
 * ChatServer with:
 *  - Leader-centric replication (followers -> leader -> all)
 *  - Consensus-based failure detection (majority of active peers, longer timeout)
 *  - On failure: registry deregistration, load-balanced reassignment, client notification
 *  - Election triggers the same failure-handling logic for the old leader's resources
 *  - Undeliverable messages reported back to sender via DeliveryFailureMessage
 */
public class ChatServer {

    private final ServerInfo serverInfo;
    private final Map<Integer, ChatClientHandler> localClients = new ConcurrentHashMap<>();
    private final Map<Integer, ChatRoom> localRooms = new ConcurrentHashMap<>();

    private final Map<Integer, ClientReplica> clientReplicas = new ConcurrentHashMap<>();
    private final Map<Integer, ChatRoomReplica> roomReplicas = new ConcurrentHashMap<>();
    private final Map<Integer, ServerInfo> peers = new ConcurrentHashMap<>();
    private final Map<Integer, VectorClock> serverRoomClocks = new ConcurrentHashMap<>();

    private final Map<Integer, PeerConnection> outboundPeers = new ConcurrentHashMap<>();
    private final Map<Integer, PeerConnection> inboundPeers  = new ConcurrentHashMap<>();
    private final Map<Integer, Object> connectLocks = new ConcurrentHashMap<>();

    // Consensus Failure Detection State
    // maps suspectedId -> set of serverIds that have reported it as suspected
    private final Map<Integer, Set<Integer>> failureSuspicions = new ConcurrentHashMap<>();
    // tracks servers already confirmed dead so we don't re-process them
    private final Set<Integer> confirmedDead = ConcurrentHashMap.newKeySet();

    // Reliable ordered multicast state
    private final Map<Integer, AtomicLong> roomSequenceNumbers = new ConcurrentHashMap<>();
    private final Map<Integer, Map<Long, PendingGroupMessage>> pendingGroupMessages = new ConcurrentHashMap<>();

    private static class PendingGroupMessage {
        final SequencedGroupMessage message;
        final Set<Integer> pendingAcks;
        final ScheduledFuture<?> retransmissionTask;

        PendingGroupMessage(SequencedGroupMessage message, Set<Integer> members, ScheduledFuture<?> task) {
            this.message = message;
            this.pendingAcks = ConcurrentHashMap.newKeySet();
            this.pendingAcks.addAll(members);
            this.retransmissionTask = task;
        }
    }

    private volatile boolean isLeader = false;
    private volatile int currentLeaderId = -1;
    private volatile boolean electionActive = false;
    private volatile boolean waitingForElectionAnswer = false;
    private volatile long electionStartTime = 0;
    private final Object leaderLock = new Object();
    private final Object reassignmentLock = new Object();

    private volatile boolean running = false;
    private ServerSocket clientServerSocket;
    private ServerSocket peerServerSocket;
    private final ExecutorService clientExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Heartbeat timeout: 3 missed heartbeats + grace period
    private static final int HEARTBEAT_INTERVAL = 2000;
    private static final long PEER_TIMEOUT_MS = HEARTBEAT_INTERVAL * 2 + 500L; // 4500 ms

    private final Map<Integer, Long> peerHeartbeats = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Startup / Shutdown
    // -------------------------------------------------------------------------

    public ChatServer(String host, int port) throws IOException, ClassNotFoundException, InterruptedException {
        this.serverInfo = new ServerInfo(queryNextServerId(), host, port);
    }

    public void start() throws IOException, InterruptedException, ClassNotFoundException {
        if (running) return;
        clientServerSocket = new ServerSocket(serverInfo.port());
        peerServerSocket = new ServerSocket(serverInfo.port() + 1000);
        running = true;

        new Thread(this::acceptClientConnections, "ClientAcceptor").start();
        new Thread(this::acceptPeerConnections, "PeerAcceptor").start();

        HttpRegistryClient.registerServer(serverInfo);
        discoverPeers();
        connectToOutboundPeers();

        Thread.sleep(500); // give peers time to respond with LeaderAnnounceMessage

        for (ServerInfo peer : peers.values()) {
            peerHeartbeats.put(peer.id(), System.currentTimeMillis());
        }

        if (currentLeaderId == -1) {
            startElection();
        }

        scheduler.scheduleAtFixedRate(this::heartbeatAndReplication, 3000, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        try {
            if (clientServerSocket != null) clientServerSocket.close();
            if (peerServerSocket != null) peerServerSocket.close();
        } catch (IOException ignored) {}
        for (ChatClientHandler handler : localClients.values()) handler.stop();
        localClients.clear();
        for (PeerConnection pc : outboundPeers.values()) pc.close();
        for (PeerConnection pc : inboundPeers.values()) pc.close();
        shutdownExecutor(clientExecutor);
        shutdownExecutor(scheduler);
        try { HttpRegistryClient.deregisterServer(serverInfo.id()); } catch (Exception ignored) {}
    }

    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try { if (!executor.awaitTermination(5, TimeUnit.SECONDS)) executor.shutdownNow(); }
        catch (InterruptedException e) { executor.shutdownNow(); }
    }

    // -------------------------------------------------------------------------
    // Accept connections
    // -------------------------------------------------------------------------

    private void acceptClientConnections() {
        while (running) {
            try {
                Socket socket = clientServerSocket.accept();
                clientExecutor.submit(() -> handleClientSocket(socket));
            } catch (IOException e) { if (running) e.printStackTrace(); }
        }
    }

    private void handleClientSocket(Socket socket) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            out.writeObject("WELCOME");
            out.flush();
            int clientId = (int) in.readObject();

            ChatClientHandler handler = new ChatClientHandler(clientId, socket, this, out, in);
            addLocalClient(handler);

            // Scan local rooms directly — don't trust roomMemberships to be in sync
            for (ChatRoom room : localRooms.values()) {
                if (room.getAllMemberIds().contains(clientId)) {
                    room.addLocalMember(handler);
                    System.out.println("[Client] Client " + clientId + " re-registered as local member of room " + room.getRoomId());
                }
            }

            clientExecutor.submit(handler);
            System.out.println("[Client] Client " + clientId + " connected to Server " + serverInfo.id() + " from " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort());

            sendLocalStateToLeader();   // push our updated local state to the leader
        } catch (Exception e) { System.out.println("Handshake failed: " + e.getMessage()); }
    }

    private void acceptPeerConnections() {
        while (running) {
            try {
                Socket socket = peerServerSocket.accept();
                clientExecutor.submit(() -> handlePeerSocket(socket));
            } catch (IOException e) { if (running) e.printStackTrace(); }
        }
    }

    private void handlePeerSocket(Socket socket) {
        try {
            ObjectOutputStream tempOut = new ObjectOutputStream(socket.getOutputStream());
            tempOut.flush();
            ObjectInputStream tempIn = new ObjectInputStream(socket.getInputStream());
            Object first = tempIn.readObject();
            if (first instanceof IdentificationMessage(int id)) {
                PeerConnection pc = new PeerConnection(id, socket, tempOut, tempIn, this);
                pc.startReader();
                inboundPeers.put(id, pc);
                peerHeartbeats.put(id, System.currentTimeMillis());
                // notify new server of current leader
                if (currentLeaderId != -1) {
                    pc.send(new LeaderAnnounceMessage(currentLeaderId));
                }
            }
        } catch (Exception ignored) {}
    }

    // -------------------------------------------------------------------------
    // Client message handling
    // -------------------------------------------------------------------------

    public void handleClientObject(Object obj) {
        if (obj instanceof DirectMessage dm) {
            deliverDirectMessageOrFail(dm);
        } else if (obj instanceof GroupMessage gm) {
            sendMessageToRoom(gm);
        } else if (obj instanceof GroupMessageAck ack) {
            handleGroupMessageAck(ack);
        } else if (obj instanceof RoomActions.CreateRoomRequest req) {
            handleCreateRoom(req);
        } else if (obj instanceof RoomActions.JoinRoomRequest req) {
            handleJoinRoom(req);
        } else if (obj instanceof RoomActions.LeaveRoomRequest req) {
            handleLeaveRoom(req);
        } else if (obj instanceof RoomActions.AddMemberRequest req) {
            handleAddMember(req);
        } else if (obj instanceof RoomActions.RemoveMemberRequest req) {
            handleRemoveMember(req);
        }
    }

    // -------------------------------------------------------------------------
    // Direct message delivery with failure notification
    // -------------------------------------------------------------------------

    /**
     * Try to deliver a DM. If the target doesn't exist anywhere, notify the sender.
     * If the target is on another server, forward there; that server will do the check.
     */
    private void deliverDirectMessageOrFail(DirectMessage dm) {
        ChatClientHandler localTarget = localClients.get(dm.targetId());
        if (localTarget != null) {
            localTarget.sendObject(dm);
            return;
        }

        ServerInfo targetServer = findServerForTarget(dm.targetId());
        if (targetServer != null && targetServer.id() != serverInfo.id()) {
            // Forward to the responsible server — it will deliver or fail there
            sendToPeer(targetServer, dm);
            return;
        }

        // Target truly not found anywhere — notify the sender
        sendDeliveryFailure(dm.senderId(), dm.targetId(), "User " + dm.targetId() + " is not connected or does not exist.");
    }

    /**
     * Send a DeliveryFailureMessage to the original sender.
     * If the sender is local, send directly; otherwise forward to their server.
     */
    private void sendDeliveryFailure(int senderId, int targetId, String reason) {
        DeliveryFailureMessage failure = new DeliveryFailureMessage(senderId, targetId, reason);
        ChatClientHandler senderHandler = localClients.get(senderId);
        if (senderHandler != null) {
            senderHandler.sendObject(failure);
        } else {
            ServerInfo senderServer = findServerForTarget(senderId);
            if (senderServer != null) {
                sendToPeer(senderServer, failure);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Room management
    // -------------------------------------------------------------------------

    private void handleCreateRoom(RoomActions.CreateRoomRequest req) {
        int roomId = Math.abs(UUID.randomUUID().hashCode());
        ChatRoom room = new ChatRoom(roomId, req.roomName(), req.creatorId(), this);
        room.addMember(req.creatorId());
        addLocalRoom(room);

        // Sync creator into replica — addLocalRoom creates it with an empty member set
        ChatRoomReplica replica = roomReplicas.get(roomId);
        if (replica != null) replica.members().add(req.creatorId());
        ClientReplica clientReplica = clientReplicas.get(req.creatorId());
        if (clientReplica != null) clientReplica.roomMemberships().add(roomId);

        ChatClientHandler client = localClients.get(req.creatorId());
        if (client != null) {
            VectorClock clock = serverRoomClocks.computeIfAbsent(roomId, _ -> new VectorClock());
            long seq = 1L;
            client.sendObject(new RoomActions.CreateRoomResponse(req.requestId(), true, roomId, "Room created successfully", clock, seq));
        }
        sendLocalStateToLeader();
    }

    private void handleJoinRoom(RoomActions.JoinRoomRequest req) {
        ChatRoom room = localRooms.get(req.roomId());

        if (room != null) {
            // Determine which server this client lives on
            ServerInfo clientServer = null;
            if (localClients.containsKey(req.clientId())) {
                clientServer = serverInfo; // client is local
            } else if (req.requestingServerId() != -1) {
                clientServer = peers.get(req.requestingServerId());
            } else {
                // Fall back to existing replica
                ClientReplica existing = clientReplicas.get(req.clientId());
                if (existing != null) clientServer = existing.assignedServer();
            }

            if (clientServer != null) {
                final ServerInfo finalClientServer = clientServer;
                clientReplicas.compute(req.clientId(), (k, existing) -> {
                    if (existing == null) {
                        return new ClientReplica(k, finalClientServer, new HashSet<>(),
                                "", k + PUSH_PORT_OFFSET);
                    }
                    return new ClientReplica(k, finalClientServer, existing.roomMemberships(),
                            existing.clientHost(), existing.clientPort());
                });
            }

            room.addMember(req.clientId());
            // If client is local, make sure they're in localMembers too
            ChatClientHandler localHandler = localClients.get(req.clientId());
            System.out.println(req.clientId() + " joined room " + req.roomId() + (localHandler==null));
            if (localHandler != null) {
                room.addLocalMember(localHandler);
            }

            // Update replica membership for this client (cluster-wide tracking)
            ChatRoomReplica roomReplica = roomReplicas.get(req.roomId());
            if (roomReplica != null) roomReplica.members().add(req.clientId());
            ClientReplica clientReplica = clientReplicas.get(req.clientId());
            if (clientReplica != null) clientReplica.roomMemberships().add(req.roomId());

            sendLocalStateToLeader();

            // Reset this client's entry in the room clock to 0 so all members
            // expect their next message to start from 1 regardless of previous sessions
            VectorClock serverClock = serverRoomClocks.computeIfAbsent(req.roomId(), _ -> new VectorClock());
            serverClock.set(req.clientId(), 0);
            VectorClock clock = serverClock.snapshot();
            long currentSeq = roomSequenceNumbers.computeIfAbsent(req.roomId(), _ -> new AtomicLong(0)).get();

            // Send join response to the client (may be local or on another server)
            RoomActions.JoinRoomResponse response = new RoomActions.JoinRoomResponse(req.clientId(), true, req.roomId(), "Joined room", room.getAllMemberIds(), clock, currentSeq + 1);
            ChatClientHandler localClient = localClients.get(req.clientId());
            if (localClient != null) {
                localClient.sendObject(response);
            } else {
                ClientReplica cr = clientReplicas.get(req.clientId());
                if (cr != null) sendToPeer(cr.assignedServer(), response);
            }

            // Notify all OTHER members that this client joined, including the reset clock
            // so they update their expectation for this client's messages
            RoomActions.RoomMemberNotification joinNotif = new RoomActions.RoomMemberNotification(
                    req.roomId(), req.clientId(), true, clock);
            Set<Integer> notifiedServers = new HashSet<>();
            for (int memberId : room.getAllMemberIds()) {
                if (memberId == req.clientId()) continue;
                notifyMember(joinNotif, notifiedServers, memberId);
            }
        } else {
            // Room is not local — forward to the owning server if known
            ChatRoomReplica replica = roomReplicas.get(req.roomId());
            if (replica != null && replica.assignedServer().id() != serverInfo.id()) {
                sendToPeer(replica.assignedServer(), new RoomActions.JoinRoomRequest(req.clientId(), req.roomId(), serverInfo.id()));
            } else {
                RoomActions.JoinRoomResponse response = new RoomActions.JoinRoomResponse(req.clientId(), false,
                        req.roomId(), "Room not found", null, null, 0);
                ChatClientHandler localClient = localClients.get(req.clientId());
                if (localClient != null) {
                    localClient.sendObject(response);
                } else {
                    ClientReplica cr = clientReplicas.get(req.clientId());
                    if (cr != null) sendToPeer(cr.assignedServer(), response);
                }
            }
        }
    }

    private void handleLeaveRoom(RoomActions.LeaveRoomRequest req) {
        ChatRoom room = localRooms.get(req.roomId());
        if (room == null) {
            // Forward to the owning server
            ChatRoomReplica replica = roomReplicas.get(req.roomId());
            if (replica != null && replica.assignedServer().id() != serverInfo.id()) {
                sendToPeer(replica.assignedServer(), req);
            }
            return;
        }

        room.removeMember(req.clientId());

        // Update replica membership for this client (cluster-wide tracking)
        ChatRoomReplica roomReplica = roomReplicas.get(req.roomId());
        if (roomReplica != null) roomReplica.members().remove(req.clientId());
        ClientReplica clientReplica = clientReplicas.get(req.clientId());
        if (clientReplica != null) clientReplica.roomMemberships().remove(req.roomId());

        // Send leave response to the client
        ChatClientHandler localClient = localClients.get(req.clientId());
        if (localClient != null) {
            localClient.sendObject(new RoomActions.LeaveRoomResponse(req.clientId(), true, "Left room"));
        } else {
            ClientReplica cr = clientReplicas.get(req.clientId());
            if (cr != null) sendToPeer(cr.assignedServer(), new RoomActions.LeaveRoomResponse(req.clientId(), true, "Left room"));
        }

        // Notify all REMAINING members that this client left
        RoomActions.RoomMemberNotification leaveNotif = new RoomActions.RoomMemberNotification(req.roomId(), req.clientId(), false, null);
        Set<Integer> notifiedServers = new HashSet<>();
        for (int memberId : room.getAllMemberIds()) {
            notifyMember(leaveNotif, notifiedServers, memberId);
        }

        sendLocalStateToLeader();
    }

    private void notifyMember(RoomActions.RoomMemberNotification leaveNotif, Set<Integer> notifiedServers, int memberId) {
        ChatClientHandler localMember = localClients.get(memberId);
        if (localMember != null) {
            localMember.sendObject(leaveNotif);
        } else {
            ClientReplica cr = clientReplicas.get(memberId);
            if (cr != null && notifiedServers.add(cr.assignedServer().id())) {
                sendToPeer(cr.assignedServer(), leaveNotif);
            }
        }
    }

    private void handleAddMember(RoomActions.AddMemberRequest req) {
        ChatRoom room = localRooms.get(req.roomId());
        if (room != null) {
            room.addMember(req.targetClientId());
            ChatRoomReplica replica = roomReplicas.get(req.roomId());
            if (replica != null) replica.members().add(req.targetClientId());

            ChatClientHandler requester = localClients.get(req.requesterId());
            if (requester != null) {
                requester.sendObject(new RoomActions.AddMemberResponse(true, "Member added"));
            }
            sendLocalStateToLeader();
        }
    }

    private void handleRemoveMember(RoomActions.RemoveMemberRequest req) {
        ChatRoom room = localRooms.get(req.roomId());
        if (room != null) {
            room.removeMember(req.targetClientId());
            ChatRoomReplica replica = roomReplicas.get(req.roomId());
            if (replica != null) replica.members().remove(req.targetClientId());

            ChatClientHandler requester = localClients.get(req.requesterId());
            if (requester != null) {
                requester.sendObject(new RoomActions.RemoveMemberResponse(true, "Member removed"));
            }
            sendLocalStateToLeader();
        }
    }

    // -------------------------------------------------------------------------
    // Peer message handling
    // -------------------------------------------------------------------------

    public void handlePeerObject(Object obj, int senderServerId) {
        if (obj instanceof HeartbeatMessage(int id)) {
            peerHeartbeats.put(id, System.currentTimeMillis());
            failureSuspicions.remove(id);          // clear suspicions when heartbeat arrives

        } else if (obj instanceof ReplicationMessage rm) {
            applyReplication(rm);

        } else if (obj instanceof SuspectFailureMessage sfm) {
            handleSuspectFailure(sfm);

        } else if (obj instanceof DirectMessage dm) {
            // Forwarded DM — try local delivery or report failure
            deliverDirectMessageOrFail(dm);

        } else if (obj instanceof GroupMessage gm) {
            receiveForDelivery(gm.targetId(), gm);

        } else if (obj instanceof DeliveryFailureMessage dfm) {
            // Another server couldn't deliver — forward to the sender if they're local
            ChatClientHandler sender = localClients.get(dfm.senderId());
            if (sender != null) sender.sendObject(dfm);

        } else if (obj instanceof ElectionMessage em) {
            handleElectionMessage(em);

        } else if (obj instanceof SequencedGroupMessage sm) {
            handleIncomingSequencedMessage(sm);

        } else if (obj instanceof GroupMessageAck ack) {
            handleGroupMessageAck(ack);

        } else if (obj instanceof ReassignmentNotification(
                Map<Integer, ClientReplica> updatedClients, Map<Integer, ChatRoomReplica> updatedRooms
        )) {
            clientReplicas.putAll(updatedClients);
            roomReplicas.putAll(updatedRooms);
        } else if (obj instanceof LeaderAnnounceMessage(int leaderId)) {
            if (currentLeaderId == -1) {   // only accept if we don't already know a leader
                currentLeaderId = leaderId;
                isLeader = (currentLeaderId == serverInfo.id());
                System.out.println("[Leader] Current leader is Server " + currentLeaderId + (isLeader ? " (that's me!)" : ""));
            }
        } else if (obj instanceof PushReassignmentMessage(
                ClientReplica clientReplica, ServerInfo newServer, List<RoomActions.RoomStateUpdateMessage> roomStates
        )) {
            pushReassignmentToClient(clientReplica, newServer, roomStates);
        } else if (obj instanceof RoomActions.JoinRoomRequest req) {
            handleJoinRoom(new RoomActions.JoinRoomRequest(req.clientId(), req.roomId(), senderServerId));
        } else if (obj instanceof RoomActions.JoinRoomResponse res) {
            ChatClientHandler client = localClients.get(res.clientId());
            if (client != null) client.sendObject(res);
        } else if (obj instanceof RoomActions.RoomMemberNotification notif) {
            ChatRoomReplica roomReplica = roomReplicas.get(notif.roomId());
            Set<Integer> members = roomReplica != null ? roomReplica.members() : Collections.emptySet();
            for (ChatClientHandler handler : localClients.values()) {
                if (members.contains(handler.getClientId())) {
                    handler.sendObject(notif);
                }
            }
        } else if (obj instanceof RoomActions.LeaveRoomRequest req) {
            handleLeaveRoom(req);
        } else if (obj instanceof RoomActions.RoomStateUpdateMessage rum) {
            // Forwarded from leader — deliver to all local clients who are members
            for (int memberId : rum.members()) {
                ChatClientHandler local = localClients.get(memberId);
                if (local != null) local.sendObject(rum);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Group messaging (reliable ordered multicast)
    // -------------------------------------------------------------------------

    private void handleIncomingSequencedMessage(SequencedGroupMessage seqMsg) {
        int roomId = seqMsg.roomId();
        ChatRoomReplica roomReplica = roomReplicas.get(roomId);
        Set<Integer> members = roomReplica != null ? roomReplica.members() : Collections.emptySet();
        for (ChatClientHandler handler : localClients.values()) {
            if (members.contains(handler.getClientId())) {
                handler.sendObject(seqMsg);
            }
        }
    }

    public void sendMessageToRoom(GroupMessage groupMessage) {
        ChatRoom room = localRooms.get(groupMessage.groupId());
        if (room == null) {
            // Try to find which server owns the room and forward there
            ServerInfo roomServer = findServerForTarget(groupMessage.groupId());
            if (roomServer != null && roomServer.id() != serverInfo.id()) {
                sendToPeer(roomServer, groupMessage);
            } else {
                // Room doesn't exist anywhere — notify sender
                sendDeliveryFailure(groupMessage.senderId(), groupMessage.groupId(), "Room " + groupMessage.groupId() + " does not exist.");
            }
            return;
        }

        VectorClock serverClock = serverRoomClocks.computeIfAbsent(groupMessage.groupId(), _ -> new VectorClock());
        serverClock.merge(groupMessage.timestamp());

        //room.broadcastMessage(groupMessage);
        long seqNum = roomSequenceNumbers.computeIfAbsent(groupMessage.groupId(), _ -> new AtomicLong(0)).incrementAndGet();
        // update the replica so peers get the latest sequence number
        ChatRoomReplica existing = roomReplicas.get(groupMessage.groupId());
        if (existing != null) {
            roomReplicas.put(groupMessage.groupId(), new ChatRoomReplica(existing.roomId(), existing.assignedServer(), new HashSet<>(existing.members()), new ArrayList<>(existing.messageLog()), seqNum));
        }
        SequencedGroupMessage seqMsg = new SequencedGroupMessage(groupMessage, seqNum, groupMessage.groupId());

        ChatRoomReplica r = roomReplicas.get(groupMessage.groupId());
        if (r != null) r.messageLog().add(groupMessage);

        Set<Integer> members = new HashSet<>(room.getAllMemberIds());
        System.out.println("Message to group " + members + " from " + groupMessage.senderId());
        members.remove(groupMessage.senderId());
        ScheduledFuture<?> task = scheduler.schedule(() -> retransmitGroupMessage(seqMsg, groupMessage.groupId()), 3000, TimeUnit.MILLISECONDS);
        pendingGroupMessages.computeIfAbsent(groupMessage.groupId(), _ -> new ConcurrentHashMap<>()).put(seqNum, new PendingGroupMessage(seqMsg, members, task));
        broadcastSequencedMessage(room, seqMsg);
    }

    private void broadcastSequencedMessage(ChatRoom room, SequencedGroupMessage seqMsg) {
        System.out.println("[Broadcast] Local members: " + room.getLocalMembers().keySet() + " all members: " + room.getAllMemberIds());
        for (ChatClientHandler c : room.getLocalMembers().values()) c.sendObject(seqMsg);
        Set<Integer> notifiedServers = new HashSet<>();
        for (int mId : room.getAllMemberIds()) {
            if (!room.getLocalMembers().containsKey(mId)) {
                ServerInfo s = findServerForTarget(mId);
                if (s != null && !notifiedServers.contains(s.id())) {
                    sendToPeer(s, seqMsg);
                    notifiedServers.add(s.id());
                } else {
                    System.out.println("[Broadcast] No server found");
                }
            }
        }
    }

    private void retransmitGroupMessage(SequencedGroupMessage seqMsg, int roomId) {
        Map<Long, PendingGroupMessage> roomPending = pendingGroupMessages.get(roomId);
        if (roomPending == null) return;
        PendingGroupMessage p = roomPending.get(seqMsg.sequenceNumber());
        if (p == null || p.pendingAcks.isEmpty()) return;
        for (int mId : p.pendingAcks) {
            ChatClientHandler h = localClients.get(mId);
            if (h != null) h.sendObject(seqMsg);
            else {
                ServerInfo s = findServerForTarget(mId);
                if (s != null) sendToPeer(s, seqMsg);
            }
        }
        scheduler.schedule(() -> retransmitGroupMessage(seqMsg, roomId), 3000, TimeUnit.MILLISECONDS);
    }

    private void handleGroupMessageAck(GroupMessageAck ack) {
        Map<Long, PendingGroupMessage> roomPending = pendingGroupMessages.get(ack.roomId());
        if (roomPending == null) return;
        PendingGroupMessage p = roomPending.get(ack.sequenceNumber());
        if (p != null) {
            p.pendingAcks.remove(ack.clientId());
            if (p.pendingAcks.isEmpty()) {
                p.retransmissionTask.cancel(false);
                roomPending.remove(ack.sequenceNumber());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Replication — leader-centric model
    //
    //  Followers push their LOCAL state snapshot to the leader.
    //  The leader merges everything and broadcasts the full picture to all.
    // -------------------------------------------------------------------------

    /**
     * Called by followers: send only THIS server's locally-owned replicas to the leader.
     */
    private void sendLocalStateToLeader() {
        if (isLeader) {
            // I am the leader — just broadcast the current full picture
            broadcastFullReplication();
            return;
        }
        if (currentLeaderId == -1) return;
        ServerInfo leader = peers.get(currentLeaderId);
        if (leader == null) return;

        // Build a partial ReplicationMessage containing only resources this server owns
        Map<Integer, ClientReplica> myClients = new HashMap<>();
        for (Map.Entry<Integer, ClientReplica> e : clientReplicas.entrySet()) {
            if (e.getValue().assignedServer().id() == serverInfo.id()) myClients.put(e.getKey(), e.getValue());
        }
        Map<Integer, ChatRoomReplica> myRooms = new HashMap<>();
        for (Map.Entry<Integer, ChatRoomReplica> e : roomReplicas.entrySet()) {
            if (e.getValue().assignedServer().id() == serverInfo.id()) {
                ChatRoomReplica r = e.getValue();
                myRooms.put(e.getKey(), new ChatRoomReplica(r.roomId(), r.assignedServer(), new HashSet<>(r.members()), new ArrayList<>(r.messageLog()), r.sequenceNumber()));
            }
        }
        sendToPeer(leader, new ReplicationMessage(myClients, myRooms));
    }

    /**
     * Called by the leader: broadcast the FULL merged replica map to every peer.
     */
    private void broadcastFullReplication() {
        Map<Integer, ChatRoomReplica> roomsCopy = new HashMap<>();
        for (Map.Entry<Integer, ChatRoomReplica> e : roomReplicas.entrySet()) {
            ChatRoomReplica r = e.getValue();
            roomsCopy.put(e.getKey(), new ChatRoomReplica(r.roomId, r.assignedServer(), new HashSet<>(r.members()), new ArrayList<>(r.messageLog()), r.sequenceNumber()));
        }
        ReplicationMessage rm = new ReplicationMessage(new HashMap<>(clientReplicas), roomsCopy);

        for (ServerInfo p : peers.values()) {
            if (p.id() != serverInfo.id()) sendToPeer(p, rm);
            System.out.println("leader replicated to " + p.id() + ", " + rm);
        }
        System.out.println(roomReplicas);
    }

    /**
     * applyReplication is called when a ReplicationMessage arrives.
     *  - If I am the leader: a follower pushed their partial state to me.
     *    Merge it in, then broadcast the full picture.
     *  - If I am a follower: the leader pushed the full picture to me. Just apply it.
     */
    private void applyReplication(ReplicationMessage rm) {
        clientReplicas.putAll(rm.clientData());
        roomReplicas.putAll(rm.roomData());
        if (isLeader) {
            broadcastFullReplication();
        } else {
            System.out.println("leader replicated with " + rm);
        }
    }

    // -------------------------------------------------------------------------
    // Heartbeat / failure detection loop
    // -------------------------------------------------------------------------

    private void heartbeatAndReplication() {
        if (!running) return;
        try { discoverPeers(); } catch (Exception e) {
            System.out.println("Failed to refresh peers: " + e.getMessage());
        }

        sendHeartbeats();
        checkPeerLivenessAndConsensus();
        checkLeaderAndHandleFailures();
        sendLocalStateToLeader();
    }

    private void sendHeartbeats() {
        HeartbeatMessage hm = new HeartbeatMessage(serverInfo.id());
        for (ServerInfo peer : peers.values()) sendToPeer(peer, hm);
    }

    private void checkPeerLivenessAndConsensus() {
        long now = System.currentTimeMillis();

        for (ServerInfo peer : peers.values()) {
            if (peer.id() == serverInfo.id()) continue;
            if (confirmedDead.contains(peer.id())) continue;

            long last = peerHeartbeats.getOrDefault(peer.id(), 0L);
            if (now - last > PEER_TIMEOUT_MS) {
                Set<Integer> voters = failureSuspicions.computeIfAbsent(peer.id(), _ -> ConcurrentHashMap.newKeySet());
                boolean added = voters.add(serverInfo.id());
                if (added) {
                    System.out.println("[Failure] Server " + peer.id() + " suspected by Server " + serverInfo.id());
                    SuspectFailureMessage msg = new SuspectFailureMessage(peer.id(), serverInfo.id());
                    for (ServerInfo p : peers.values()) {
                        if (p.id() != serverInfo.id()) sendToPeer(p, msg);
                    }
                }
            }
        }
    }

    private void handleSuspectFailure(SuspectFailureMessage msg) {
        if (confirmedDead.contains(msg.suspectedId())) return;
        Set<Integer> voters = failureSuspicions.computeIfAbsent(msg.suspectedId(), _ -> ConcurrentHashMap.newKeySet());
        boolean added = voters.add(msg.reporterId());
        if (added) {
            System.out.println("[Failure] Server " + msg.suspectedId() + " suspected by " + voters.size() + " peer(s).");
        }
    }

    private void checkLeaderAndHandleFailures() {
        if (electionActive) {
            if (System.currentTimeMillis() - electionStartTime > 15000) {
                electionActive = false;
                startElection();
            }
            return;
        }

        // Non-leader: detect leader failure via consensus
        if (currentLeaderId != -1 && currentLeaderId != serverInfo.id()) {
            if (isPeerConsensusDead(currentLeaderId)) {
                System.out.println("Consensus: Leader " + currentLeaderId + " is dead. Starting election.");
                int oldLeaderId = currentLeaderId;
                confirmedDead.add(oldLeaderId);
                currentLeaderId = -1;
                startElection();
                return;
            }
        }

        // Leader: detect and handle failed peers
        if (isLeader) {
            List<Integer> deadIds = new ArrayList<>();
            for (Integer peerId : peers.keySet()) {
                if (peerId != serverInfo.id() && !confirmedDead.contains(peerId) && isPeerConsensusDead(peerId)) {
                    deadIds.add(peerId);
                }
            }
            for (Integer id : deadIds) {
                confirmedDead.add(id);
                ServerInfo failed = peers.remove(id);
                if (failed != null) {
                    System.out.println("Leader: Confirmed failure of server " + id + ". Handling reassignment.");
                    handleServerFailure(failed);
                }
            }
        }
    }

    /**
     * Returns true when a strict majority of currently-known active peers
     * (not counting the suspected peer itself, and not counting already-dead ones)
     * have reported the peer as suspected.
     */
    private boolean isPeerConsensusDead(int peerId) {
        Set<Integer> voters = failureSuspicions.get(peerId);
        if (voters == null || voters.isEmpty()) return false;

        // Count only active (non-dead, non-suspected) participants
        long activePeerCount = peers.values().stream().filter(s -> s.id() != peerId && !confirmedDead.contains(s.id())).count();

        // Need strict majority of those active participants to have voted
        return voters.stream().filter(v -> !confirmedDead.contains(v)).count() > (activePeerCount / 2);
    }

    // -------------------------------------------------------------------------
    // Server failure handling (deregister + reassign + notify clients)
    // -------------------------------------------------------------------------

    private void handleServerFailure(ServerInfo failed) {
        synchronized (reassignmentLock) {
            // 1. Deregister the failed server from the registry
            try {
                HttpRegistryClient.deregisterServer(failed.id());
                System.out.println("Leader: Deregistered failed server " + failed.id() + " from registry.");
            } catch (Exception e) {
                System.out.println("Leader: Could not deregister server " + failed.id() + ": " + e.getMessage());
            }

            // 2. Reassign rooms and clients with load balancing
            Map<Integer, ClientReplica> updClients = new HashMap<>();
            Map<Integer, ChatRoomReplica> updRooms = new HashMap<>();

            // Reassign rooms
            for (Map.Entry<Integer, ChatRoomReplica> entry : new HashMap<>(roomReplicas).entrySet()) {
                if (entry.getValue().assignedServer().id() == failed.id()) {
                    ServerInfo next = selectNewServerLoadBalanced(failed);
                    ChatRoomReplica nr = new ChatRoomReplica(entry.getKey(), next, new HashSet<>(entry.getValue().members()), new ArrayList<>(entry.getValue().messageLog()), entry.getValue().sequenceNumber());
                    roomReplicas.put(entry.getKey(), nr);
                    updRooms.put(entry.getKey(), nr);

                    if (next.id() == serverInfo.id()) {
                        ChatRoom room = new ChatRoom(nr.roomId(), "Recovered", -1, this);
                        for (int mId : nr.members()) {
                            room.addMember(mId);
                            // Register any already-connected local clients as local members
                            ChatClientHandler localHandler = localClients.get(mId);
                            if (localHandler != null) room.addLocalMember(localHandler);
                        }
                        room.getMessageHistory().addAll(nr.messageLog());
                        localRooms.put(nr.roomId(), room);
                        roomSequenceNumbers.computeIfAbsent(nr.roomId(), _ -> new AtomicLong(0)).set(nr.sequenceNumber());

                        VectorClock clock = serverRoomClocks.getOrDefault(nr.roomId(), new VectorClock());
                        RoomActions.RoomStateUpdateMessage update = new RoomActions.RoomStateUpdateMessage(nr.roomId(), clock, nr.sequenceNumber() + 1, nr.members());
                        for (int memberId : nr.members()) {
                            ChatClientHandler local = localClients.get(memberId);
                            if (local != null) {
                                local.sendObject(update);
                            } else {
                                ClientReplica cr = clientReplicas.get(memberId);
                                if (cr != null) {
                                    if (cr.assignedServer().id() == serverInfo.id()) {
                                        pushMessageToClient(cr, update);
                                    } else {
                                        sendToPeer(cr.assignedServer(), update);
                                    }
                                }
                            }
                        }
                    }
                    System.out.println("[Reassignment] Room " + entry.getKey() + " -> Server " + next.id());
                }
            }

            // Reassign clients and collect who needs to be notified
            Map<Integer, ServerInfo> clientNotifications = new HashMap<>();
            for (Map.Entry<Integer, ClientReplica> entry : new HashMap<>(clientReplicas).entrySet()) {
                if (entry.getValue().assignedServer().id() == failed.id()) {
                    ServerInfo next = selectNewServerLoadBalanced(failed);
                    ClientReplica updated = new ClientReplica(entry.getKey(), next, entry.getValue().roomMemberships(), entry.getValue().clientHost(), entry.getValue().clientPort());
                    clientReplicas.put(entry.getKey(), updated);
                    updClients.put(entry.getKey(), updated);
                    clientNotifications.put(entry.getKey(), next);
                    System.out.println("[Reassignment] Client " + entry.getKey() + " -> Server " + next.id());
                }
            }

            // For clients reassigned to us, register them as local members in their rooms
            for (Map.Entry<Integer, ServerInfo> e : clientNotifications.entrySet()) {
                if (e.getValue().id() != serverInfo.id()) continue;
                int reassignedClientId = e.getKey();
                ClientReplica replica = clientReplicas.get(reassignedClientId);
                if (replica == null) continue;
                ChatClientHandler localHandler = localClients.get(reassignedClientId);
                if (localHandler == null) continue; // not reconnected yet — handled on connect
                for (int roomId : replica.roomMemberships()) {
                    ChatRoom room = localRooms.get(roomId);
                    if (room != null) room.addLocalMember(localHandler);
                }
            }

            // 3. Broadcast updated replica map to all surviving peers
            ReassignmentNotification notification = new ReassignmentNotification(updClients, updRooms);
            for (ServerInfo p : peers.values()) {
                if (p.id() != serverInfo.id()) sendToPeer(p, notification);
            }
            clientReplicas.putAll(updClients);
            roomReplicas.putAll(updRooms);

            // 4. Notify each affected client via push connection
            for (Map.Entry<Integer, ServerInfo> e : clientNotifications.entrySet()) {
                int affectedClientId = e.getKey();
                ServerInfo newServer = e.getValue();
                ClientReplica replica = clientReplicas.get(affectedClientId);
                if (replica == null) continue;

                List<RoomActions.RoomStateUpdateMessage> roomStates = new ArrayList<>();
                for (int roomId : replica.roomMemberships()) {
                    ChatRoomReplica roomReplica = roomReplicas.get(roomId);
                    if (roomReplica != null) {
                        VectorClock clock = serverRoomClocks.getOrDefault(roomId, new VectorClock());
                        roomStates.add(new RoomActions.RoomStateUpdateMessage(roomId, clock, roomReplica.sequenceNumber() + 1, roomReplica.members()));
                    }
                }

                if (newServer.id() == serverInfo.id()) {
                    pushReassignmentToClient(replica, newServer, roomStates);
                } else {
                    sendToPeer(newServer, new PushReassignmentMessage(replica, newServer, roomStates));
                }
            }

            System.out.println("[Reassignment] Done. " + updClients.size() + " client(s) and " + updRooms.size() + " room(s) reassigned from failed Server " + failed.id() + ".");
        }
    }

    private void pushReassignmentToClient(ClientReplica replica, ServerInfo newServer, List<RoomActions.RoomStateUpdateMessage> roomStates) {
        System.out.println("[Reassignment] Pushing reassignment to Client " + replica.clientId() + " -> Server " + newServer.id());
        clientExecutor.submit(() -> {
            try (Socket s = new Socket(replica.clientHost(), replica.clientPort())) {
                ObjectOutputStream o = new ObjectOutputStream(s.getOutputStream());
                o.flush();
                o.writeObject(new ServerReassignmentMessage(newServer));
                o.flush();
                for (RoomActions.RoomStateUpdateMessage rum : roomStates) {
                    o.writeObject(rum);
                    o.flush();
                }
            } catch (IOException e) {
                System.out.println("[Push] Could not reach Client " + replica.clientId() + " — " + e.getMessage());
            }
        });
    }

    private void pushMessageToClient(ClientReplica replica, Object message) {
        clientExecutor.submit(() -> {
            try (Socket s = new Socket(replica.clientHost(), replica.clientPort())) {
                ObjectOutputStream o = new ObjectOutputStream(s.getOutputStream());
                o.flush();
                o.writeObject(message);
                o.flush();
            } catch (IOException e) {
                System.out.println("[Push] Could not reach Client " + replica.clientId()
                        + " at " + replica.clientHost() + ":" + replica.clientPort()
                        + " — " + e.getMessage());
            }
        });
    }

    /**
     * Load-balanced server selection: pick the surviving server with the fewest assigned clients.
     */
    private ServerInfo selectNewServerLoadBalanced(ServerInfo failed) {
        // Count how many clients each surviving server currently holds
        Map<Integer, Long> load = new HashMap<>();
        for (ServerInfo s : peers.values()) {
            if (!s.equals(failed) && !confirmedDead.contains(s.id())) {
                load.put(s.id(), 0L);
            }
        }
        // Count current assignments (excluding the dead server)
        for (ClientReplica cr : clientReplicas.values()) {
            if (!cr.assignedServer().equals(failed) && !confirmedDead.contains(cr.assignedServer().id())) {
                load.merge(cr.assignedServer().id(), 1L, Long::sum);
            }
        }

        // I am always a valid candidate
        load.putIfAbsent(serverInfo.id(), 0L);

        return load.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(e -> {
                    if (e.getKey() == serverInfo.id()) return serverInfo;
                    ServerInfo peer = peers.get(e.getKey());
                    return peer != null ? peer : serverInfo;
                })
                .orElse(serverInfo);
    }

    // -------------------------------------------------------------------------
    // Election (Bully algorithm)
    // -------------------------------------------------------------------------

    private synchronized void startElection() {
        if (electionActive) return;
        System.out.println("[Election] Server " + serverInfo.id() + " is starting an election.");
        electionActive = true;
        electionStartTime = System.currentTimeMillis();
        List<ServerInfo> higher = peers.values().stream()
                .filter(p -> p.id() > serverInfo.id() && !confirmedDead.contains(p.id()))
                .toList();
        boolean sent = false;
        for (ServerInfo p : higher) if (sendElectionMessage(p.id(), ElectionMessage.Type.ELECTION)) sent = true;
        if (sent) {
            waitingForElectionAnswer = true;
            scheduler.schedule(this::checkElectionTimeout, 5, TimeUnit.SECONDS);
        } else {
            becomeLeader();
        }
    }

    private void handleElectionMessage(ElectionMessage msg) {
        synchronized (leaderLock) {
            switch (msg.type()) {
                case ELECTION -> {
                    if (msg.senderId() < serverInfo.id()) {
                        sendElectionMessage(msg.senderId(), ElectionMessage.Type.ANSWER);
                        startElection();
                    }
                }
                case ANSWER -> waitingForElectionAnswer = false;
                case COORDINATOR -> {
                    currentLeaderId = msg.senderId();
                    isLeader = (currentLeaderId == serverInfo.id());
                    electionActive = false;
                    System.out.println("[Leader] New leader elected: Server " + currentLeaderId + (isLeader ? " (that's me!)" : ""));
                }
            }
        }
    }

    private void becomeLeader() {
        isLeader = true;
        currentLeaderId = serverInfo.id();
        electionActive = false;
        System.out.println("[Leader] Server " + serverInfo.id() + " is now the leader.");
        for (ServerInfo p : peers.values()) sendElectionMessage(p.id(), ElectionMessage.Type.COORDINATOR);

        // Deregister all confirmed dead servers from the registry (including the previous leader)
        for (Integer deadId : confirmedDead) {
            try {
                HttpRegistryClient.deregisterServer(deadId);
                System.out.println("[Leader] Deregistered dead server " + deadId + " from registry.");
            } catch (Exception e) {
                System.out.println("[Leader] Could not deregister server " + deadId + ": " + e.getMessage());
            }
        }

        // Clean up orphaned resources still assigned to dead servers
        for (Integer deadId : confirmedDead) {
            ServerInfo deadInfo = peers.getOrDefault(deadId, new ServerInfo(deadId, "unknown", -1));
            boolean hasOrphans = clientReplicas.values().stream().anyMatch(r -> r.assignedServer().id() == deadId)
                    || roomReplicas.values().stream().anyMatch(r -> r.assignedServer().id() == deadId);
            if (hasOrphans) {
                System.out.println("[Leader] Cleaning up orphans for dead server " + deadId);
                handleServerFailure(deadInfo);
            }
        }
    }

    private void checkElectionTimeout() {
        if (electionActive && waitingForElectionAnswer) becomeLeader();
    }

    private boolean sendElectionMessage(int tId, ElectionMessage.Type type) {
        ServerInfo t = peers.get(tId);
        return t != null && sendToPeer(t, new ElectionMessage(type, serverInfo.id()));
    }

    // -------------------------------------------------------------------------
    // Peer connectivity
    // -------------------------------------------------------------------------

    private boolean sendToPeer(ServerInfo p, Object o) {
        PeerConnection pc = outboundPeers.get(p.id());
        if (pc != null && pc.isAlive()) return pc.send(o);
        connectOutbound(p);
        pc = outboundPeers.get(p.id());
        return pc != null && pc.send(o);
    }

    private void connectToOutboundPeers() {
        for (ServerInfo peer : peers.values()) connectOutbound(peer);
    }

    private void connectOutbound(ServerInfo p) {
        synchronized (connectLocks.computeIfAbsent(p.id(), _ -> new Object())) {
            try {
                Socket s = new Socket(p.host(), p.port() + 1000);
                PeerConnection pc = new PeerConnection(p.id(), s, this);
                pc.sendIdentification();
                pc.startReader();
                outboundPeers.put(p.id(), pc);
            } catch (IOException ignored) {}
        }
    }

    private void discoverPeers() throws IOException, InterruptedException, ClassNotFoundException {
        List<ServerInfo> disc = HttpRegistryClient.getActiveServers();
        for (ServerInfo si : disc) {
            if (si.id() != serverInfo.id() && !confirmedDead.contains(si.id())) {
                if (!peers.containsKey(si.id())) {
                    peerHeartbeats.put(si.id(), System.currentTimeMillis());
                    System.out.println("Discovered new peer: Server " + si.id());
                    connectOutbound(si);
                }
                peers.put(si.id(), si);
            }
        }
    }

    private int queryNextServerId() throws IOException, ClassNotFoundException, InterruptedException {
        return HttpRegistryClient.fetchNextId("/nextServerId");
    }

    // -------------------------------------------------------------------------
    // Message routing helpers
    // -------------------------------------------------------------------------
    public void receiveForDelivery(int tId, GroupMessage gm) {
        if (tId == -1) {
            if (localRooms.containsKey(gm.groupId())) {
                sendMessageToRoom(gm);
            } else {
                ServerInfo s = findServerForTarget(gm.groupId());
                if (s != null) sendToPeer(s, gm);
                else sendDeliveryFailure(gm.senderId(), gm.groupId(),
                        "Room " + gm.groupId() + " does not exist.");
            }
        } else {
            ChatClientHandler h = localClients.get(tId);
            if (h != null) {
                h.sendObject(gm);
            } else {
                ServerInfo s = findServerForTarget(tId);
                if (s != null) sendToPeer(s, gm);
                else sendDeliveryFailure(gm.senderId(), gm.groupId(),
                        "User " + tId + " is not connected or does not exist.");
            }
        }
    }

    private ServerInfo findServerForTarget(int id) {
        ClientReplica cr = clientReplicas.get(id);
        if (cr != null && !confirmedDead.contains(cr.assignedServer().id())) return cr.assignedServer();
        ChatRoomReplica rr = roomReplicas.get(id);
        if (rr != null && !confirmedDead.contains(rr.assignedServer().id())) return rr.assignedServer();
        return null;
    }

    // -------------------------------------------------------------------------
    // Local state management
    // -------------------------------------------------------------------------

    public void addLocalClient(ChatClientHandler h) {
        localClients.put(h.getClientId(), h);
        String host = h.getSocket().getInetAddress().getHostAddress();
        int pushPort = h.getClientId() + PUSH_PORT_OFFSET;
        clientReplicas.put(h.getClientId(), new ClientReplica(h.getClientId(), serverInfo, new HashSet<>(), host, pushPort));
    }

    public void addLocalRoom(ChatRoom r) {
        localRooms.put(r.getRoomId(), r);
        roomReplicas.put(r.getRoomId(), new ChatRoomReplica(r.getRoomId(), serverInfo, new HashSet<>(), new ArrayList<>(), 0L));
    }

    public ChatClientHandler getLocalClient(int id) { return localClients.get(id); }

    public void handleClientDisconnect(int clientId) {
        localClients.remove(clientId);

        for (ChatRoom room : localRooms.values()) {
            if (room.getAllMemberIds().contains(clientId)) {
                room.removeMember(clientId);

                ChatRoomReplica roomReplica = roomReplicas.get(room.getRoomId());
                if (roomReplica != null) roomReplica.members().remove(clientId);

                RoomActions.RoomMemberNotification leaveNotif = new RoomActions.RoomMemberNotification(
                        room.getRoomId(), clientId, false, null);
                Set<Integer> notifiedServers = new HashSet<>();
                for (int memberId : room.getAllMemberIds()) {
                    notifyMember(leaveNotif, notifiedServers, memberId);
                }
            }
        }

        clientReplicas.remove(clientId);  // don't reassign dead clients on future failovers
        sendLocalStateToLeader();
        System.out.println("[Disconnect] Client " + clientId + " disconnected and cleaned up.");
    }

    public void handlePeerDisconnect(int peerId) {
        outboundPeers.remove(peerId);
        inboundPeers.remove(peerId);
        peerHeartbeats.remove(peerId);
        System.out.println("[Disconnect] Peer connection to Server " + peerId + " closed.");
    }

    public int getServerId() { return serverInfo.id(); }

    // -------------------------------------------------------------------------
    // Serializable types
    // -------------------------------------------------------------------------

    public record ClientReplica(int clientId, ServerInfo assignedServer, Set<Integer> roomMemberships, String clientHost, int clientPort) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record ChatRoomReplica(int roomId, ServerInfo assignedServer, Set<Integer> members, List<GroupMessage> messageLog, long sequenceNumber) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record ReassignmentNotification(Map<Integer, ClientReplica> updatedClients, Map<Integer, ChatRoomReplica> updatedRooms) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record IdentificationMessage(int serverId) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record SuspectFailureMessage(int suspectedId, int reporterId) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record ServerReassignmentMessage(ServerInfo newServer) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record LeaderAnnounceMessage(int leaderId) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    public record PushReassignmentMessage(ClientReplica clientReplica, ServerInfo newServer, List<RoomActions.RoomStateUpdateMessage> roomStates) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }

    /**
     * Sent to a client (or forwarded through servers) when a message could not
     * be delivered to its intended target.
     */
    public record DeliveryFailureMessage(int senderId, int targetId, String reason) implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
    }
}