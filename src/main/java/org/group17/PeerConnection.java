package org.group17;

import java.io.*;
import java.net.*;

public class PeerConnection {

    private final int peerId;
    private final Socket socket;
    private final ChatServer server;

    private final ObjectOutputStream out;
    private final ObjectInputStream in;

    private volatile boolean alive = true;
    private Thread readerThread;


    public PeerConnection(int peerId, Socket socket, ChatServer server) throws IOException {
        this.peerId = peerId;
        this.socket = socket;
        this.server = server;

        // ObjectOutputStream first — flush immediately so the remote
        // ObjectInputStream can read the stream header without blocking
        this.out = new ObjectOutputStream(socket.getOutputStream());
        this.out.flush();
        this.in  = new ObjectInputStream(socket.getInputStream());
    }

    public PeerConnection(int peerId, Socket socket, ObjectOutputStream out, ObjectInputStream in, ChatServer server) {
        this.peerId = peerId;
        this.socket = socket;
        this.server = server;
        this.out = out;
        this.in  = in;
    }

    // -------------------- Lifecycle --------------------
    public void sendIdentification() throws IOException {
        synchronized (out) {
            out.writeObject(new ChatServer.IdentificationMessage(server.getServerId()));
            out.flush();
        }
    }

    public void startReader() {
        readerThread = new Thread(() -> {
            try {
                while (alive) {
                    Object obj = in.readObject();   // blocks until data arrives
                    server.handlePeerObject(obj, peerId);
                }
            } catch (IOException | ClassNotFoundException e) {
                // Only log if this was unexpected (not a normal connection close)
                if (alive) {
                    // During normal shutdown, we expect EOF and SocketExceptions
                    boolean isExpectedShutdown = (e instanceof EOFException) ||
                            (e instanceof SocketException) ||
                            socket.isClosed();

                    if (!isExpectedShutdown) {
                        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                        System.out.println("PeerConnection from " + server.getServerId() + " to " + peerId +
                                " lost unexpectedly: " + msg);
                    }
                }
            } finally {
                close();
                server.handlePeerDisconnect(peerId);
            }
        }, "PeerReader-" + peerId);

        readerThread.setDaemon(true);
        readerThread.start();
    }

    // -------------------- Send --------------------
    public boolean send(Object obj) {
        if (!alive) return false;
        try {
            synchronized (out) {
                out.writeObject(obj);
                out.flush();
            }
            return true;
        } catch (IOException e) {
            System.out.println("PeerConnection send to " + peerId + " failed: " + e.getMessage());
            close();
            return false;
        }
    }

    // -------------------- State --------------------

    public int  getPeerId() { return peerId; }
    public boolean isAlive() { return alive && socket != null && !socket.isClosed(); }

    // -------------------- Teardown --------------------

    public void close() {
        if (!alive) return;   // already closed
        alive = false;

        try { if (in  != null) in.close();  } catch (IOException ignored) {}
        try { if (out != null) out.close(); } catch (IOException ignored) {}
        try { if (socket != null) socket.close(); } catch (IOException ignored) {}

        // interrupt the reader thread if it is still blocked on readObject()
        if (readerThread != null && readerThread.isAlive()) {
            readerThread.interrupt();
        }
    }
}