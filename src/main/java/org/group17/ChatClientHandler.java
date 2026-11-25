package org.group17;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ChatClientHandler implements Runnable {

    private final int clientId;
    private final Socket socket;
    private final ChatServer server;
    private final ObjectOutputStream out;
    private final ObjectInputStream in;
    private volatile boolean running = true;

    public ChatClientHandler(int clientId, Socket socket, ChatServer server, ObjectOutputStream out, ObjectInputStream in) {
        this.clientId = clientId;
        this.socket = socket;
        this.server = server;
        this.out = out;
        this.in = in;
    }

    @Override
    public void run() {
        try {
            while (running) {
                Object obj = in.readObject();
                server.handleClientObject(obj);
            }
        } catch (Exception e) {
            stop();
        }
    }

    public void sendObject(Object obj) {
        try {
            out.writeObject(obj);
            out.flush();
        } catch (IOException e) {
            System.out.println("Failed to send object to client " + clientId);
        }
    }

    public void stop() {
        if (!running) return;
        running = false;

        try {
            if (!socket.isClosed()) socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        server.handleClientDisconnect(clientId);
        System.out.println("ChatClientHandler: Client " + clientId + " stopped.");
    }

    public int getClientId() {
        return clientId;
    }

    public Socket getSocket() {
        return socket;
    }
}