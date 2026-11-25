package org.group17;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RegistryService {

//    public static final String HOST = "172.20.10.2";
    public static final String HOST = "localhost";
    public static final int PORT = 8000;

    private final Map<Integer, ServerInfo> activeServers = new ConcurrentHashMap<>();
    private HttpServer server;

    private final AtomicInteger nextServerId = new AtomicInteger(1);
    private final AtomicInteger nextClientId = new AtomicInteger(1);
    private final AtomicInteger nextRoomId = new AtomicInteger(100001);


    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(HOST, PORT), 0);

        server.createContext("/servers", this::handleServers);
        server.createContext("/nextServerId", this::nextServerId);
        server.createContext("/nextClientId", this::nextClientId);
        server.createContext("/nextRoomId", this::nextRoomId);
        server.setExecutor(null); // default executor
        server.start();

        System.out.println("Registry HTTP server started on port " + PORT);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            System.out.println("Registry HTTP server stopped");
        }
    }


    // ---------------- handlers ----------------

    private void handleServers(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        switch (method) {
            case "GET" -> handleGetServers(exchange);
            case "POST" -> handleRegister(exchange);
            case "DELETE" -> handleDeregister(exchange);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleGetServers(HttpExchange exchange) throws IOException {
        byte[] response = serialize(new ArrayList<>(activeServers.values()));
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void handleRegister(HttpExchange exchange) throws IOException {
        ServerInfo serverInfo = (ServerInfo) deserialize(exchange.getRequestBody());
        activeServers.put(serverInfo.id(), serverInfo);
        System.out.println("Registry: Server " + serverInfo.id() + " registered/refreshed");
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void handleDeregister(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath(); // /servers/{id}
        String[] parts = path.split("/");
        if (parts.length == 3) {
            int serverId = Integer.parseInt(parts[2]);
            activeServers.remove(serverId);
            System.out.println("Registry: Server " + serverId + " deregistered");
            exchange.sendResponseHeaders(204, -1);
        } else {
            exchange.sendResponseHeaders(400, -1);
        }
        exchange.close();
    }


    // ids

    private void nextServerId(HttpExchange exchange) throws IOException {
        nextId(exchange, nextServerId);
    }

    private void nextClientId(HttpExchange exchange) throws IOException {
        nextId(exchange, nextClientId);
    }

    private void nextRoomId(HttpExchange exchange) throws IOException {
        nextId(exchange, nextRoomId);
    }

    private void nextId(HttpExchange exchange, AtomicInteger nextId) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        int id = nextId.getAndIncrement();
        byte[] response = serialize(id);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }


    // ---------------- utils ----------------

    private byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
        }
        return bos.toByteArray();
    }

    private Object deserialize(InputStream in) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(in))) {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
