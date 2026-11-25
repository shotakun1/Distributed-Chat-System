package org.group17;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class HttpRegistryClient {

    private static final String REGISTRY_URL = "http://" + RegistryService.HOST + ":" + RegistryService.PORT;
    private static final HttpClient client = HttpClient.newHttpClient();

    /** Register this server at the registry (serialized object) */
    public static void registerServer(ServerInfo server) throws IOException, InterruptedException {
        byte[] body;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(server);
            oos.flush();
            body = baos.toByteArray();
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(REGISTRY_URL + "/servers"))
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        client.send(request, HttpResponse.BodyHandlers.discarding());
    }

    /** Deregister this server from the registry */
    public static void deregisterServer(int serverId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(REGISTRY_URL + "/servers/" + serverId))
                .DELETE()
                .build();

        client.send(request, HttpResponse.BodyHandlers.discarding());
    }

    /** Get the list of currently active servers (serialized list) */
    public static List<ServerInfo> getActiveServers() throws IOException, InterruptedException, ClassNotFoundException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(REGISTRY_URL + "/servers"))
                .GET()
                .build();

        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        byte[] body = response.body();
        if (body == null || body.length == 0) return new ArrayList<>();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(body); ObjectInputStream ois = new ObjectInputStream(bais)) {
            @SuppressWarnings("unchecked")
            List<ServerInfo> servers = (List<ServerInfo>) ois.readObject();
            return servers;
        }
    }

    public static int fetchNextId(String endpoint) throws IOException, InterruptedException, ClassNotFoundException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(REGISTRY_URL + endpoint))
                .GET()
                .build();

        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (ObjectInputStream ois = new ObjectInputStream(response.body())) {
            return (int) ois.readObject();
        }
    }
}
