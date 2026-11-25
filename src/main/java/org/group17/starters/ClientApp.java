package org.group17.starters;

import org.group17.ChatClient;
import org.group17.ServerInfo;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ClientApp {
    private static ChatClient client;
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        try {
            System.out.println("--- Initializing Client ---");
            // Connects to registry to get ID
            client = new ChatClient();

            boolean running = true;
            while (running) {
                printMenu();
                System.out.print("> ");
                String input = scanner.nextLine();

                switch (input) {
                    case "1" -> listServers();
                    case "2" -> connectToServer();
                    case "3" -> createRoom();
                    case "4" -> joinRoom();
                    case "5" -> sendGroupMessage();
                    case "6" -> sendDirectMessage();
                    case "7" -> leaveRoom();
                    case "8" -> showJoinedRooms(); // 新增 Case
                    case "9" -> {                  // 修改退出 Case 为 9
                        client.disconnect();
                        System.out.println("Exiting...");
                        running = false;
                        System.exit(0);
                    }
                    default -> System.out.println("Unknown command.");
                }
            }
        } catch (Exception e) {
            System.out.println("Fatal Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void showJoinedRooms() {
        List<Integer> rooms = client.getJoinedRooms();
        if (rooms.isEmpty()) {
            System.out.println("You have not joined any chat rooms.");
        } else {
            System.out.println("\n--- Joined Chat Rooms ---");
            for (int roomId : rooms) {
                java.util.Set<Integer> members = client.getRoomMembers(roomId);
                System.out.println("Room ID: " + roomId + " | Members: " + members);
            }
            System.out.println("-------------------------");
        }
    }

    private static void printMenu() {
        System.out.println("\n--- Client " + (client != null ? client.getClientId() : "?") + " Menu ---");
        System.out.println("1. List Available Servers");
        System.out.println("2. Connect to Server");
        System.out.println("3. Create Chat Room");
        System.out.println("4. Join Chat Room");
        System.out.println("5. Send Group Message");
        System.out.println("6. Send Direct Message");
        System.out.println("7. Leave Room");
        System.out.println("8. Show Joined Rooms & Members"); 
        System.out.println("9. Quit");
    }

    private static void listServers() {
        List<ServerInfo> servers = client.discoverServers();
        System.out.println("Found " + servers.size() + " servers:");
        for (ServerInfo s : servers) {
            System.out.println(" - ID: " + s.id() + " | " + s.host() + ":" + s.port());
        }
    }

    private static void connectToServer() {
        System.out.print("Enter Server ID to connect to: ");
        try {
            int sId = Integer.parseInt(scanner.nextLine());
            List<ServerInfo> servers = client.discoverServers();
            ServerInfo target = servers.stream().filter(s -> s.id() == sId).findFirst().orElse(null);

            if (target != null) {
                client.connect(target);
            } else {
                System.out.println("Server ID not found in registry.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid number.");
        }
    }

    private static void createRoom() throws ExecutionException, InterruptedException {
        System.out.print("Enter Room Name: ");
        String name = scanner.nextLine();
        Integer roomId = client.createRoom(name).get();
        System.out.println("RoomID "+ roomId +" created");
    }

    private static void joinRoom() {
        System.out.print("Enter Room ID: ");
        try {
            int rId = Integer.parseInt(scanner.nextLine());
            client.joinRoom(rId);
        } catch (NumberFormatException e) {
            System.out.println("Invalid ID.");
        }
    }

    private static void sendGroupMessage() {
        System.out.print("Enter Room ID: ");
        try {
            int rId = Integer.parseInt(scanner.nextLine());
            System.out.print("Enter Message: ");
            String msg = scanner.nextLine();
            client.sendMessageToRoom(rId, msg);
        } catch (NumberFormatException e) {
            System.out.println("Invalid ID.");
        }
    }

    private static void sendDirectMessage() {
        System.out.print("Enter Target Client ID: ");
        try {
            int cId = Integer.parseInt(scanner.nextLine());
            System.out.print("Enter Message: ");
            String msg = scanner.nextLine();
            client.sendDirectMessage(cId, msg);
        } catch (NumberFormatException e) {
            System.out.println("Invalid ID.");
        }
    }

    private static void leaveRoom() {
        System.out.print("Enter Room ID: ");
        try {
            int rId = Integer.parseInt(scanner.nextLine());
            client.leaveRoom(rId);
        } catch (NumberFormatException e) {
            System.out.println("Invalid ID.");
        }
    }
}