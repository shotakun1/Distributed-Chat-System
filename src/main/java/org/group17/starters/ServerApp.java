package org.group17.starters;

import org.group17.ChatServer;

import java.util.Scanner;

public class ServerApp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("--- Chat Server Launcher ---");
        System.out.print("Enter port to bind (default 5000): ");
        String portStr = scanner.nextLine();
        int port = portStr.isEmpty() ? 5000 : Integer.parseInt(portStr);

        try {// need to change on diff hosts
//            ChatServer server = new ChatServer("172.20.10.2", port);
            ChatServer server = new ChatServer("localhost", port);
            server.start();

            System.out.println("Server " + server.getServerId() + " started on port " + port);
            System.out.println("Press enter to shutdown.");

            scanner.nextLine();
            server.stop();
            System.out.println("Server stopped.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}