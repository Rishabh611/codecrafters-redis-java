import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
	    // You can use print statements as follows for debugging, they'll be visible when running tests.
	    System.out.println("Logs from your program will appear here!");

		ServerSocket serverSocket = null;
		int port = 6379;

		try {
			serverSocket = new ServerSocket(port);
			// Since the tester restarts your program quite often, setting SO_REUSEADDR
			// ensures that we don't run into 'Address already in use' errors
			serverSocket.setReuseAddress(true);
			// Wait for connection from client.
			while(true) {
				Socket clientSocket = serverSocket.accept();
				Thread clientThread = new Thread(() -> handleClient(clientSocket));
				clientThread.start();
			}
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}


  	}
	private static void handleClient(Socket clientSocket) {
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(clientSocket.getInputStream())
			);

			BufferedWriter out = new BufferedWriter(
					new OutputStreamWriter(clientSocket.getOutputStream())
			);

			char[] buffer = new char[1024];

			HashMap<String, String> storageMap = new HashMap<>();
			HashMap<String, LocalDateTime> expirationMap = new HashMap<>();
			RedisClient redisClient = new RedisClient(clientSocket, storageMap, expirationMap);
			while(true) {
				StringBuilder requestBuilder = new StringBuilder();
				int byteRead = in.read(buffer);
				if (byteRead == -1) break;

				requestBuilder.append(new String(buffer, 0, byteRead));
				String inputLine = requestBuilder.toString();
				System.out.println("input: " + inputLine.replace("\r", "\\r").replace("\n", "\\n"));
				char typeChar = inputLine.charAt(0);
				inputLine = inputLine.substring(1);
				System.out.println("Type Character: " + typeChar);
				try {
					switch (typeChar) {
						case '+':
							redisClient.handleSimpleString(inputLine);
							break;
						case '-':
							redisClient.handleError(inputLine);
							break;
						case ':':
							redisClient.handleInteger(inputLine);
							break;
						case '$':
							redisClient.handleBulkString(inputLine);
							break;
						case '*':
							redisClient.handleArray(inputLine);
							break;
						default:
							System.out.println("Unknown command type " + typeChar);
							break;
					}
				} catch (IOException | RedisError e) {
					throw new RuntimeException(e);
				}
			}

		} catch (IOException e) {
			System.out.println("IOException " + e.getMessage());
		} finally {
			try {
				clientSocket.close();
			} catch (IOException e) {
				System.out.println("Error closing client socket: " + e.getMessage());
			}
		}

	}
}
