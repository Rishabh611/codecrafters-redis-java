import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
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
			StringBuilder requestBuilder = new StringBuilder();
			while(true) {
				int byteRead = in.read(buffer);
				if (byteRead == -1) break;

				requestBuilder.append(new String(buffer, 0, byteRead));
				String inputLine = requestBuilder.toString();
//				System.out.println(inputLine);
				char typeChar = inputLine.charAt(0);
				if (typeChar == '+') {
					String str = RedisProtocolParser.parseSimpleString(inputLine.substring(1));
				} else if (typeChar == '-') {
					RedisError error = RedisProtocolParser.parseError(inputLine.substring(1));
				} else if (typeChar == ':') {
					int num = RedisProtocolParser.parseInteger(inputLine.substring(1));
				} else if (typeChar == '$') {
					String bulkString = RedisProtocolParser.parseBulkString(inputLine.substring(1));
				} else if (typeChar == '*') {
					ArrayList<String> arr = (ArrayList<String>) RedisProtocolParser.parseArray(inputLine.substring(1));
//					System.out.println(arr);
					if (arr.get(0).equalsIgnoreCase("echo")){
						out.write("+" + arr.get(1) + "\r\n");
						out.flush();
					} else if(arr.get(0).equalsIgnoreCase("ping")){
						out.write("+PONG\r\n");
						out.flush();
					}
				}
//				if(inputLine.equals("PING")) {
//					out.write("+PONG\r\n");
//					out.flush();
//				}
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
