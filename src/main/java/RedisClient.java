import java.io.*;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
public class RedisClient {
    private Socket clientSocket;
    BufferedReader in;
    BufferedWriter out;

    HashMap<String, String> storageMap;
    HashMap<String, LocalDateTime> expirationMap;

    public RedisClient(Socket socket, HashMap<String, String> storageMap, HashMap<String, LocalDateTime> expirationMap) throws IOException {
        this.clientSocket = socket;
        this.in = new BufferedReader(
                new InputStreamReader(socket.getInputStream())
        );
        this.out = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream())
        );
        this.storageMap = storageMap;
        this.expirationMap = expirationMap;
    }

    public void handleSimpleString(String input) throws IOException {
        String str = RedisProtocolParser.parseSimpleString(input.substring(1));
        out.write(str);
        out.flush();
    }

    public void handleError(String input) throws RedisError {
        throw new RedisError();
    }

    public void handleInteger(String input) throws IOException {
        int num = RedisProtocolParser.parseInteger(input.substring(1));
        out.write(num);
        out.flush();
    }

    public void handleBulkString(String input) throws IOException{
        String str = RedisProtocolParser.parseBulkString(input.substring(1));
        out.write(str);
        out.flush();
    }

    public void handleArray(String input) throws IOException {
        List<String> arr = RedisProtocolParser.parseArray(input);
        System.out.println("Array generated " + arr);
        String command = arr.getFirst().toLowerCase();
        String output = switch (command) {
            case "echo" -> "+" + arr.get(1) + "\r\n";
            case "ping" -> "+PONG\r\n";
            case "set" -> handleSET(arr);
            case "get" -> handleGET(arr.get(1));
            default -> throw new IllegalStateException("Unexpected value: " + command);
        };
        out.write(output);
        out.flush();
    }

    private String handleSET(List<String> array){
        String key = array.get(1);
        String value = array.get(2);
        System.out.println("Adding key: " + key + " with value " + value);
        if(array.get(3).equalsIgnoreCase("px")){
            int expirySeconds = Integer.parseInt(array.get(4));
            expirationMap.put(key, LocalDateTime.now().plusSeconds((long) (expirySeconds * 0.001)));
            System.out.println("The key will expire in " + expirySeconds * 0.001 + " in local time " + LocalDateTime.now().plusSeconds((long) (expirySeconds * 0.001)));
        }
        storageMap.put(key, value);
        return "+OK\r\n";
    }

    private String handleGET(String key){
        if(storageMap.containsKey(key) && LocalDateTime.now().isBefore(expirationMap.get(key))){
           return "+" + storageMap.get(key) + "\r\n";
        } else {
           return "$-1\r\n";
        }
    }
}
