import java.io.*;
import java.net.Socket;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
public class RedisClient {
    private Socket clientSocket;
    BufferedReader in;
    BufferedWriter out;

    HashMap<String, String> storageMap;
    HashMap<String, Instant> expirationMap;

    public RedisClient(Socket socket, HashMap<String, String> storageMap, HashMap<String, Instant> expirationMap) throws IOException {
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

    public void handleArray(String input) throws IOException, RedisError {
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

    private String handleSET(List<String> array) throws RedisError {
        String key = array.get(1);
        String value = array.get(2);
        System.out.println("Adding key: " + key + " with value " + value);
        if(array.size() > 2 && array.get(3).equalsIgnoreCase("px")){
            int expirySeconds = Integer.parseInt(array.get(4));
            Instant expiryTime = Instant.now().plusMillis(expirySeconds);
            storageMap.put(key, value);
            System.out.println("The key will expire in " + expirySeconds * 0.001 + " in local time " + expiryTime);
            expirationMap.put(key, expiryTime);
        }
        else{
            storageMap.put(key, value);
        }
        return "+OK\r\n";
    }

    private String handleGET(String key){
        Instant expirationTime = expirationMap.get(key);
        if(expirationTime != null && expirationTime.isBefore(Instant.now())){
            storageMap.remove(key);
            expirationMap.remove(key);
            return "$-1\r\n";
        }
        String value = storageMap.get(key);
        if (value == null) {
            return "$-1\r\n";
        } else {
            return "$" + value.length() + "\r\n" + value + "\r\n";
        }
    }
}
