import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
public class RedisClient {
    private Socket clientSocket;
    BufferedReader in;
    BufferedWriter out;

    HashMap<String, String> map;
    public RedisClient(Socket socket, HashMap<String, String> map) throws IOException {
        this.clientSocket = socket;
        this.in = new BufferedReader(
                new InputStreamReader(socket.getInputStream())
        );
        this.out = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream())
        );
        this.map = map;
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
            case "set" -> handleSET(arr.get(1), arr.get(2));
            case "get" -> handleGET(arr.get(1));
            default -> throw new IllegalStateException("Unexpected value: " + command);
        };
        out.write(output);
        out.flush();
    }

    private String handleSET(String key, String value){
        map.put(key, value);
        return "+OK\r\n";
    }

    private String handleGET(String key){
        if(map.containsKey(key)){
           return "+" + map.get(key) + "\r\n";
        } else {
           return "$-1\r\n";
        }
    }
}
