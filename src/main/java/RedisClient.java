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
         switch (command) {
            case "echo" -> handleEcho(arr.get(1));
            case "ping" -> handlePing();
            case "set" -> handleSET(arr);
            case "get" -> handleGET(arr.get(1));
            case "config" -> handleConfig(arr.get(1), arr.get(2));
            default -> throw new IllegalStateException("Unexpected value: " + command);
        };
    }

    private void handleEcho(String str) throws IOException {
        out.write("+" + str + "\r\n");
        out.flush();
    }

    private void handlePing() throws IOException {
        out.write("+PONG\r\n");
        out.flush();
    }

    private void handleSET(List<String> array) throws RedisError, IOException {
        String key = array.get(1);
        String value = array.get(2);
        System.out.println("Adding key: " + key + " with value " + value);
        if(array.size() > 3 ){
            if(array.get(3).equalsIgnoreCase("px")){
                int expirySeconds = Integer.parseInt(array.get(4));
                Instant expiryTime = Instant.now().plusMillis(expirySeconds);
                storageMap.put(key, value);
                System.out.println("The key will expire in " + expirySeconds * 0.001 + " in local time " + expiryTime);
                expirationMap.put(key, expiryTime);
            }
        }
        else{
            storageMap.put(key, value);
        }
        out.write("+OK\r\n");
        out.flush();
    }

    private void handleGET(String key) throws IOException {
        String res = null;
        Instant expirationTime = expirationMap.get(key);
        if(expirationTime != null && expirationTime.isBefore(Instant.now())){
            storageMap.remove(key);
            expirationMap.remove(key);
            res = "$-1\r\n";
            out.write(res);
            out.flush();
        }else {
            String value = storageMap.get(key);
            if (value == null) {
                res = "$-1\r\n";
            } else {
                res = "$" + value.length() + "\r\n" + value + "\r\n";
            }
            out.write(res);
            out.flush();
        }

    }

    private void handleConfig(String subcommand, String parameter) throws RedisError, IOException {
        System.out.println("Subcommand " + subcommand + " and parameter: " + parameter);
        if (subcommand.equalsIgnoreCase("get")) {
            String res = switch(parameter.toLowerCase()) {
                case "dir" -> Main.dir;
                case "dbfilename" -> Main.fileName;
                default -> throw new RedisError();
            };
            out.write("*2\r\n$" + parameter.length() + "\r\n" + parameter + "\r\n$" + res.length() + "\r\n" + res + "\r\n");
            out.flush();
        } else {
            throw new RedisError();
        }
    }
}
