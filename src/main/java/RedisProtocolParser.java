import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class RedisProtocolParser {
    static final String CLRF = "\r\n";


    public static String parseSimpleString(String data){
        try {
            if (data.endsWith(CLRF)) {
                return data.substring(0, data.length() - CLRF.length());
            } else {
                throw new Exception("Invalid request");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static RedisError parseError(String data) {
        try {
            if (data.endsWith(CLRF)) {
                throw new RedisError();
            } else {
                throw new Exception("Invalid request");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

     public static int parseInteger(String data) {
        try {
            if (data.endsWith(CLRF)) {
                return Integer.parseInt(data.substring(0, data.length() - CLRF.length()));
            } else {
                throw new RedisError();
            }
        } catch (RedisError e) {
            throw new RuntimeException(e);
        }
    }

     public static String parseBulkString(String data) {
         System.out.println("Data to be parsed to String " + data.replace("\r", "\\r").replace("\n", "\\n"));
       try{
           if (data.endsWith(CLRF)) {
               int i = 0;
               while(Character.isDigit(data.charAt(i))){
                   i++;
               }
               int stringLength = Integer.parseInt(data.substring(0,i));
               data = data.substring(i, data.length() - CLRF.length());
               String result = data.substring(CLRF.length());
               if (result.length() == stringLength){
                   return result;
               } else {
                   throw new RedisError();
               }

           } else {
               throw new RedisError();
           }
       } catch (RedisError e) {
           throw new RuntimeException(e);
       }
    }

     public static List<String> parseArray(String data) {
        System.out.println("Data: " + data.replace("\r", "\\r").replace("\n", "\\n") + "to be parsed as an array");
        HashSet<Character> typeChars = new HashSet<>(Arrays.asList('+', '-', '$', ':', '*'));
        try{
            int i = 0;
            while(Character.isDigit(data.charAt(i))){
                i++;
            }
            int arrayLength = Integer.parseInt(data.substring(0, i));
            System.out.println("Array of length " + arrayLength + " to be created");
            List<String> arr = new ArrayList<>();

            String remaining = data.substring(i+CLRF.length());
            i = 0;

            while(i < remaining.length() && arr.size() < arrayLength) {
                while(i < remaining.length() && Character.isWhitespace(remaining.charAt(i))){
                    i++;
                }
                if(i>=remaining.length()) break;

                char typeChar = remaining.charAt(i);
                if(!typeChars.contains(typeChar)){
                    throw new RedisError();
                }

                if (typeChar == '$') {
//                    System.out.println(remaining);
                    int j = i + 1;
                    while(Character.isDigit(remaining.charAt(j))){
                        j++;
                    }
                    int numLength = j - i - 1;

                    int stringEnd = 1 + numLength + 2 * CLRF.length() + Integer.parseInt(remaining.substring(i + 1, j));
                    String substring = remaining.substring(i + 1, i + stringEnd);
//                    System.out.println(substring);
                    if(substring.endsWith(CLRF)){
                        arr.add(RedisProtocolParser.parseBulkString(substring));
                    } else{
                        throw new RedisError();
                    }
                    i = i + stringEnd;

                }

            }

//            System.out.println(arr);
            if (arr.size() == arrayLength){
                return arr;
            } else {
                throw new RedisError();
            }
        } catch (RedisError e) {
            throw new RuntimeException(e);
        }
     }
}
