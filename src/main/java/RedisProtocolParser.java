import java.util.ArrayList;
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
       try{
           if (data.endsWith(CLRF)) {
               int stringLength = data.charAt(0);
               data = data.substring(1, data.length() - CLRF.length());
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
        try{
            int arrayLength = data.charAt(0);
            List<String> arr = new ArrayList<>();

            data = data.substring(1);

            int i = 0;

            while(i < data.length()) {
                int j = i;

                while (! data.startsWith(CLRF, j)){
                    j++;
                }
                arr.add(data.substring(i, j));
                i = j + CLRF.length();
            }

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
