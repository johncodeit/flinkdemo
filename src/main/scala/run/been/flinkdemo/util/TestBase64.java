package run.been.flinkdemo.util;

import java.util.Base64;

public class TestBase64 {
    public static void main(String[] args) {
        String src = "eyduYW1lJzonamlhbmduaW5nJ30=";
        String str = getFromBase64(src);
        System.out.println(str);

    }
    public static String getFromBase64(String encodeString) {

        byte[] decode = Base64.getDecoder().decode(encodeString);

        String decodeString=new String(decode);

        return decodeString;

    }
}
