import javax.xml.bind.DatatypeConverter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by cem2ran on 19/11/2015.
 */
public class KeyTest {

    public static final String KEY = "cem2ran@gmail.com";

    @org.junit.Test
    public void testXor() throws Exception {
        Key ki1 = new Key(SHA1(KEY));

        Key ki2 = new Key(SHA1(KEY));

        //I believe this is the bucket index. Use this if we're working with buckets
        System.out.println(ki1.getDistance(ki2));

        //Otherwise use this for distance
        System.out.println(ki1.xor(ki2).getInt());
    }

    public static String toHexString(byte[] array) {
        return DatatypeConverter.printHexBinary(array);
    }

    public static byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }

    public static byte[] SHA1 (String message){
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-1");
            digest.update(message.getBytes("utf8"));
            return digest.digest();
        } catch (NoSuchAlgorithmException|UnsupportedEncodingException e) {
            throw new RuntimeException("Invalid input!");
        }
    }
}