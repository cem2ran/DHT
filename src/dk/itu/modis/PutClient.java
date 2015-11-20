
import javafx.util.Pair;

import java.net.*;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PutClient {

    private static Socket socket = null;
    private static Pair<Key, String> res;

    /**
     *
     * @param args hostName, port, key, value
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            System.out
                    .println("Please input the hostname and port to which to connect + the identifier and the message you want to send");
            return;
        }

        // Getting all the stuff from the command line
        InetAddress address = InetAddress.getByName(args[0]);
        int port = Integer.parseInt(args[1]);
        String id = args[2];
        String value = args[3];

        try {
            // create a Socket to send it
            socket = new Socket(address, port);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            // create the resource and the message
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            Key key = new Key(digest.digest(id.getBytes()));
            res = new Pair(key, value);

            Message message = new Message(Type.PUT, res);

            out.writeObject(message);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } finally {
            socket.close();

        }

    }

}