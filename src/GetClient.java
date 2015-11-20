import javafx.util.Pair;

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class GetClient {

    private static Socket socket = null;
    private static final int listenPort = 63000;

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, NoSuchAlgorithmException {
        if (args.length != 3) {
            System.out.println("Please input the hostname and port from where you want to get the message + the identifier of the message");

            return;
        }

        // get the stuff from the command line
        InetAddress address = InetAddress.getByName(args[0]);
        int port = Integer.parseInt(args[1]);
        String resourceID = args[2];

        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        Key key = new Key(digest.digest(resourceID.getBytes()));
        Pair<Key, String> pair = new Pair<>(key, ""+listenPort);


        try {
            // create a Socket to send
            socket = new Socket(address, port);

            // sendGet
            Message forward = new Message(Type.GET, pair);

            try{
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(forward);
                socket.close();
                // receive the value (if any exists)
                ServerSocket server = new ServerSocket(listenPort);
                server.setSoTimeout(5000);
                socket = server.accept();

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                Message reply = (Message) in.readObject(); //blocks until answer is given

                System.out.println("Message received");

                String resource = (String)reply.getResource();

                if(resource == null){
                    System.out.println("NO RESOURCE");
                }else{
                    System.out.println("Message is " + resource);
                }

            } catch (ConnectException e){

            } finally{
                socket.close();
            }
        }
        catch(Exception e){

        }

    }
}