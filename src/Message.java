import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

enum Type { PUT, GET, STORE, RETRIEVE, REPLICATE, RESOURCE }

public class Message implements java.io.Serializable {
    Type type;
    Object resource;

    public Message(Type type, Object res) {
        this.type = type;
        this.resource = res;
    }

    public Message() {

    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Object getResource() {
        return resource;
    }

    public void setResource(Object resource) {
        this.resource = resource;
    }


    public static Message deserialize(byte[] bytes) throws IOException,
            ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return (Message) o.readObject();

    }

    public static byte[] serialize(Message message) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(message);
        return b.toByteArray();
    }

    public static void sendGet(int id, InetAddress address,int port) throws IOException
    {
        //create the message
        Integer intObj = id;
        Message message = new Message(Type.GET, intObj);

        //serialize the message
        byte[] buff = new byte[512];
        buff = serialize(message);

        //send the message
        DatagramSocket destinationSocket = new DatagramSocket();
        DatagramPacket packet = new DatagramPacket(buff,buff.length,address,port);
        destinationSocket.send(packet);


    }

}