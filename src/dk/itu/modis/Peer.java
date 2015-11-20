import javafx.util.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Peer {
    private final Map<Integer, String> values = new HashMap<>();
    public final Map<Key, String> routingTable;
    public final Key my_key;

    public final String host;
    public final int my_port;

    public final static String LEAVE_MSG = "LEAVE", JOIN_MSG = "JOIN", CLOSEST_MSG = "CLOSEST", COPY_ROUTING_TABLE = "COPY_ROUTING_TABLE", I_AM_CLOSE = "I_AM_CLOSE";;
    private final HashMap<Key, String> resources;

    protected Peer(Key my_key, String host, int my_port, Map<Key, String> routingTable){
        this.my_key = my_key;
        this.routingTable = routingTable;
        this.host = host;
        this.my_port = my_port;
        resources = new HashMap<>();
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
    //TODO add input from args
        String myhost = "localhost";
        MessageDigest digest = MessageDigest.getInstance("SHA-1");

        if(args.length == 1){
            //boot peer
            Peer me = new Peer(new Key(digest.digest(args[0].getBytes())), myhost, Integer.parseInt(args[0]), new ConcurrentHashMap<>() );
            System.out.println("booted...");

            me.receive();




        }else if(args.length > 1){

            int myPort = 0;
            String hostIP = null;
            int hostPort = 0;
            try {
                myPort = Integer.parseInt(args[0]);
                hostIP = args[1];
                hostPort = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.out.println("not enough args!");
            }

            try {
                Peer me = join(myhost, myPort, hostIP, hostPort);

                System.out.println("joined network");
                System.out.println("messaging my peers...");

                Iterator it = me.routingTable.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry pair = (Map.Entry)it.next();
                    String[] ip_port = pair.getValue().toString().split(":");
                    System.out.println("I am here... " +  pair.getValue().toString());
                    Socket socket = new Socket(ip_port[0], Integer.parseInt(ip_port[1]));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(I_AM_CLOSE +":"+me.my_key +":"+myPort);

                    socket.setSoTimeout(2000); //timeout 2 seconds
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    //wait for resources that I should have.
                    try {
                        while (true) {
                            Message msg = (Message) in.readObject();
                            Pair<Key, String> p3 = (Pair<Key, String>) msg.getResource();
                            me.resources.put(p3.getKey(), p3.getValue());
                            System.out.println("Stored: "+ p3.getKey() + " -> " + p3.getValue());
                        }
                    }catch (SocketTimeoutException e){
//                        System.out.println("socket timed out");
                    }

                }
                System.out.println("done.");
                System.out.println("ready to receive");
                me.receive();


            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    private void receive(){
        try {
            ServerSocket server = new ServerSocket(my_port);

            while(true){
                Socket socket = server.accept();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            Object object = in.readObject();

                            String connectedHost = socket.getRemoteSocketAddress().toString().split(":")[0].replace("/", "");

                            if(object instanceof String){
                                String[] msg = object.toString().split(":");
                                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                                switch (msg[0]){
                                    case JOIN_MSG:
                                        //reply with our RoutingTable
                                        Map tmpTable = new ConcurrentHashMap<Key, String>();
                                        tmpTable.putAll(routingTable);
                                        tmpTable.put(my_key, host+":"+ my_port); //add our self
                                        System.out.println("JOIN: "+ host+":"+ my_port);

                                        out.writeObject(tmpTable); //return a copy of routing table
                                        out.close();
                                        System.out.println("sent routing table from a join message");

                                        break;

//                                    case LEAVE_MSG:
//                                        removeHostFromRoutingTable(msg[1]);
//                                        System.out.println("removed " + msg[1] + " from routing table");
//                                        break;

                                    case CLOSEST_MSG:
                                        Key target = new Key(msg[1], null);

                                        //replyWithNodesUUIDAndRoutingTable
                                        Key[] sorted = getSortedRoutingTableByDistanceToTarget(target);
                                        Key closest = sorted[0];
                                        //return a copy of routing table entry
                                        if(closest.equals(my_key)){
                                            //we are closest
                                            out.writeObject(new Pair<>(closest, host+":"+my_port));
                                        }else{
                                            //other peer is closer
                                            out.writeObject(new Pair<>(closest, routingTable.get(closest)));
                                        }
                                        out.close();

                                        break;
                                    case COPY_ROUTING_TABLE:
                                        Map tmpTable1 = new ConcurrentHashMap<Key, String>();
                                        tmpTable1.putAll(routingTable);
                                        tmpTable1.put(my_key, host+":"+ my_port);
                                        out.writeObject(tmpTable1);
                                        out.close();
                                        System.out.println("sent routing table");
                                        break;
                                    case I_AM_CLOSE:
                                        //TODO add peer to routing table
                                        Key hist_key = new Key(msg[1], null);
                                        String his_port = msg[2];
                                        addToRoutingTable(hist_key, connectedHost+":"+his_port);
                                        List<Key> keysList = getResourcesBelongToHim(hist_key);

                                        for(Key key : keysList){
                                            Message message = new Message(Type.STORE, new Pair(key, resources.get(key)));

                                            out.writeObject(message);
//                                            send(connectedHost, Integer.parseInt(his_port), message);
                                            System.out.println("Sent resource to better peer: " + resources.get(key));
                                            resources.remove(key);
                                        }

                                        break;
                                }

                            }else if (object instanceof Message){
                                Message msg = (Message)object;
                                Pair<Key, String> closest = null;
                                Socket socket1 = null;
                                String[] ip_port;
                                switch (msg.getType()){
                                    case PUT:
                                        System.out.println("received ");
                                        Pair<Key, String> p1 = (Pair<Key, String>)msg.getResource();
                                        Key targetKey = p1.getKey();
                                        closest = findClosest(targetKey, routingTable);
                                        //found our self
                                        if(closest == null){
                                            socket1 = new Socket("localhost", my_port);
                                        }else{
                                            ip_port = closest.getValue().split(":");
                                            socket1 = new Socket(ip_port[0], Integer.parseInt(ip_port[1]));
                                        }
                                        ObjectOutputStream out = new ObjectOutputStream(socket1.getOutputStream());
                                        msg.setType(Type.STORE);
                                        out.writeObject(msg);
                                        socket1.close();
                                        break;

                                    case GET:
                                        Pair<Key, String> p2 = (Pair<Key, String>) msg.getResource();
                                        Pair<Key, String> newPair = new Pair<>(p2.getKey(),
                                                connectedHost+":"+((Pair<Key, String>) msg.getResource()).getValue());

                                        msg.setResource(newPair);
                                        Key target = p2.getKey();

                                        for (int i = 0; i < routingTable.size(); i++) {

                                            try {
                                                closest = findClosest(target, routingTable);
                                                ip_port = closest.getValue().split(":");
                                                socket1 = new Socket(ip_port[0], Integer.parseInt(ip_port[1]));
                                                break;
                                            }catch(Exception e){
                                                routingTable.remove(closest.getKey());
                                            }
                                        }
                                        if(socket1 != null) {

                                            ObjectOutputStream out1 = new ObjectOutputStream(socket1.getOutputStream());
                                            msg.setType(Type.RETRIEVE);
                                            out1.writeObject(msg);
                                            socket1.close();
                                        }
                                        break;

                                    case STORE:
                                        Pair<Key, String> p3 = (Pair<Key, String>) msg.getResource();
                                        resources.put(p3.getKey(), p3.getValue());
                                        System.out.println("Stored: "+ p3.getKey() + " -> " + p3.getValue());

                                        //TODO send to our nearest neighbour

                                        for (int i = 0; i < routingTable.size(); i++) { //indexes never used!

                                            int size = routingTable.size();
                                            Key[] keys = getSortedRoutingTableByDistanceToTarget(my_key);
                                            System.out.println(keys[1]);
                                            assert routingTable.size() == size;
                                            ip_port = routingTable.get(keys[1]).split(":");
                                            Socket socket2;

                                            try {
                                                socket2 = new Socket(ip_port[0], Integer.parseInt(ip_port[1]));
                                                ObjectOutputStream out3 = new ObjectOutputStream(socket2.getOutputStream());
                                                msg.setType(Type.REPLICATE);
                                                out3.writeObject(msg);

                                            } catch (Exception e) {
                                                routingTable.remove(keys[1]);

                                            }
                                            break;
                                        }

                                        break;

                                    case RETRIEVE:
                                        System.out.println("should send resource...");
                                        Pair<Key, String> p4 =  (Pair<Key, String>) msg.getResource();
                                        System.out.println("Key is: " + p4.getKey() + " value is: "+ p4.getValue());
                                        String value = resources.get(p4.getKey());
                                        ip_port = p4.getValue().split(":");
                                        socket1 = new Socket(ip_port[0], Integer.parseInt(ip_port[1]));
                                        Message reply = new Message(Type.RESOURCE, value);
                                        ObjectOutputStream out2 = new ObjectOutputStream(socket1.getOutputStream());
                                        out2.writeObject(reply);
                                        socket1.close();
                                        System.out.println("sent resource");
                                        break;


                                    case REPLICATE:
                                        Pair<Key, String> p5 = (Pair<Key, String>) msg.getResource();
                                        resources.put(p5.getKey(), p5.getValue());
                                        System.out.println("Stored: "+ p5.getKey() + " -> " + p5.getValue());
                                        break;

                                }
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }finally {
                        }

                        Key[] table = getSortedRoutingTableByDistanceToTarget(my_key);

                        for (Key id : table){
                            String s = routingTable.get(id);
                            if(s == null){

                                System.out.println("I contain: self");
                            }else{

                                System.out.println("I contain: " + s);
                            }
                        }
                        System.out.println("----------------------------------------------------");
                    }
                }).start();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<Key> getResourcesBelongToHim(Key his_key) {
        List<Key> keyList = new ArrayList<>();

        Iterator it = resources.entrySet().iterator();

        while(it.hasNext()){
            Map.Entry entry = (Map.Entry) it.next();
            Key resourec_key = (Key) entry.getKey();
            int distanceToMe = my_key.xor(resourec_key).getInt().intValue();
            int distanceToHim = his_key.xor(resourec_key).getInt().intValue();

            //is he closer than me?
            if(distanceToMe > distanceToHim){
                keyList.add((Key) entry.getKey());
            }
        }

        return keyList;
    }

    private void addToRoutingTable(Key key, String ip_port){
        System.out.println("ADD TBL: " + ip_port);
        routingTable.put(key, ip_port);

        //limit routing table
        int limit = 3;
        Key[] keys = getSortedRoutingTableByDistanceToTarget(my_key);

//        if(keys.length > limit+1){
//            System.out.println("managing size of routing table");
//            try {
//                for(int i = limit+1; i < keys.length; i++){
//                    routingTable.remove(keys[i]);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

    }
    private Key[] getSortedRoutingTableByDistanceToTarget(Key target) {
        //Include self when checking
        Map<Key, String> tmpTable = new HashMap<>();
        tmpTable.putAll(routingTable);
        tmpTable.put(my_key, "localhost:"+ my_port);
//        System.out.println("my key" + my_key.toString());
        //sort list
        Key[] sorted = tmpTable.keySet().toArray(new Key[tmpTable.size()]);
        Arrays.sort(sorted, (o1, o2) -> target.xor(o1).getInt().subtract(target.xor(o2).getInt()).intValue());
        System.out.println(Arrays.toString(sorted));
        return sorted;
    }


    private void removeHostFromRoutingTable(String id) {
        routingTable.remove(new Key(id.getBytes()));
    }

    public static Peer join(String myHost, int myPort, String host, int host_port) throws Exception{
        try {
            Socket boot_socket = new Socket(host, host_port);

            ObjectOutputStream out_boot = new ObjectOutputStream(boot_socket.getOutputStream());

            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            Key key = new Key(digest.digest((""+myPort).getBytes()));
            out_boot.writeObject((JOIN_MSG+":"+key.toString()+":"+myPort));
            System.out.println("sent join msg");

            ObjectInputStream in_boot = new ObjectInputStream(boot_socket.getInputStream());

            Map<Key, String> boot_routingTable = (ConcurrentHashMap<Key, String>) in_boot.readObject(); //blocks

            assert boot_routingTable.size() > 0;

            if(boot_routingTable.size() == 1){
                //only boot node -> we do not find closest peers
                return new Peer(key, myHost, myPort, boot_routingTable);
            }
            boot_socket.close();
            //TODO create new routing table.

            //findClosests neighbours
            Pair<Key, String> closest = findClosest(key, boot_routingTable);

            //get his credentials
            String[] ip_port = closest.getValue().split(":");

            //get his routing table
            Socket socket_to_closest_peer = new Socket(ip_port[0].replace("/", ""), Integer.parseInt(ip_port[1]));
            ObjectOutputStream out_closest = new ObjectOutputStream(socket_to_closest_peer.getOutputStream());
            out_closest.writeObject(COPY_ROUTING_TABLE+":"+key.toString());

            //wait for table
            ObjectInputStream in_closest = new ObjectInputStream(socket_to_closest_peer.getInputStream());
            Map routingTable = (ConcurrentHashMap<Key, String>) in_closest.readObject(); //blocks until it gets the table (potential failure!)

            socket_to_closest_peer.close();
            System.out.println("done joining");
            return new Peer(key, myHost, myPort, routingTable);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new Exception("Could not join network");
    }


    public void leave() {
        routingTable.forEach((uuid, s) -> {
            String[] host_ip = s.trim().split(":");
            send(host_ip[0], Integer.parseInt(host_ip[1]), LEAVE_MSG);
        });
    }



    private void send(String host, int port, Object message){
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            ObjectOutputStream out_stream = new ObjectOutputStream(socket.getOutputStream());
            out_stream.writeObject(message);
            out_stream.close();
        } catch (IOException e) {
            e.printStackTrace();
            //TODO crash/fail update table
        } finally {
            if(socket != null) try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

//    public static int distance(Key one, Key two){
//        return Math.abs(one.hashCode() - two.hashCode());
//    }

    public static Pair<Key, String> requestClosests(Key target, String hostip) throws IOException, ClassNotFoundException {
        System.out.println(hostip);
        String[] host_ip = hostip.split(":");
            Socket s = new Socket(host_ip[0], Integer.parseInt(host_ip[1]));
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());

            //send message to peer asking for his closest peer to target
            out.writeObject(CLOSEST_MSG+":"+target);

            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            Pair<Key, String> node = (Pair<Key, String>) in.readObject();
            out.close();
            in.close();
            s.close();
            return node;

    }

    /*
     * find the closest peer in routing table
     */
    public static Pair<Key, String> findClosest(Key target, Map<Key, String> routingTable) throws IOException, ClassNotFoundException {

        //sort list
        Key[] sorted = routingTable.keySet().toArray(new Key[routingTable.size()]);
        Arrays.sort(sorted, (o1, o2) -> target.xor(o1).getInt().subtract(target.xor(o2).getInt()).intValue());
        if(sorted.length == 0){
            return null;
        }
        Key closest = sorted[0], prevClosest = null;

        Pair<Key, String> node = new Pair<>(closest, routingTable.get(closest));

        while( !(closest.equals(target)) && !(closest.equals(prevClosest))){
            prevClosest = closest;
            String p = routingTable.get(closest);
            if(p != null) {
                node = requestClosests(target, p);
            }
            if(node == null) return null;
            else closest = node.getKey();
        }

        return node;
    }


}
