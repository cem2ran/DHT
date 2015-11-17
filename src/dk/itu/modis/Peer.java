package dk.itu.modis;

import javafx.util.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Peer {

    private final Map<Integer, String> values = new HashMap<>();
    public final Map<UUID, String> routingTable;

    public final UUID id;
    public final String host;
    public final int port;

    public final static String LEAVE_MSG = "LEAVE", JOIN_MSG = "JOIN", CLOSEST_MSG = "CLOSEST";

    protected Peer(UUID id, String host, int port, Map<UUID, String> routingTable){
        this.id = id;
        this.routingTable = routingTable;
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) {

        try {
            Peer me = join("localhost", 42000, "localhost", 1025);
            me.receive();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void receive(){
        try {
            ServerSocket server = new ServerSocket(port);
            while(true){
                Socket socket = server.accept();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            Object o = in.readObject();

                            if(o instanceof String){
                                String[] msg = o.toString().split(":");
                                switch (msg[0]){
                                    case LEAVE_MSG:
                                        removeHostFromRoutingTable(msg[1]);
                                        break;
                                    case JOIN_MSG:
                                        String host = socket.getRemoteSocketAddress().toString();
                                        int port = socket.getPort();
                                        // Generate at client instead
                                        //UUID nodeID = UUID.randomUUID(); //TODO GUID?

                                        //String UUID = msg[1];
                                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                        out.writeObject(routingTable);



                                        //replyWithNodesUUIDAndRoutingTable

                                        break;
                                    case CLOSEST_MSG:

                                        break;
                                }
                            }else{
                                //Other object types
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void removeHostFromRoutingTable(String id) {
        routingTable.remove(UUID.fromString(id));
    }

    public static Peer join(String myHost, int myPort, String host, int host_port) throws Exception{
        try {
            Socket client = new Socket(myHost, myPort);

            ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
            UUID id = UUID.randomUUID();
            out.write((JOIN_MSG+":"+id.toString()).getBytes("UTF-8"));

            ObjectInputStream in = new ObjectInputStream(client.getInputStream());

            Map<UUID, String> bootNodeRoutingTable = (Map<UUID, String>) in.readObject();

            assert bootNodeRoutingTable.size() > 0;
            if(bootNodeRoutingTable.size() == 1){
                //No other nodes so we do not findOtherNodes
                return new Peer(id, myHost, myPort, bootNodeRoutingTable);
            }
            //findClosests
            Pair<UUID, String> closest = findClosest(id, bootNodeRoutingTable);

            //create new routing table.


            return new Peer(id, myHost, myPort, bootNodeRoutingTable);
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

    public static int distance(UUID one, UUID two){
        return one.hashCode() ^ two.hashCode();
    }

    public static Pair<UUID, String> requestClosests(UUID target, String hostip){
        String[] host_ip = hostip.split(":");
        try {
            Socket s = new Socket(host_ip[0], Integer.parseInt(host_ip[1]));
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());

            out.write((CLOSEST_MSG+":"+target).getBytes("UTF-8"));

            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            Pair<UUID, String> node = (Pair<UUID, String>) in.readObject();
            s.close();

            return node;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static Pair<UUID, String> findClosest(UUID target, Map<UUID, String> routingTable){
        //find the closest peer in routing table
        UUID[] sorted = routingTable.keySet().toArray(new UUID[routingTable.size()]);
        Arrays.sort(sorted, (o1, o2) -> distance(target, o1)  - distance(target, o2));
        UUID closest = sorted[0], prevClosest = null;

        Pair<UUID, String> node = null;

        while( closest != target && closest != prevClosest ){
            prevClosest = closest;
            node = requestClosests(closest, routingTable.get(closest));
            if(node == null) return null;
            else closest = node.getKey();
        }

        if(closest == target)
            return node;
        else return null;


    }
}
