import javafx.util.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Peer {
//HELLO
    private final Map<Integer, String> values = new HashMap<>();
    public final Map<UUID, String> routingTable;

    public final UUID my_uuid;
    public final String host;
    public final int port;

    public final static String LEAVE_MSG = "LEAVE", JOIN_MSG = "JOIN", CLOSEST_MSG = "CLOSEST", COPY_ROUTING_TABLE = "COPY_ROUTING_TABLE";

    protected Peer(UUID my_uuid, String host, int port, Map<UUID, String> routingTable){
        this.my_uuid = my_uuid;
        this.routingTable = routingTable;
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) {
    //TODO add input from args
        String myhost = "localhost";

        if(args.length == 1){
            //boot peer
            Peer me = new Peer(UUID.randomUUID(), myhost, Integer.parseInt(args[0]), new ConcurrentHashMap<>() );
//            me.routingTable.put(me.my_uuid, myhost + ":" + me.port);
            System.out.println("booted...");
            me.receive();
        }else if(args.length > 1){
            //myo
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
//                me.routingTable.put(me.my_uuid, me.host+":"+me.port);
                System.out.println("ready to receive");
                me.receive();
            } catch (Exception e) {
                e.printStackTrace();
            }
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

                            String connectedHost = socket.getRemoteSocketAddress().toString();
                            int connectedPort = socket.getPort();
                            //TODO should get which port to contact peer on!!
                            if(o instanceof String){
                                String[] msg = o.toString().split(":");
                                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

//                                System.out.println("received: " + o.toString());

                                switch (msg[0]){
                                    case JOIN_MSG:
                                        //replyWithNodesUUIDAndRoutingTable
                                        Map tmpTable = new ConcurrentHashMap<UUID, String>();
                                        tmpTable.putAll(routingTable);
                                        tmpTable.put(my_uuid, host+":"+port);

                                        out.writeObject(tmpTable); //return a copy of routing table
                                        out.close();
                                        System.out.println("sent routing table");
                                        System.out.println(tmpTable.toString());

                                        break;

                                    case LEAVE_MSG:
                                        removeHostFromRoutingTable(msg[1]);
                                        System.out.println("removed " + msg[1] + " from routing table");
                                        break;

                                    case CLOSEST_MSG:
                                        UUID target = UUID.fromString(msg[1]);
                                        if(target == null){
                                            System.out.println("looking for null from " + connectedPort);
                                        }
                                        //replyWithNodesUUIDAndRoutingTable
                                        UUID[] sorted = getSortedRoutingTableByDistanceToTarget(target);
                                        UUID closest = sorted[0];
                                        //return a copy of routing table entry
                                        out.writeObject(new Pair<>(closest, routingTable.get(closest)));
                                        out.close();

                                        //find out if we should update our own table
                                        //final UUID meAsTarget = my_uuid;
                                        //UUID[] sortedByMe = getSortedRoutingTableByDistanceToTarget(meAsTarget);

                                        //add those peers that we haven't met before
//                                        try {
//                                            for (int i = 0; i < routingTable.size(); i++) {
//                                                if (!routingTable.containsKey(sortedByMe[i])) {
                                                    //routingTable.put(target, connectedHost + ":" + connectedPort);
//                                                }
//                                            }
//                                        } catch (Exception e) {
//                                            System.out.println("table size: " + routingTable.size());
//                                        }

                                        //remove those peers that are further than 4 peers away
//                                        for (int i = 4; i < sortedByMe.length; i++) {
//                                            routingTable.remove(sortedByMe[i]);
//                                        }
                                        break;
                                    case COPY_ROUTING_TABLE:
                                        Map tmpTable1 = new ConcurrentHashMap<UUID, String>();
                                        tmpTable1.putAll(routingTable);
                                        tmpTable1.put(my_uuid, host+":"+port);
                                        out.writeObject(tmpTable1);
                                        out.close();
                                        System.out.println("sent routing table");
                                        break;
                                }

                                //TODO update our routing table
                                if(msg.length == 2){
                                    UUID his_uuid = UUID.fromString(msg[1]);
                                    addAndUpdateRoutingTable(his_uuid, connectedHost+":"+connectedPort);
                                }
                            }else{
                                //Other object types
                                System.out.println("not a string");
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }finally {
                        }
                    }
                }).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addAndUpdateRoutingTable(UUID his_uuid, String ip_port) {
        routingTable.put(his_uuid, ip_port);
        UUID[] sorted = getSortedRoutingTableByDistanceToTarget(my_uuid);

        //remove furthest peers from table
        if(sorted.length > 4){
            for (int i = 4; i < sorted.length; i++) {
                routingTable.remove(sorted[i]);
            }
        }
    }

    private UUID[] getSortedRoutingTableByDistanceToTarget(UUID target) {
        //sort list
        UUID[] sorted = routingTable.keySet().toArray(new UUID[routingTable.size()]);
        Arrays.sort(sorted, (o1, o2) -> distance(target, o1)  - distance(target, o2));
        return sorted;
    }


    private void removeHostFromRoutingTable(String id) {
        routingTable.remove(UUID.fromString(id));
    }

    public static Peer join(String myHost, int myPort, String host, int host_port) throws Exception{
        try {
            Socket boot_socket = new Socket(host, host_port);

            ObjectOutputStream out_boot = new ObjectOutputStream(boot_socket.getOutputStream());
            //TODO should maybe be done by boot peer
            UUID uuid = UUID.randomUUID();
            out_boot.writeObject((JOIN_MSG+":"+uuid.toString()+":"+myPort));
            System.out.println("sent join msg");

            ObjectInputStream in_boot = new ObjectInputStream(boot_socket.getInputStream());

            Map<UUID, String> boot_routingTable = (ConcurrentHashMap<UUID, String>) in_boot.readObject(); //blocks

            assert boot_routingTable.size() > 0;

            if(boot_routingTable.size() == 1){
                //only boot node -> we do not find closest peers
                return new Peer(uuid, myHost, myPort, boot_routingTable);
            }

            //TODO create new routing table.

            //findClosests neighbours
            Pair<UUID, String> closest = findClosest(uuid, boot_routingTable);

            //get his credentials
            String[] ip_port = closest.getValue().split(":");

            //get his routing table
            Socket socket_to_closest_peer = new Socket(ip_port[0].replace("/", ""), Integer.parseInt(ip_port[1]));
            ObjectOutputStream out_closest = new ObjectOutputStream(socket_to_closest_peer.getOutputStream());
            out_closest.writeObject(COPY_ROUTING_TABLE+":"+uuid.toString());

            //wait for table
            ObjectInputStream in_closest = new ObjectInputStream(socket_to_closest_peer.getInputStream());
            Map routingTable = (ConcurrentHashMap<UUID, String>) in_closest.readObject(); //blocks until it gets the table (potential failure!)

            System.out.println("done joining");
            return new Peer(uuid, myHost, myPort, routingTable);
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
        return Math.abs(one.hashCode() - two.hashCode());
    }

    public static Pair<UUID, String> requestClosests(UUID target, String hostip){
        System.out.println(hostip);
        String[] host_ip = hostip.split(":");
        try {
            Socket s = new Socket(host_ip[0], Integer.parseInt(host_ip[1]));
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());

            //send message to peer asking for his closest peer to target
            out.writeObject(CLOSEST_MSG+":"+target);

            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            Pair<UUID, String> node = (Pair<UUID, String>) in.readObject();
            out.close();
            in.close();
            s.close();
            return node;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    /*
     * find the closest peer in routing table
     */
    public static Pair<UUID, String> findClosest(UUID target, Map<UUID, String> routingTable){

        //sort list
        UUID[] sorted = routingTable.keySet().toArray(new UUID[routingTable.size()]);
        Arrays.sort(sorted, (o1, o2) -> distance(target, o1)  - distance(target, o2));

        UUID closest = sorted[0], prevClosest = null;

        Pair<UUID, String> node = new Pair<>(closest, routingTable.get(closest));

        while( closest != target && closest != prevClosest ){
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
