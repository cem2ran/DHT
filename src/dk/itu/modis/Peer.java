import javafx.util.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Peer {
//HELLO
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
    //TODO add input from args
        String myhost = "localhost";

        if(args.length == 1){
            //boot peer
            Peer me = new Peer(UUID.randomUUID(), "localhost", Integer.parseInt(args[0]), new ConcurrentHashMap<>() );
            me.routingTable.put(me.id, me.host+":"+me.port);
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

                            if(o instanceof String){
                                String[] msg = o.toString().split(":");
                                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                                switch (msg[0]){
                                    case JOIN_MSG:

                                        //replyWithNodesUUIDAndRoutingTable
                                        out.writeObject(routingTable); //return a copy of routing table
                                        out.close();
                                        break;

                                    case LEAVE_MSG:
                                        removeHostFromRoutingTable(msg[1]);
                                        break;

                                    case CLOSEST_MSG:
                                        UUID target = UUID.fromString(msg[1]);
                                        //replyWithNodesUUIDAndRoutingTable
                                        UUID[] sorted = getSortedRoutingTableByDistanceToTarget(target);
                                        UUID closest = sorted[0];
                                        //return a copy of routing table entry
                                        out.writeObject(new Pair<>(closest, routingTable.get(closest)));
                                        out.close();

                                        //find out if we should update our own table
                                        final UUID meAsTarget = id;
                                        UUID[] sortedByMe = getSortedRoutingTableByDistanceToTarget(meAsTarget);

                                        //add those peers that we haven't met before
                                        for (int i = 0; i < 4; i++) {
                                            if(!routingTable.containsKey(sortedByMe[i])){
                                                routingTable.put(sortedByMe[i], connectedHost+":"+connectedPort);
                                            }
                                        }

                                        //remove those peers that are further than 4 peers away
                                        for (int i = 4; i < sortedByMe.length; i++) {
                                            routingTable.remove(sortedByMe[i]);
                                        }
                                        break;
                                }
                            }else{
                                //Other object types
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
            Socket client = new Socket(myHost, myPort);

            ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
            //TODO should maybe be done by boot peer
            UUID uuid = UUID.randomUUID();
            out.write((JOIN_MSG+":"+uuid.toString()).getBytes("UTF-8"));

            ObjectInputStream in = new ObjectInputStream(client.getInputStream());

            Map<UUID, String> bootNodeRoutingTable = (ConcurrentHashMap<UUID, String>) in.readObject(); //blocks

            assert bootNodeRoutingTable.size() > 0;

            if(bootNodeRoutingTable.size() == 1){
                //No other nodes so we do not findOtherNodes
                return new Peer(uuid, myHost, myPort, bootNodeRoutingTable);
            }
            //TODO create new routing table.
            Map routingTable = new ConcurrentHashMap<>();

            //findClosests neighbours
            List<Pair<UUID, String>> closest = findClosest(uuid, bootNodeRoutingTable);

            //take the last 4 peers and put them as our neighbours in routing table
            try {
                for (int i = closest.size(); i > closest.size()-4; i--) {
                    routingTable.put(closest.get(i).getKey(), closest.get(i).getValue());
                }
            } catch (Exception e) {
                System.out.println("did not find 4 neighbours this time...");
            }

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
        return one.hashCode() ^ two.hashCode();
    }

    public static Pair<UUID, String> requestClosests(UUID target, String hostip){
        String[] host_ip = hostip.split(":");
        try {
            Socket s = new Socket(host_ip[0], Integer.parseInt(host_ip[1]));
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());

            //send message to peer asking for his closest peer to target
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

    /*
     * find the closest peer in routing table
     */
    public static List<Pair<UUID, String>> findClosest(UUID target, Map<UUID, String> routingTable){

        //sort list
        UUID[] sorted = routingTable.keySet().toArray(new UUID[routingTable.size()]);
        Arrays.sort(sorted, (o1, o2) -> distance(target, o1)  - distance(target, o2));

        UUID closest = sorted[0], prevClosest = null;

        Pair<UUID, String> node = null;

        List<Pair<UUID, String>> lastContacted = new ArrayList<>();

        while( closest != target && closest != prevClosest ){
            prevClosest = closest;
            node = requestClosests(target, routingTable.get(closest));
            lastContacted.add(node);
            if(node == null) return null;
            else closest = node.getKey();
        }

        return lastContacted;

        //if(closest == target)
        //    return node;
        //else {
        //
        //};


    }
}
