package app_kvECS;

import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import ecs.IECSNode;
import ecs.ECSNode;

import java.io.*;
import java.util.concurrent.CountDownLatch;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.comm.CommModule;
import shared.messages.KVAdminMsg;

import static shared.messages.KVMessage.StatusType.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import shared.messages.KVMsg;


public class ECSClient implements IECSClient, Watcher {
    private Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private boolean stop = false;
    private boolean running;
    private List<ECSNode> idleServers = new ArrayList<ECSNode>();
    public List<ECSNode> activeServers = new ArrayList<ECSNode>();
    private List<ECSNode> activatingServers = new ArrayList<ECSNode>();
    private CountDownLatch isConnected = new CountDownLatch(1);
    final ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, this);
    private Socket clientSocket;
    private CommModule clientComm;
    private List<HashMap<String, String>> metadata = new ArrayList<HashMap<String,String>>();
    public ECSClient(String configfile) throws KeeperException, IOException{
        try {
            System.out.println("Config File Name:");
            System.out.println(configfile);
            File file = new File(configfile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            //StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                String[] args = line.split("\\s+");
                String serverName = args[0];
                String host = args[1];
                String port = args[2];
                ECSNode node = new ECSNode(serverName, host, Integer.parseInt(port), null);
                idleServers.add(node);
            }
            fr.close();
        } catch (IOException e) {
            setRunning(false);
            printError("ERROR: could not access ecs config file");
            this.stop=true;
        }

        try{
            if(zk.exists("/keeper",false)==null) {
                final String rootPath = zk.create("/keeper", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Node created at: " + rootPath);
            }
        }catch(KeeperException | InterruptedException e){
            e.printStackTrace();

        }
        //Look at zookeeper to intialize contructors
        try {
            List<String> attendance = zk.getChildren("/keeper", false);
            for (int i = 0; i< attendance.size();i++){
                System.out.println("Found: "+attendance.get(i));
                byte[] locData = zk.getData("/keeper/"+attendance.get(i),false, null);
                String addr_port = new String(locData);
                String host = addr_port.split(":")[0];
                int port = Integer.parseInt(addr_port.split(":")[1]);
                for(int j = 0; j< idleServers.size();j++){
                    if(attendance.get(i).equals(idleServers.get(j).getNodeName())) {
                        System.out.println("Adding to activated: "+attendance.get(i));
                        activeServers.add(idleServers.get(j));
                        idleServers.remove(j);
                        j--;
                        break;
                    }
                }

            }
            calc_metadata(activeServers);
        }catch (InterruptedException e){
            e.printStackTrace();
        }


    }
    public void process(WatchedEvent event) {
        System.out.println(event.getState());
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            isConnected.countDown();
        }
        try {
            zk.getChildren("/keeper", nodeWatch);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    Watcher nodeWatch=new Watcher(){
        @Override
        public void process(WatchedEvent watchedEvent) {
            System.out.println("change in nodes");
            System.out.println(watchedEvent.getState());
            try {
                zk.getChildren("/keeper", nodeWatch);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    };


    public void run() throws Exception {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            String cmdLine = stdin.readLine();
            this.handleCommand(cmdLine);

        }
    }
    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }
    public void setRunning(boolean run) {
        running = run;
    }
    public String handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;

            System.out.println(PROMPT + "Application exit!");
            return "Application exit";
        }else if(tokens[0].equals("start")){
            //final ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", new ZooKeeperWatcher());
            boolean success = start();
            if (success){
                System.out.println("All activated servers started");
                return "All activated servers started";
            }else{
                System.out.println("Not all activated servers started");
                return "Not all activated servers started";
            }


        }else if(tokens[0].equals("stop")){
            //final ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", new ZooKeeperWatcher());
            boolean success = stop();
            if (success){
                System.out.println("All activated servers stopped");
                return "All activated servers stopped";
            }else{
                System.out.println("Not all activated servers stopped");
                return "Not all activated servers stopped";
            }


        }else if(tokens[0].equals("shutDown")){
            //final ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", new ZooKeeperWatcher());
            boolean success = shutdown();
            if (success){
                System.out.println("All activated servers shut down");
                return "All activated servers shut down";
            }else{
                System.out.println("Not all activated servers shut down");
                return "Not all activated servers shut down";
            }


        }else if(tokens[0].equals("addNodes")){
            if (tokens.length==2){
                if (idleServers.size()==0){
                    printError("No servers available!");
                    return "No servers available!";
                }else if(idleServers.size()<Integer.parseInt(tokens[1])){
                    printError("Not enough servers available! "+String.valueOf(idleServers.size())+" servers available.");
                    return "Not enough servers available! "+String.valueOf(idleServers.size())+" servers available.";
                }
                //addNodes(Integer.parseInt(tokens[1]), "FIFO", 10);
                String addrs="";
                for (int i = 0;i<Integer.parseInt(tokens[1]);i++){
                    IECSNode newNode=addNode("FIFO",10);
                    String host =newNode.getNodeHost();
                    int port = newNode.getNodePort();
                    String name=newNode.getNodeName();
                    String addr=host+":"+String.valueOf(port)+":"+name;
                    addrs=addrs+" "+addr;
                }
                System.out.println("Added "+tokens[1]+" nodes");
                return addrs;
            }else {
                printError("Invalid number of parameters!");
                return "Invalid number of parameters!";
            }


        }else if(tokens[0].equals("addNode")){
            if (tokens.length==1){
                if (idleServers.size()==0){
                    printError("No servers available!");
                    return "No servers available!";
                }
                IECSNode inode=addNode("FIFO", 10);
                String host =inode.getNodeHost();
                int port = inode.getNodePort();
                String name=inode.getNodeName();
                System.out.println("Added node " + name+" at "+host+":"+port);

                return "Added node " + name+" at "+host+":"+port;
            }else {
                printError("Invalid number of parameters!");
                return "Invalid number of parameters!";
            }


        }else if(tokens[0].equals("removeNode")){
            if (tokens.length==2){
                if (activeServers.size()==0){
                    printError("No servers running!");
                    return "No servers running!";
                }else if (activeServers.size()==1){
                    printError("Only one server running, run 'shutDown' to close the last node");
                    return "Only one server running, run 'shutDown' to close the last node";
                }
                String toRemove = (tokens[1]);
                boolean success=removeNode(toRemove);
                if(!success){System.out.println("Specified server not active");};
            }else {
                printError("Invalid number of parameters!");
                return "Invalid number of parameters!";
            }

            return "start";

        }else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                    return "No valid log level!";
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                    return PROMPT +
                            "Log level changed to level " + level;
                }
            } else {
                printError("Invalid number of parameters!");
                return "Invalid number of parameters!";
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
            return null;
        } else {
            printError("Unknown command");
            printHelp();
            return "Unknown command";
        }
    }
    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");

    }
    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }
    private void printHelp() {

    }
    @Override
    public boolean start() throws IOException {
        boolean allsuccess=true;
        for(int i=0;i<activeServers.size();i++) {
            String host=activeServers.get(i).getNodeHost();
            int port = activeServers.get(i).getNodePort();
            this.clientSocket = new Socket(host, port);
            this.clientComm = new CommModule(this.clientSocket, null);
            this.clientComm.sendAdminMsg(null, START, null, null);
            KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
            if(replyMsg.getStatus()!=START_SUCCESS){
                allsuccess=false;
            }
            this.clientSocket = null;
            this.clientComm.closeConnection();
        }
        return allsuccess;
    }

    @Override
    public boolean stop() throws IOException {

        boolean allsuccess=true;
        for(int i=0;i<activeServers.size();i++) {
            String host=activeServers.get(i).getNodeHost();
            int port = activeServers.get(i).getNodePort();
            this.clientSocket = new Socket(host, port);
            this.clientComm = new CommModule(this.clientSocket, null);
            this.clientComm.sendAdminMsg(null, STOP, null, null);
            KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
            if(replyMsg.getStatus()!=STOP_SUCCESS){
                allsuccess=false;
            }
            this.clientSocket = null;
            this.clientComm.closeConnection();
        }
        return allsuccess;
    }

    public boolean shutdown_server(String name) throws IOException {
        //boolean allsuccess=true;
        for(int i=0;i<activeServers.size();i++) {
            if (activeServers.get(i).getNodeName().equals(name)) {
                String host = activeServers.get(i).getNodeHost();
                int port = activeServers.get(i).getNodePort();
                //String name = activeServers.get(i).getNodeName();
                System.out.println("Shutting down: " + name);
                this.clientSocket = new Socket(host, port);
                this.clientComm = new CommModule(this.clientSocket, null);
                this.clientComm.sendAdminMsg(null, SHUTDOWN, null, null);
                KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                this.clientSocket = null;
                this.clientComm.closeConnection();
                if (replyMsg.getStatus() != SHUTDOWN_SUCCESS) {
                    //allsuccess=false;
                    System.out.println(name + " not shutdown");
                } else {
                    System.out.println(name + " shutdown");
                    idleServers.add(activeServers.get(i));
                    activeServers.remove(i);

                    try {
                        boolean allsuccess = awaitNodes(activeServers.size(), 20000);
                        return allsuccess;
                    } catch (Exception e) {
                        printError("Await nodes failure during shutdown");
                    }
                    return false;
                }

            }

        }
        return false;
    }

    @Override
    public boolean shutdown() throws IOException {
        //boolean allsuccess=true;
        for(int i=0;i<activeServers.size();i++) {

            String host=activeServers.get(i).getNodeHost();
            int port = activeServers.get(i).getNodePort();
            String name = activeServers.get(i).getNodeName();
            System.out.println("Shutting down: "+name);
            this.clientSocket = new Socket(host, port);
            this.clientComm = new CommModule(this.clientSocket, null);
            this.clientComm.sendAdminMsg(null, SHUTDOWN, null, null);
            KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
            if(replyMsg.getStatus()!=SHUTDOWN_SUCCESS){
                //allsuccess=false;
                System.out.println(name+" not shutdown");
            }else{
                System.out.println(name+" shutdown");
                idleServers.add(activeServers.get(i));
                activeServers.remove(i);
                i--;
            }
            this.clientSocket = null;
            this.clientComm.closeConnection();
        }
        try {
            boolean allsuccess = awaitNodes(0, 10000);
            return allsuccess;
        }catch (Exception e){
            printError("Await nodes failure during shutdown");
        }
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        IECSNode inode;
        int numServersAvail = idleServers.size();

        int r = new Random().nextInt(numServersAvail);

        ECSNode node = idleServers.get(r);
        String host=idleServers.get(r).getNodeHost();
        int port = idleServers.get(r).getNodePort();
        String name = idleServers.get(r).getNodeName();
        node = new ECSNode(name, host, port, null);
        //inodes.add(inode);
        Process proc;
        String scriptPath = System.getProperty("user.dir")+"/src/app_kvECS/startnode.sh";
        String serverPath = System.getProperty("user.dir")+"/m3-server.jar";

        String[] cmd = {"sh", scriptPath, node.getNodeHost(),serverPath,String.valueOf(node.getNodePort()),name};
        for (int j = 0; j<6;j++) {
            System.out.println(cmd[j]);
        }
        Runtime run = Runtime.getRuntime();
        try{
            proc=run.exec(cmd);
            String inputLine;
        }catch(IOException e){
            e.printStackTrace();
        }
        System.out.println(name+": "+host+":"+port);
        System.out.println("Moving to activating: "+idleServers.get(r).getNodeName());
        node = (idleServers.get(r));
        idleServers.remove(r);
        boolean success=false;
        try {
            success = awaitNodes(activeServers.size()+1, 10000);
            System.out.println("Await nodes returned: "+success);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(!success){
            System.out.println("New server not checked in");
        }else {
            System.out.println("New server checked in");
        }

        List<ECSNode> thServers = new ArrayList<ECSNode>();
        thServers.addAll(activeServers);
        //thServers.addAll(activatingServers);
        String[] succ=null;
        //get succesor info before node is added to metadata
        if(activeServers.size()>0) {
            succ = getNodeByKey(host + ":" + String.valueOf(port));
        }
        thServers.add(node);
        calc_metadata(thServers);

        try {
            System.out.println(host+":"+String.valueOf(port));
            this.clientSocket = new Socket(host, port);
            this.clientComm = new CommModule(this.clientSocket, null);

            //Init node
            this.clientComm.sendAdminMsg(null, INIT_SERVER, this.metadata, name);
            KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
            if (replyMsg.getStatus() != INIT_SERVER_SUCCESS) {
                System.out.println("Init server failed: " + name);
            }


            //Start node
            this.clientComm.sendAdminMsg(null, START, null, null);
            replyMsg = (KVAdminMsg) clientComm.receiveMsg();
            if (replyMsg.getStatus() != START_SUCCESS) {
                System.out.println("Server start failed: " + name);
            }
            System.out.println("Replied: " + replyMsg.getStatus());
            this.clientSocket = null;
            this.clientComm.closeConnection();
        }catch (Exception e){
            e.printStackTrace();
        }

        if(activeServers.size()>0) {
            try {
                String succhost = succ[0].split(":")[0];
                int succport = Integer.parseInt(succ[0].split(":")[1]);

                this.clientSocket = new Socket(succhost, succport);
                this.clientComm = new CommModule(this.clientSocket, null);

                //Getting the range of new node so successor will move data to it
                String[] newNode = getNodeByKey(host + ":" + String.valueOf(port));
                System.out.println(host + ":" + String.valueOf(port) + "  " + newNode[1]);
                this.clientComm.sendAdminMsg(host + ":" + port, MOVE_DATA, null, newNode[1]);
                KVAdminMsg succreplyMsg = (KVAdminMsg) clientComm.receiveMsg();
                if (succreplyMsg.getStatus() == MOVE_DATA_SUCCESS) {
                    System.out.println("data moved");
                }
                System.out.println("Replied: " + succreplyMsg.getStatus());

                this.clientSocket = null;
                this.clientComm.closeConnection();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        activeServers.add(node);
        UpdateAllNodesMeta();
        inode = (IECSNode) node;
        return inode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) throws IOException {
        setupNodes(count,cacheStrategy,cacheSize);
        Collection<IECSNode> inodes = new ArrayList<IECSNode>();
        IECSNode inode;
        boolean success=false;
        //Sending ssh launch calls on random servers
        for(int i = 0; i < count; i++) {
            int numServersAvail = idleServers.size();

            int r = new Random().nextInt(numServersAvail);

            ECSNode node = idleServers.get(r);
            String host=idleServers.get(r).getNodeHost();
            int port = idleServers.get(r).getNodePort();
            String name = idleServers.get(r).getNodeName();
            inode = (IECSNode) node;
            inodes.add(inode);
            Process proc;
            String scriptPath = System.getProperty("user.dir")+"/src/app_kvECS/startnode.sh";
            String serverPath = System.getProperty("user.dir")+"/m3-server.jar";

            String[] cmd = {"sh", scriptPath, node.getNodeHost(),serverPath,String.valueOf(node.getNodePort()),name};
            for (int j = 0; j<6;j++) {
                System.out.println(cmd[j]);
            }

            Runtime run = Runtime.getRuntime();
            String result="";
            //System.out.println(serverPath);
            //System.out.println(host);
            //System.out.println(port);
            try{
                proc=run.exec(cmd);

                //BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                String inputLine;
                //while ((inputLine = in.readLine()) != null) {
                //inputLine = in.readLine();
                //System.out.println(inputLine);
                //inputLine = in.readLine();
                //System.out.println(inputLine);
                    //result += inputLine;
                //}
                //System.out.println("Try to close");
                //in.close();
                //System.out.println(result);

            }catch(IOException e){
                e.printStackTrace();
            }

            System.out.println(name+": "+host+":"+port);

            System.out.println("Moving to activating: "+idleServers.get(r).getNodeName());
            activatingServers.add(idleServers.get(r));
            idleServers.remove(r);
        }

        try {
            success = awaitNodes(activeServers.size()+activatingServers.size(), 10000);
            System.out.println("Await nodes returned: "+success);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(!success){
            System.out.println("Not all servers checked in");
        }else {
            System.out.println("All servers checked in");
        }

        List<ECSNode> thServers = new ArrayList<ECSNode>();
        thServers.addAll(activeServers);
        //thServers.addAll(activatingServers);
        String[] succ=null;
        for (int i=0; i<activatingServers.size();i++) {
            String host = activatingServers.get(i).getNodeHost();
            int port = activatingServers.get(i).getNodePort();
            String name = activatingServers.get(i).getNodeName();
            //get succesor info before node is added to metadata
            if(activeServers.size()>0) {
                succ = getNodeByKey(host + ":" + String.valueOf(port));
            }
            thServers.add(activatingServers.get(i));
            calc_metadata(thServers);

            try {
                System.out.println(host+":"+String.valueOf(port));
                this.clientSocket = new Socket(host, port);
                this.clientComm = new CommModule(this.clientSocket, null);

                //Init node
                this.clientComm.sendAdminMsg(null, INIT_SERVER, this.metadata, name);
                KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                if (replyMsg.getStatus() != INIT_SERVER_SUCCESS) {
                    System.out.println("Init server failed: " + name);
                }


                //Start node
                this.clientComm.sendAdminMsg(null, START, null, null);
                replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                if (replyMsg.getStatus() != START_SUCCESS) {
                    System.out.println("Server start failed: " + name);
                }
                System.out.println("Replied: " + replyMsg.getStatus());
                this.clientSocket = null;
                this.clientComm.closeConnection();
            }catch (Exception e){
                e.printStackTrace();
            }

            //Find successor server and move data from it

            if(activeServers.size()>0) {

                String succhost = succ[0].split(":")[0];
                int succport = Integer.parseInt(succ[0].split(":")[1]);
//                succhost = activatingServers.get(i).getNodeHost();
//                succport = activatingServers.get(i).getNodePort();
//                String succname = activatingServers.get(i).getNodeName();
                this.clientSocket = new Socket(succhost, succport);
                this.clientComm = new CommModule(this.clientSocket, null);
                //this.clientComm.sendAdminMsg(null, LOCK, null, null);
                //KVAdminMsg succreplyMsg = (KVAdminMsg) clientComm.receiveMsg();
                //if (succreplyMsg.getStatus() == LOCK_SUCCESS) {
                //    System.out.println(succname + " locked");
               // }
                //Getting the range of new node so successor will move data to it
                String[] newNode=getNodeByKey(host+":"+String.valueOf(port));
                System.out.println(succhost+":"+String.valueOf(succport)+"  "+newNode[1]);
                this.clientComm.sendAdminMsg(host+":"+port, MOVE_DATA, null, newNode[1]);
                KVAdminMsg succreplyMsg = (KVAdminMsg) clientComm.receiveMsg();
                if (succreplyMsg.getStatus() == MOVE_DATA_SUCCESS) {
                    System.out.println("data moved");
                }
                System.out.println("Replied: " + succreplyMsg.getStatus());
                UpdateAllNodesMeta();
//                this.clientComm.sendAdminMsg(null, UNLOCK, null, null);
//                succreplyMsg = (KVAdminMsg) clientComm.receiveMsg();
//                if (succreplyMsg.getStatus() == LOCK_SUCCESS) {
//                    System.out.println(succname + " unlocked");
//                }
                this.clientSocket = null;
                this.clientComm.closeConnection();
            }
        }
        //Checking for server znode checkin

        activeServers.addAll(activatingServers);
        activatingServers.clear();


        return inodes;
    }
    private boolean UpdateAllNodesMeta(){
        boolean successful=true;
        for(int i=0;i<activeServers.size();i++){
            try {
                String host = activeServers.get(i).getNodeHost();
                int port = activeServers.get(i).getNodePort();
                String name = activeServers.get(i).getNodeName();
                System.out.println(host+":"+String.valueOf(port));
                this.clientSocket = new Socket(host, port);
                this.clientComm = new CommModule(this.clientSocket, null);
                this.clientComm.sendAdminMsg(null, UPDATE, this.metadata, null);
                KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                if (replyMsg.getStatus() != UPDATE_SUCCESS) {
                    System.out.println(name + " metadata update failed");
                    successful=false;
                }
            }catch(Exception e){
                String name = activeServers.get(i).getNodeName();
                System.out.println("Exception while updating metadata: "+name);
                successful=false;
            }
        }
        return successful;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        long t = System.currentTimeMillis();
        long f = t+timeout;
        while(System.currentTimeMillis()<f) {
            List<String> attendance = zk.getChildren("/keeper",false);
            if(attendance.size()== count){

                return true;
            }

        }
        int present = zk.getChildren("/keeper",false).size();
        System.out.println("Wanted: "+String.valueOf(count)+ " Got: "+String.valueOf(present));
        return false;
    }
    public boolean removeNode(String name){
        List<ECSNode> thServers = new ArrayList<ECSNode>();
        thServers.addAll(activeServers);
        for (int i = 0; i<activeServers.size();i++){
            //System.out.println("See: "+activeServers.get(i).getNodeName());
            if (thServers.get(i).getNodeName().equals(name)){
                System.out.println("Found server to remove");
                thServers.remove(i);
                String removeRange = getNodeByKey(activeServers.get(i).getNodeHost() + ":" + String.valueOf(activeServers.get(i).getNodePort()))[1];
                //recalculate metadata
                calc_metadata(thServers);
                //find successor info
                String[] succ = getNodeByKey(activeServers.get(i).getNodeHost() + ":" + String.valueOf(activeServers.get(i).getNodePort()));
                String succhost = succ[0].split(":")[0];
                int succport = Integer.parseInt(succ[0].split(":")[1]);
                if(succ!=null) {
                    try {


                        //update successor metadata
                        this.clientSocket = new Socket(succhost, succport);
                        this.clientComm = new CommModule(this.clientSocket, null);
                        this.clientComm.sendAdminMsg(null, UPDATE, this.metadata, null);
                        KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                        System.out.println("Replied: " + replyMsg.getStatus());
                        this.clientSocket = null;
                        this.clientComm.closeConnection();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try{
                    //move data to successor
                    this.clientSocket = new Socket(activeServers.get(i).getNodeHost(), activeServers.get(i).getNodePort());
                    this.clientComm = new CommModule(this.clientSocket, null);
                    KVAdminMsg replyMsg = null;

                    if(succ!=null) {
                        this.clientComm.sendAdminMsg(succhost+":"+succport, MOVE_DATA, null, removeRange);
                        replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                        System.out.println("Replied: " + replyMsg.getStatus());
                    }
                    UpdateAllNodesMeta();
//                    this.clientComm.sendAdminMsg(null, SHUTDOWN, null, null);
//                    replyMsg = (KVAdminMsg) clientComm.receiveMsg();
//
//                    if(replyMsg.getStatus()!=SHUTDOWN_SUCCESS){
//                        //allsuccess=false;
//                        System.out.println(name+" not shutdown");
//                    }else{
//                        System.out.println(name+" shutdown");
//                        idleServers.add(activeServers.get(i));
//                        activeServers.remove(i);
//
//                    }
                    shutdown_server(name);
                    this.clientSocket = null;
                    this.clientComm.closeConnection();
                }catch( Exception e){
                    printError("Problem moving data");
                    e.printStackTrace();
                }


                return true;
                //if(replyMsg.getStatus()!=INIT_SERVER_SUCCESS){
                //    allsuccess=false;
                //}


            }
        }
        return false;

    }
    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        System.out.println("Calling removeNodes");
        List<ECSNode> thServers = new ArrayList<ECSNode>();
        thServers.addAll(activeServers);
        for (String name : nodeNames){
            System.out.println("Looking for: "+name);
            for (int i = 0; i<activeServers.size();i++){
                //System.out.println("See: "+activeServers.get(i).getNodeName());
                if (activeServers.get(i).getNodeName().equals(name)){
                    System.out.println("Found server to remove");
                    thServers.remove(i);
                    //recalculate metadata
                    calc_metadata(thServers);
                    //find successor info
                    String[] succ = getNodeByKey(activeServers.get(i).getNodeHost() + ":" + String.valueOf(activeServers.get(i).getNodePort()));
                    if(succ!=null) {
                        try {

                            String succhost = succ[0].split(":")[0];
                            int succport = Integer.parseInt(succ[0].split(":")[1]);
                            //update successor metadata
                            this.clientSocket = new Socket(succhost, succport);
                            this.clientComm = new CommModule(this.clientSocket, null);
                            this.clientComm.sendAdminMsg(null, UPDATE, this.metadata, null);
                            KVAdminMsg replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                            System.out.println("Replied: " + replyMsg.getStatus());
                            this.clientSocket = null;
                            this.clientComm.closeConnection();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    try{
                        //move data to successor
                        this.clientSocket = new Socket(activeServers.get(i).getNodeHost(), activeServers.get(i).getNodePort());
                        this.clientComm = new CommModule(this.clientSocket, null);
                        KVAdminMsg replyMsg = null;

                        if(succ!=null) {
                            this.clientComm.sendAdminMsg(activeServers.get(i).getNodeHost()+":"+String.valueOf(activeServers.get(i).getNodePort()), MOVE_DATA, null, succ[1]);
                            replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                            System.out.println("Replied: " + replyMsg.getStatus());
                        }
                        this.clientComm.sendAdminMsg(null, SHUTDOWN, null, null);
                        replyMsg = (KVAdminMsg) clientComm.receiveMsg();
                        if(replyMsg.getStatus()!=SHUTDOWN_SUCCESS){
                            //allsuccess=false;
                            System.out.println(name+" not shutdown");
                        }else{
                            System.out.println(name+" shutdown");
                            idleServers.add(activeServers.get(i));
                            activeServers.remove(i);
                            i--;
                        }
                        this.clientSocket = null;
                        this.clientComm.closeConnection();
                    }catch( Exception e){
                        printError("Problem moving data");
                        e.printStackTrace();
                    }
                    UpdateAllNodesMeta();

                    //if(replyMsg.getStatus()!=INIT_SERVER_SUCCESS){
                    //    allsuccess=false;
                    //}


                }
            }
        }
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {

        return null;
    }

    @Override
    public String[] getNodeByKey(String Key) {

        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md5.update(Key.getBytes());
        byte[] digest = md5.digest();
        BigInteger key_hash = new BigInteger(1, digest);
        BigInteger addKey = new BigInteger("2", 16);
        key_hash = key_hash.subtract(addKey);
//        for (String mkey: this.metadata.keySet()) {
//            logger.info(mkey + " -> " + this.metadata.get(mkey));
//        }
        // Lookup hashmap to find the appropriate server
        for (HashMap.Entry<String,String> map : this.metadata.get(1).entrySet()) {

            String addr_port = map.getKey();
            String range = map.getValue();
            BigInteger range_start = new BigInteger("0" + range.split(":")[0], 16);
            BigInteger range_end = new BigInteger("0" + range.split(":")[1], 16);
            BigInteger cutEnd = new BigInteger("3", 16);
            BigInteger addStart = new BigInteger("2", 16);

            //range_end = range_end.subtract(cutEnd);
            //range_start = range_start.add(addStart);

            boolean in_range;
//            System.out.println("Key: "+key_hash);
//            System.out.println("Start: "+range_start);
//            System.out.println("End: "+range_end);
            if (range_start.compareTo(range_end) == -1) { // Range start < Range end
                in_range = (key_hash.compareTo(range_start) != -1) && (key_hash.compareTo(range_end) != 1);
            } else { // Range start >= Range end. Use OR: range wraps around hash ring
                in_range = (key_hash.compareTo(range_start) != -1) || (key_hash.compareTo(range_end) != 1);
            }

            if(in_range) { // Key hash falls in this range
                String info[]={addr_port,range};
                return info;
            }

        }


        return null;
    }

    private void calc_metadata(List<ECSNode> nodeList) {

        HashMap<String, String> newReadMetadata = new HashMap<String,String>();
        HashMap<String, String> newWriteMetadata = new HashMap<String,String>();

        // 1. Create a list of hashes and a map of {hashes -> addr:port}
        List<BigInteger> serverHashList = new ArrayList<BigInteger>();
        HashMap<BigInteger,String> serverHashMap = new HashMap<BigInteger,String>();

        for (int i=0; i<nodeList.size(); i++) {
            String addr_port = nodeList.get(i).getNodeHost() + ":" + String.valueOf(nodeList.get(i).getNodePort());
            BigInteger server_hash = computeHash(addr_port);
            serverHashList.add(server_hash);
            serverHashMap.put(server_hash, addr_port);
        }

        // 2. Sort list of hashes
        Collections.sort(serverHashList);

        // 3. Now, build the write metadata by giving to each server the range between its position (inclusive) and
        // that of its predecessor (exclusive) in the sorted serverHashList. The read metadata is between the server
        // position (inclusive) and the position of the predecessor of the predecessor of its predecessor (exclusive)
        for (int i=0; i<serverHashList.size(); i++) {

            // Predecessor is the last element if i==0
            int predec, pp_predec;
            if (i==0) {
                predec = serverHashList.size() - 1;
            } else {
                predec = i-1;
            }

            if (serverHashList.size() > 3) {
                if (i == 0) {
                    pp_predec = serverHashList.size() - 3;
                } else if (i == 1) {
                    pp_predec = serverHashList.size() - 2;
                } else if (i == 2) {
                    pp_predec = serverHashList.size() - 1;
                } else {
                    pp_predec = i - 3;
                }
            } else {
                pp_predec = i;
            }

            BigInteger server_pos = serverHashList.get(i);
            BigInteger server_predecessor_pos = serverHashList.get(predec);
            BigInteger server_pp_predecessor_pos = serverHashList.get(pp_predec);

            if (i==0) { // The exact predecessor position is excluded from the range
                server_predecessor_pos = server_predecessor_pos.add(BigInteger.ONE);
            } else {
                server_predecessor_pos = server_predecessor_pos.subtract(BigInteger.ONE);
            }

            if (i==0||i==1||i==2) { // The exact predecessor position is excluded from the range
                server_pp_predecessor_pos = server_pp_predecessor_pos.add(BigInteger.ONE);
            } else {
                server_pp_predecessor_pos = server_pp_predecessor_pos.subtract(BigInteger.ONE);
            }

            String addr_port = serverHashMap.get(server_pos);
            String write_range = server_predecessor_pos.toString(16) + ":" + server_pos.toString(16);
            String read_range = server_pp_predecessor_pos.toString(16) + ":" + server_pos.toString(16);

            newWriteMetadata.put(addr_port, write_range);
            newReadMetadata.put(addr_port, read_range);
        }

        this.metadata = Arrays.asList(newReadMetadata, newWriteMetadata);
        
    }

    private BigInteger computeHash(String val) {
        // Get D5 hash value as an integer
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md5.update(val.getBytes());
        byte[] digest = md5.digest();
        BigInteger hash = new BigInteger(1, digest);
        return hash;
    }

    public static void main(String[] args) {

        try {
            String cmd = "rm "+System.getProperty("user.dir")+"/server_out/*";
            System.out.println(cmd);
            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        try {
            new LogSetup("logs/ecs.log", Level.INFO);
            if(args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: ECSClient <ecs.config>!");
            }else {
                String configfile = args[0];
                //new ECSClient(configfile);
                ECSClient app = new ECSClient(configfile);
                app.run();

            }

        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
