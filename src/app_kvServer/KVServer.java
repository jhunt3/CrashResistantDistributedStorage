package app_kvServer;


import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.json.simple.JSONObject;
import shared.comm.CommModule;
import shared.messages.KVMsg;
import storage.KVStorage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static shared.messages.KVMessage.StatusType.*;

public class KVServer extends Thread implements IKVServer{
	private static final Logger logger = Logger.getRootLogger();
	private final int port;
	private final int cacheSize;
	private ServerSocket serverSocket;
	private final String hostname;

	private final KVStorage storage;

	// Flags
	private boolean stopped; // Accepts ECS commands only (true), accept all requests (false)
	private boolean writeLock; // Lock for synchronized writing
	private boolean running; // Shutdown (false), or running (true)

	// Additional
	private List<HashMap<String, String>> metadata;
	public List<Object> dataToFlush = new ArrayList<Object>(); // consists of key objects to be deleted
	private String serverName;
	private final CountDownLatch isConnected = new CountDownLatch(1);
	private KVStore store;

	// Replication
	private List<String> hashList;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy, String name) {
		this.hostname = "127.0.0.1";
		this.port = port;
		this.cacheSize = cacheSize;
		// Storage

		//String storageName = this.hostname + this.port;
		this.serverName=name;
		this.storage = new KVStorage(strategy, cacheSize, this.serverName);
		this.running = false;
		serverName=name;

	}

//	public KVServer(String host, int port, int cacheSize, String strategy, String name) {
//		this.hostname = host;
//		this.port = port;
//		this.cacheSize = cacheSize;
//		this.strategy = strategy;
//		String storageName = this.hostname + this.port;
//		this.storage = new KVStorage(strategy, cacheSize, storageName);
//	}

	@Override
	public void initKVServer(List<HashMap<String, String>> metadata, List<String> hashList,String name){
		this.metadata = metadata;
		this.hashList=hashList;
	}

	@Override
	public void setStart(){
		this.stopped = false;
	}

	@Override
	public void setStop(){
		this.stopped = true;
	}

	@Override
	public void shutDown(){
		this.close(); // close socket
	}

	@Override
	public void lockWrite(){
		this.writeLock = true;
	}

	@Override
	public void unLockWrite(){
		this.writeLock = false;
	}

	@Override
	public boolean isStopped(){
		return this.stopped;
	}

	@Override
	public boolean isWriteLock(){
		return this.writeLock;
	}

	@Override
	public void moveData(String range, String sendTo) throws Exception{
		this.lockWrite();
		JSONObject kvObject = this.storage.getKVObject();
		String host = sendTo.split(":")[0];
		int port = Integer.parseInt(sendTo.split(":")[1]);

		Socket clientSocket = new Socket(host, port);
		CommModule clientComm = new CommModule(clientSocket, null);
		logger.debug("Host: "+host);
		logger.debug("Port: "+String.valueOf(port));
		logger.debug("Range: "+range);

		for (Object key : kvObject.keySet()) {
			logger.debug("In interator.");
			String keyStr = (String) key;
			logger.debug("Key: " + keyStr);
			BigInteger rangeStart = new BigInteger("0" + range.split(":")[0], 16);
			BigInteger rangeEnd = new BigInteger("0" + range.split(":")[1], 16);
			BigInteger keyHash = getKeyHash(keyStr);

			logger.debug("KeyHash: " +keyHash.toString());
			boolean in_range;
			//if ((keyHash.compareTo(rangeStart) != -1) && (keyHash.compareTo(rangeEnd) != 1)){ // in new range
			if (rangeStart.compareTo(rangeEnd) == -1) { // Range start < Range end
				in_range = (keyHash.compareTo(rangeStart) != -1) && (keyHash.compareTo(rangeEnd) != 1);
			} else { // Range start >= Range end. Use OR: range wraps around hash ring
				in_range = (keyHash.compareTo(rangeStart) != -1) || (keyHash.compareTo(rangeEnd) != 1);
			}
			logger.debug("In range? "+in_range);
			if (in_range) {

				// add to flush data list
				logger.debug("Adding key to flush");
				this.dataToFlush.add(key);
				logger.debug("Added key to flush");
				logger.debug("Getting key value");
				String value = kvObject.get(key).toString();
				logger.debug("Got key value");

				logger.debug("KVPair: " + keyStr+":"+value);

				try {
					clientComm.sendMsg(PUT, keyStr, value, null);
					KVMsg replyMsg = (KVMsg) clientComm.receiveMsg();
					if (replyMsg.getStatus() != PUT_SUCCESS) {
						System.out.println("Move data failure");
					}

				} catch (Exception e) {

					e.printStackTrace();
					throw e;
				}
			}
		}
		logger.debug("Starting flush data");

		this.flushData();
		logger.debug("Finished flush data");
		clientSocket = null;
		logger.debug("Closing conn");
		clientComm.closeConnection();
		logger.debug("Closed conn");
		this.unLockWrite();

	}

//	public void moveData_(String range, KVServer newServer) throws Exception{
//		JSONObject kvObject = this.storage.getKVObject();
//		for (Object key : kvObject.keySet()) {
//			String keyStr = (String) key;
//			BigInteger rangeStart = new BigInteger("0" + range.split(":")[0], 16);
//			BigInteger rangeEnd = new BigInteger("0" + range.split(":")[1], 16);
//			BigInteger keyHash = getKeyHash(keyStr);
//			if ((keyHash.compareTo(rangeStart) != -1) && (keyHash.compareTo(rangeEnd) != 1)){ // in new range
//				// add to flush data list
//				this.dataToFlush.add(key);
//				String value = kvObject.get(key).toString();
//				try {
//					newServer.putKV(keyStr, value);
//				} catch (Exception e) {
//					e.printStackTrace();
//					throw e;
//				}
//			}
//		}
//	}

	@Override
	public void flushData() throws Exception{

		try {
			this.storage.flushData(this.dataToFlush);
		} catch (Exception e){
			throw e;
		}
		this.dataToFlush = new ArrayList<>();
	}

	@Override
	public void update(List<HashMap<String, String>> metadata, List<String> hashList) {
		this.metadata = metadata;
		this.hashList = hashList;
		this.flushReplicas();
	}

	private BigInteger getKeyHash(String key){
		// Get key MD5 hash value as an integer (key_hash)
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		assert md5 != null;
		md5.update(key.getBytes());
		byte[] digest = md5.digest();
		return new BigInteger(1, digest);
	}

	@Override
	public boolean inRange(String key, String mode) {
		// Get key MD5 hash value as an integer (key_hash)
		logger.debug("Checking if it's in range");
		BigInteger keyHash = getKeyHash(key);

		String addr = this.getHostname() + ':' + this.getPort();
		
		logger.debug("Got hostname and port -> " + addr);

		HashMap<String,String> working_metadata = null;
		if (mode == "put") {
			working_metadata = this.metadata.get(1);
		} else if (mode == "get") {
			working_metadata = this.metadata.get(0);
		}

		String range = working_metadata.get(addr);

		logger.debug("Got metadata range: " + range);

		for (String mkey: working_metadata.keySet()) {
			logger.debug(mkey + " -> " + working_metadata.get(mkey));
		}

		BigInteger rangeStart = new BigInteger("0" + range.split(":")[0], 16);
		BigInteger rangeEnd = new BigInteger("0" + range.split(":")[1], 16);

		logger.debug("Got range big ints");

		boolean in_range;

		if (rangeStart.compareTo(rangeEnd) == -1) { // Range start < Range end
			logger.debug("Range start < range end");
			in_range = (keyHash.compareTo(rangeStart) != -1) && (keyHash.compareTo(rangeEnd) != 1);
		} else { // Range start >= Range end. Use OR: range wraps around hash ring
			logger.debug("Range end < range start");
			in_range = (keyHash.compareTo(rangeStart) != -1) || (keyHash.compareTo(rangeEnd) != 1);
		}

		logger.debug("Done with range check in server");
		return in_range;
	}

	@Override
	public List<HashMap<String, String>> getMetadata() {
		return this.metadata;
	}

	@Override
	public int getPort(){
		return this.port;
	}

	@Override
	public String getHostname(){
		return this.hostname;
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return IKVServer.CacheStrategy.None;
	}

	@Override
	public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
	public boolean inStorage(String key){
		return storage.inStorage(null, key);
	}

	@Override
	public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getKV(String key) throws Exception{
		return storage.get(key);
	}

	@Override
	public void putKV(String key, String value) throws Exception{
		storage.put(key, value);
		JSONObject obj = new JSONObject();
		obj.put(key, value);
		propagateChanges(obj);
	}

	private boolean isRunning() {
		return this.running;
	}

	@Override
	public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
	public void clearStorage(){
		storage.clearKVStorage();
	}

	/**
	 * Creates new server socket
	 * @return boolean true if successfully initialized; else false
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * Initializes and starts the server.
	 * Loops until the the server should be closed. Creates comm module for each client bind.
	 */
	public void run(){

		this.running = initializeServer();
		try {
			ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						isConnected.countDown();
					}
				}
			});
			byte[] locData = (this.hostname+":"+String.valueOf(this.port)).getBytes(StandardCharsets.UTF_8);
			try{

				final String rootPath = zk.create("/keeper/"+serverName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				zk.setData("/keeper/"+serverName, locData, zk.exists("/keeper/"+serverName,true).getVersion());
				logger.info("Node created at: " + rootPath);

			}catch(KeeperException | InterruptedException e){
				e.printStackTrace();

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(serverSocket != null) {
			while(this.isRunning()){
				try {
					Socket client = serverSocket.accept(); // blocking
					System.out.println("Starting commModule");
					CommModule connection =
							new CommModule(client, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
		storage.clearKVStorage();
		System.exit(0);
	}

	@Override
	public void kill(){
		// TODO
	}

	@Override
	public void close(){
		logger.debug("Closing the program");
		this.running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	// Replication (Milestone 3)
	// call on commModule from adminMsg StatusType PROPAGATE_ADMIN
	public void propagateOnAdminMsg() throws Exception {
		this.propagateChanges(this.storage.getKVObject());
	}

	/**
	 * Propagates changes to successor nodes (Invoked after add/remove node, and putKV)
	 * 1. Set up two commModules that connect to its two successors based on hashList
	 * 2. Send KVMsg to the nodes with PROPAGATE StatusType and key(s)/value(s) to update
	 * 3. Capture response
	 */
	public void propagateChanges(JSONObject obj) throws Exception {
		logger.debug("Getting successors");
		List<String> successors = getSuccessors();
		logger.debug("Got successors");
		String successor1 = successors.get(0);
		String successor2 = successors.get(1);

		String host1 = successor1.split(":")[0];
		int port1 = Integer.parseInt(successor1.split(":")[1]);

		String host2 = successor2.split(":")[0];
		int port2 = Integer.parseInt(successor2.split(":")[1]);
		logger.debug("Propagating to: "+successor1+" "+successor2);
		Socket socket1 = new Socket(host1, port1);
		CommModule commMod1 = new CommModule(socket1, null);

		Socket socket2 = new Socket(host2, port2);
		CommModule commMod2 = new CommModule(socket2, null);
		logger.debug(obj);
		if (obj != null && !obj.isEmpty()){
			commMod1.sendPropagateMsg(PROPAGATE, this.hostname+":"+String.valueOf(this.port), obj);
			commMod2.sendPropagateMsg(PROPAGATE, this.hostname+":"+String.valueOf(this.port), obj);
			KVMsg replyMsg1 = (KVMsg) commMod1.receiveMsg();
			KVMsg replyMsg2 = (KVMsg) commMod2.receiveMsg();
			if (replyMsg1.getStatus() != PROPAGATE_SUCCESS) {
				System.out.println("Propagation to first successor of" + this.serverName + "failed.");
			}
			if (replyMsg2.getStatus() != PROPAGATE_SUCCESS){
				System.out.println("Propagation to second successor of" + this.serverName + "failed.");
			}
		}
	}


	/**
	 * Called on replica nodes; updates the appropriate replica.json given obj and predecessor's host:port
	 * 1. Loop through hashList to see which replica file corresponds to the predecessor
	 * 2. Loop through obj to insert key(s)/val(s) or delete key/val from replica_n.json
	 * 3. Capture response
	 */
	public void predecessorChanges(String predecessorHostPort, JSONObject obj) throws Exception {
		logger.debug("Getting Predecessors");
		List<String> predecessors = getPredecessors();
		logger.debug("Predecessors:");
		logger.debug(predecessors);
		logger.debug("Looking for "+predecessorHostPort);
		int index = predecessors.indexOf(predecessorHostPort);
		String replicaName;
		logger.debug("Index of predecessor: "+String.valueOf(index));
		if (index == 0){
			replicaName = "replica_1";
		} else {
			replicaName = "replica_2";
		}
		logger.debug("Server Stuff");

		for (Object key : obj.keySet()){
			String keyStr = key.toString();
			String valStr = obj.get(key).toString();
			this.storage.replicaPutKV(keyStr, valStr, replicaName);
		}
	}

	/**
	 * MergeReplica merges the given replica_name.json to this server's storage
	 */
	public void mergeReplica(String replica_name) throws Exception {
		this.storage.mergeReplica(replica_name);
	}

	public List<String> getSuccessors(){
		String hostPort = this.hostname + ":" + this.port;
		int currServerIndex = this.hashList.indexOf(hostPort);
		return Arrays.asList(this.hashList.get((currServerIndex+1)%this.hashList.size()), this.hashList.get((currServerIndex+2)%this.hashList.size()));
	}

	public List<String> getPredecessors(){
		String hostPort = this.hostname + ":" + this.port;
		logger.debug("Looking for "+hostPort);
		logger.debug(this.hashList);
		int currServerIndex = this.hashList.indexOf(hostPort);

		int listLength = hashList.size();
		int replica1Index = currServerIndex - 2;
		replica1Index=(((replica1Index%listLength)+listLength)%listLength);
		int replica2Index = currServerIndex - 1;
		replica2Index = (((replica2Index%listLength)+listLength)%listLength);
//		if (replica1Index < 0){
//			replica1Index = (listLength) + replica1Index;
//		}
//		if (replica2Index < 0){
//			replica2Index = (listLength) + replica2Index;
//		}

		return Arrays.asList(this.hashList.get(replica1Index), this.hashList.get(replica2Index));
	}
	public void flushReplicas(){
		this.storage.flushReplicas();
	}

	/**
	 * Main entry point for the KVServer application.
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {

		try {
			new LogSetup("logs/server.log", Level.ALL);
			if (args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				String name = args[3];
				KVServer kvServer = new KVServer(port, cacheSize, strategy, name);
				kvServer.setStop();
				kvServer.start(); // begin thread
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}

