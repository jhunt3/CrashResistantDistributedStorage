package testing;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import org.junit.Test;
import ecs.IECSNode;
import ecs.ECSNode;
import junit.framework.TestCase;
import shared.comm.CommModule;
import shared.messages.KVMessage;
import shared.messages.KVMsg;
import storage.KVStorage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static shared.messages.KVMessage.StatusType.*;

public class AdditionalTest extends TestCase {
	private KVClient kvClient;
	private ECSClient ecsClient;

	private int port = 50000; // as set in AllTests
	private int port2 = 8008;

	public void setUp() throws Exception {
		ecsClient = new ECSClient("ecs.config");
		kvClient = new KVClient();
//		try {
//			kvStore.connect();
//		} catch (Exception e) {
//			throw new Exception(e);
//		}
	}

//	public void tearDown() {
//		kvStore.disconnect();
//	}

	/**
	 * Check if store initialization works gracefully; if file exists, do nothing, if not, create a file
	 */
//	@Test
//	public void testStorageInit(){
//		Exception ex = null;
//		// File already created by server from AllTests class
//		try{
//			kvStorage.initializeStorage();
//		} catch (Exception e ) {
//			ex = e;
//		}
//		assertNull(ex);
//	}
//
//
//	@Test
//	public void testInStorageAndGetKVObject() {
//		String key = "testInStorageKey";
//		String value = "testInStorageValue";
//
//		Boolean response = null;
//		Exception ex = null;
//
//		try {
//			kvStore.put(key, value);
//			response = kvStorage.inStorage(null, key);
//		} catch (Exception e) {
//			ex = e;
//		}
//		assertTrue(ex == null && response);
//	}
//
//	@Test
//	public void testClearStorage(){
//		String key = "testInStorageKey";
//		String value = "testInStorageValue";
//		Exception ex = null;
//		try {
//			kvStore.put(key, value);
//		} catch (Exception e) {
//			ex = e;
//		}
//		assertTrue(ex == null);
//		assertTrue(kvStorage.inStorage(null, key));
//		kvStorage.clearKVStorage();
//		assertFalse(kvStorage.inStorage(null, key));
//	}
//
//	@Test
//	public void testCommModule() throws IOException, InterruptedException {
//		kvStorage.clearKVStorage();
//
//		// Initialize client socket
//		Socket clientSocket = null;
//		try {
//			clientSocket = new Socket("localhost", port);
//		} catch (IOException e) {
//			System.out.println("Error! Cannot open client socket: " + e);
//		}
//
//		// Start the client communication module
//		CommModule ClientComm = new CommModule(clientSocket, null);
//
//		// Send a put message and receive a reply at the client side
//
//		// PUT test
//		KVMsg in_msg = new KVMsg(PUT, "c_key", "c_val");
//		System.out.println("IN_MSG -> " + "Status: " + in_msg.getStatus() + " Key: " + in_msg.getKey() + " Value: " + in_msg.getValue());
//		// Send a message at the client side
//		ClientComm.sendMsg(in_msg.getStatus(), in_msg.getKey(), in_msg.getValue(), null);
//		// Read the reply also at the client side
//		KVMsg reply_msg = (KVMsg) ClientComm.receiveMsg();
//		System.out.println("REPLY_MSG -> " + "Status: " + reply_msg.getStatus() + " Key: " + reply_msg.getKey() + " Value: " + reply_msg.getValue());
//
//		assertEquals(PUT_SUCCESS, reply_msg.getStatus());
//		assertEquals(reply_msg.getKey(), in_msg.getKey());
//		assertEquals(reply_msg.getValue(), in_msg.getValue());
//
//
//		clientSocket.close();
//	}
//
//
//
//	@Test
//	public void testKVStore() throws Exception {
//
//
//		// Send a put request through the kvStore and read the reply message at the client side
//
//		KVMsg in_msg = new KVMsg(PUT, "kv_key", "kv_val");
//		KVMsg reply_msg = kvStore.put(in_msg.getKey(), in_msg.getValue());
//
//		// PUT test
//		System.out.println("IN_MSG -> " + "Status: " + in_msg.getStatus() + " Key: " + in_msg.getKey() + " Value: " + in_msg.getValue());
//		// Send a message at the client side and read the reply also at the client side
//		System.out.println("REPLY_MSG -> " + "Status: " + reply_msg.getStatus() + " Key: " + reply_msg.getKey() + " Value: " + reply_msg.getValue());
//
//		assertEquals(PUT_SUCCESS, reply_msg.getStatus());
//		assertEquals(reply_msg.getKey(), in_msg.getKey());
//		assertEquals(reply_msg.getValue(), in_msg.getValue());
//
//		in_msg = new KVMsg(PUT, "kv_key_2", "kv_val_2");
//		reply_msg = kvStore.put(in_msg.getKey(), in_msg.getValue());
//
//		// PUT test
//		System.out.println("IN_MSG -> " + "Status: " + in_msg.getStatus() + " Key: " + in_msg.getKey() + " Value: " + in_msg.getValue());
//		// Send a message at the client side and read the reply also at the client side
//		System.out.println("REPLY_MSG -> " + "Status: " + reply_msg.getStatus() + " Key: " + reply_msg.getKey() + " Value: " + reply_msg.getValue());
//
//		assertEquals(PUT_SUCCESS, reply_msg.getStatus());
//		assertEquals(reply_msg.getKey(), in_msg.getKey());
//		assertEquals(reply_msg.getValue(), in_msg.getValue());
//	}
//
//	/**
//	 * Counter logic for testing concurrency assuming key="counter" exists in storage
//	 */
//	public synchronized void incrementCounter() throws Exception {
//		int currCount = Integer.parseInt(kvStorage.get("counter"));
//		Thread.sleep(100);
//		kvStorage.put("counter", Integer.toString(currCount+1));
//	}
//
//	@Test
//	public void testKVStorageConcurrency() throws Exception {
//		int numberOfThreads = 2;
//		ExecutorService service = Executors.newFixedThreadPool(20);
//		CountDownLatch latch = new CountDownLatch(numberOfThreads);
//
//		kvStorage.put("counter", "0");
//
//		for (int i = 0; i < numberOfThreads; i++) {
//			service.submit(() -> {
//				try {
//					incrementCounter();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				latch.countDown(); //
//			});
//		}
//		latch.await();
//		int counterValue = Integer.parseInt(kvStorage.get("counter"));
//		System.out.println("Counter Value: " + counterValue);
//		assertEquals(numberOfThreads, counterValue);
//	}
//
//	@Test
//	public void testWithoutConn() throws Exception {
//		String getCmd = kvClient.handleCommand("get key");
//		String putCmd = kvClient.handleCommand("put key value");
//		String disConn = kvClient.handleCommand("disconnect");
//		System.out.println("Get: "+getCmd);
//		System.out.println("Put: "+putCmd);
//		System.out.println("Disconnect: "+disConn);
//		assertEquals(getCmd, "ERROR: Not connected to server");
//		assertEquals(putCmd, "ERROR: Not connected to server");
//		assertEquals(disConn, "ERROR: Not connected to server");
//	}
//
//	@Test
//	public void testFrontToBackConn() throws Exception {
//		String tryconnect = kvClient.handleCommand("connect 127.0.0.1 "+String.valueOf(port));
//		assertEquals(tryconnect, "Connected to server 127.0.0.1 at port "+String.valueOf(port));
//		String trydisconnect = kvClient.handleCommand("disconnect");
//		assertEquals(trydisconnect, "Disconnected from server");
//
//	}
//
//	@Test
//	public void testPutSyntax() throws Exception {
//		kvStorage.clearKVStorage();
//		kvClient.handleCommand("connect 127.0.0.1 "+String.valueOf(port));
//		String putBasic = kvClient.handleCommand("put key value");
//		System.out.println(putBasic);
//		assertEquals(putBasic, "PUT SUCCESS");
//
//		String putDelete = kvClient.handleCommand("put key");
//		System.out.println(putDelete);
//		assertEquals(putDelete, "DELETE SUCCESS");
//
//		kvClient.handleCommand("put key value");
//
//		String putNull = kvClient.handleCommand("put key null");
//		System.out.println(putNull);
//		assertEquals(putNull, "DELETE SUCCESS");
//
//		kvClient.handleCommand("put key value 1 2 3");
//		String getSpaceValue = kvClient.handleCommand("get key");
//		System.out.println(getSpaceValue);
//		assertEquals(getSpaceValue, "value 1 2 3");
//		kvClient.handleCommand("put key");
//
//		kvClient.handleCommand("put key null 1 2 3");
//		String getNullSpaceValue = kvClient.handleCommand("get key");
//		System.out.println(getNullSpaceValue);
//		assertEquals("null 1 2 3",getNullSpaceValue);
//		kvClient.handleCommand("put key");
//	}
//
//
////	@Test
////	public void testRequestWhenConnectionBroken() throws Exception {
////		kvServer = new KVServer(port2, 100, "LRU");
////		kvClient.handleCommand("connect 127.0.0.1 "+String.valueOf(port2));
////		kvServer.clearStorage();
////		kvClient.handleCommand("put key value");
////		kvServer.close();
////		String getValue = kvClient.handleCommand("get key");
////		System.out.println("Get Response: "+getValue);
////		assertEquals("ERROR: Disconnected from server", getValue);
////		assertFalse(kvClient.isRunning());
////	}
//
//	@Test
//	public void testLargePutArgs() throws Exception {
//		String longValue = "";
//		for(int i=0; i<122880;i++){
//			longValue = longValue + "i";
//		}
//		kvClient.handleCommand("connect 127.0.0.1 "+String.valueOf(port));
//		kvStorage.clearKVStorage();
//		String putValue = kvClient.handleCommand("put 01234567890123456789 "+longValue);
//		System.out.println("Put0 Response: "+putValue);
//		assertEquals("PUT SUCCESS", putValue);
//		String put1Value = kvClient.handleCommand("put 012345678901234567890 "+longValue);
//		System.out.println("Put1 Response: "+put1Value);
//		assertEquals("PUT ERROR", put1Value);
////		longValue = longValue+"iiiiiii";
////		System.out.println(longValue.length());
////		String put2Value = kvClient.handleCommand("put 11234567890123456789 "+longValue);
////		System.out.println("Put2 Response: "+put2Value);
////		assertEquals("PUT ERROR", put2Value);
//	}


	//Milestone 2

	/*@Test
	public void testShutDown() throws Exception {
		String response;

		response=ecsClient.handleCommand("addNodes 8");
		String[] addrs=response.split(" ");
		System.out.println(response);

		String[] hosts=new String[9];
		int[] ports=new int[9];
		String[] names=new String[9];
		for(int i = 1;i<addrs.length;i++) {
			hosts[i]=addrs[i].split(":")[0];
			ports[i]=Integer.parseInt(addrs[i].split(":")[1]);
			names[i]=addrs[i].split(":")[2];

		}
		response=ecsClient.handleCommand("shutDown");
		int activeServers= ecsClient.activeServers.size();
		assertEquals(0,activeServers);


	}
	@Test
	public void testMultiNodeServer() throws Exception {
		String response;

		response=ecsClient.handleCommand("addNodes 8");
		String[] addrs=response.split(" ");
		String[] hosts=new String[9];
		int[] ports=new int[9];
		String[] names=new String[9];
		for(int i = 1;i<addrs.length;i++) {
			hosts[i]=addrs[i].split(":")[0];
			ports[i]=Integer.parseInt(addrs[i].split(":")[1]);
			names[i]=addrs[i].split(":")[2];

		}

		//ecsClient.handleCommand("addNodes 2");
		System.out.println("connect "+hosts[1]+" "+String.valueOf(ports[1]));
		kvClient.handleCommand("connect "+hosts[1]+" "+String.valueOf(ports[1]));
		kvClient.handleCommand("put 1 MNS1");
		kvClient.handleCommand("put 2 MNS2");
		kvClient.handleCommand("put 3 MNS3");
		kvClient.handleCommand("put 4 MNS4");
		kvClient.handleCommand("put 5 MNS5");
		kvClient.handleCommand("put 6 MNS6");
		kvClient.handleCommand("put 7 MNS7");
		kvClient.handleCommand("put 8 MNS8");
		kvClient.handleCommand("put 9 MNS9");
		kvClient.handleCommand("put 10 MNS10");


		response=kvClient.handleCommand("get 1");
		assertEquals("MNS1",response);
		response=kvClient.handleCommand("get 2");
		assertEquals("MNS2",response);
		response=kvClient.handleCommand("get 3");
		assertEquals("MNS3",response);
		response=kvClient.handleCommand("get 4");
		assertEquals("MNS4",response);
		response=kvClient.handleCommand("get 5");
		assertEquals("MNS5",response);
		response=kvClient.handleCommand("get 6");
		assertEquals("MNS6",response);
		response=kvClient.handleCommand("get 7");
		assertEquals("MNS7",response);
		response=kvClient.handleCommand("get 8");
		assertEquals("MNS8",response);
		response=kvClient.handleCommand("get 9");
		assertEquals("MNS9",response);
		response=kvClient.handleCommand("get 10");
		assertEquals("MNS10",response);

		kvClient.handleCommand("put 1");
		kvClient.handleCommand("put 2");
		kvClient.handleCommand("put 3");
		kvClient.handleCommand("put 4");
		kvClient.handleCommand("put 5");
		kvClient.handleCommand("put 6");
		kvClient.handleCommand("put 7");
		kvClient.handleCommand("put 8");
		kvClient.handleCommand("put 9");
		kvClient.handleCommand("put 10");
		ecsClient.handleCommand("shutDown");

	}
	@Test
	public void testNodeAddition() throws Exception {
		String response;

		response=ecsClient.handleCommand("addNode");
		String addrport = response.split(" ")[4];
		String host = addrport.split(":")[0];
		int port = Integer.parseInt(addrport.split(":")[1]);


		//ecsClient.handleCommand("addNodes 2");
		System.out.println("connect "+host+" "+String.valueOf(port));
		kvClient.handleCommand("connect "+host+" "+String.valueOf(port));
		kvClient.handleCommand("put 1 SIR1");
		kvClient.handleCommand("put 2 SIR2");
		kvClient.handleCommand("put 3 SIR3");
		kvClient.handleCommand("put 4 SIR4");
		ecsClient.handleCommand("addNode");

		int activeServers= ecsClient.activeServers.size();
		assertEquals(2,activeServers);

		kvClient.handleCommand("put 1");
		kvClient.handleCommand("put 2");
		kvClient.handleCommand("put 3");
		kvClient.handleCommand("put 4");
		ecsClient.handleCommand("shutDown");

	}
	@Test
	public void testNodeRemoval() throws Exception {
		String response;

		response=ecsClient.handleCommand("addNodes 2");
		String[] addrs=response.split(" ");
		System.out.println(response);

		String host1= addrs[1].split(":")[0];
		int port1 = Integer.parseInt(addrs[1].split(":")[1]);
		String name1=addrs[1].split(":")[2];
		String host2= addrs[2].split(":")[0];
		int port2 = Integer.parseInt(addrs[2].split(":")[1]);
		String name2=addrs[2].split(":")[2];
		//ecsClient.handleCommand("addNodes 2");
		System.out.println("connect "+host1+" "+String.valueOf(port1));
		kvClient.handleCommand("connect "+host1+" "+String.valueOf(port1));
		kvClient.handleCommand("put 1 SIR1");
		kvClient.handleCommand("put 2 SIR2");
		kvClient.handleCommand("put 3 SIR3");
		kvClient.handleCommand("put 4 SIR4");
		ecsClient.handleCommand("removeNode "+name1);
		kvClient.handleCommand("connect "+host2+" "+String.valueOf(port2));
		int activeServers= ecsClient.activeServers.size();
		assertEquals(1,activeServers);

		kvClient.handleCommand("put 1");
		kvClient.handleCommand("put 2");
		kvClient.handleCommand("put 3");
		kvClient.handleCommand("put 4");
		ecsClient.handleCommand("shutDown");

	}
	@Test
	public void testAddingRemovingNodes() throws Exception {
		String response;
		response=ecsClient.handleCommand("addNodes 8");
		String[] addrs=response.split(" ");
		System.out.println(response);

		String[] hosts=new String[9];
		int[] ports=new int[9];
		String[] names=new String[9];
		for(int i = 1;i<addrs.length;i++) {
			hosts[i]=addrs[i].split(":")[0];
			ports[i]=Integer.parseInt(addrs[i].split(":")[1]);
			names[i]=addrs[i].split(":")[2];

		}
		int activeServers=0;
		//response=ecsClient.handleCommand("shutDown");
		//response=ecsClient.handleCommand("addNodes 8");
		System.out.println(response);
		activeServers=ecsClient.activeServers.size();
		assertEquals(8,activeServers);
		System.out.println("Removing: "+names[1]+","+names[2]);
		response=ecsClient.handleCommand("removeNode "+names[1]);
		System.out.println(response);
		response=ecsClient.handleCommand("removeNode "+names[2]);
		System.out.println(response);
		activeServers=ecsClient.activeServers.size();
		assertEquals(6,activeServers);
		response=ecsClient.handleCommand("addNodes 2");
		System.out.println(response);
		activeServers=ecsClient.activeServers.size();
		assertEquals(8,activeServers);
		response=ecsClient.handleCommand("removeNode "+names[3]);
		System.out.println(response);
		response=ecsClient.handleCommand("removeNode "+names[4]);
		System.out.println(response);
		response=ecsClient.handleCommand("removeNode "+names[5]);
		System.out.println(response);
		response=ecsClient.handleCommand("removeNode "+names[6]);
		System.out.println(response);
		activeServers=ecsClient.activeServers.size();
		assertEquals(4,activeServers);
		response=ecsClient.handleCommand("addNodes 4");
		System.out.println(response);
		activeServers=ecsClient.activeServers.size();
		assertEquals(8,activeServers);
		response=ecsClient.handleCommand("shutDown");
		System.out.println(response);
		activeServers=ecsClient.activeServers.size();
		assertEquals(0,activeServers);
	}*/
	@Test
	public void testStorageIntegrityOnAdd() throws Exception {
		String response;

		response=ecsClient.handleCommand("addNode");
		System.out.println(response);
		String addrport = response.split(" ")[4];
		String host = addrport.split(":")[0];
		int port = Integer.parseInt(addrport.split(":")[1]);
		//ecsClient.handleCommand("addNodes 2");
		System.out.println("connect "+host+" "+String.valueOf(port));
		kvClient.handleCommand("connect "+host+" "+String.valueOf(port));
		kvClient.handleCommand("put 1 SIA1");
		kvClient.handleCommand("put 2 SIA2");
		kvClient.handleCommand("put 3 SIA3");
		kvClient.handleCommand("put 4 SIA4");
		ecsClient.handleCommand("addNode");
		response=kvClient.handleCommand("get 1");
		assertEquals("SIA1",response);
		response=kvClient.handleCommand("get 2");
		assertEquals("SIA2",response);
		response=kvClient.handleCommand("get 3");
		assertEquals("SIA3",response);
		response=kvClient.handleCommand("get 4");
		assertEquals("SIA4",response);

		kvClient.handleCommand("put 1");
		kvClient.handleCommand("put 2");
		kvClient.handleCommand("put 3");
		kvClient.handleCommand("put 4");
		ecsClient.handleCommand("shutDown");

	}

	@Test
	public void testStorageIntegrityOnRemove() throws Exception {
		String response;

		response=ecsClient.handleCommand("addNodes 2");
		String[] addrs=response.split(" ");
		System.out.println(response);

		String host1= addrs[1].split(":")[0];
		int port1 = Integer.parseInt(addrs[1].split(":")[1]);
		String name1=addrs[1].split(":")[2];
		String host2= addrs[2].split(":")[0];
		int port2 = Integer.parseInt(addrs[2].split(":")[1]);
		String name2=addrs[2].split(":")[2];
		//ecsClient.handleCommand("addNodes 2");
		System.out.println("connect "+host1+" "+String.valueOf(port1));
		kvClient.handleCommand("connect "+host1+" "+String.valueOf(port1));
		kvClient.handleCommand("put 1 SIR1");
		kvClient.handleCommand("put 2 SIR2");
		kvClient.handleCommand("put 3 SIR3");
		kvClient.handleCommand("put 4 SIR4");
		ecsClient.handleCommand("removeNode "+name1);
		kvClient.handleCommand("connect "+host2+" "+String.valueOf(port2));
		response=kvClient.handleCommand("get 1");
		assertEquals("SIR1",response);
		response=kvClient.handleCommand("get 2");
		assertEquals("SIR2",response);
		response=kvClient.handleCommand("get 3");
		assertEquals("SIR3",response);
		response=kvClient.handleCommand("get 4");
		assertEquals("SIR4",response);

		kvClient.handleCommand("put 1");
		kvClient.handleCommand("put 2");
		kvClient.handleCommand("put 3");
		kvClient.handleCommand("put 4");
		ecsClient.handleCommand("shutDown");

	}
/*
	public void testMoveData() throws Exception {
		String response;

		System.out.println("Adding 2 nodes");
		response=ecsClient.handleCommand("addNodes 2");
		System.out.println("Response to adding nodes: "+ response);
		String[] addrs = response.split(" ");
		String[] hosts=new String[3];
		int[] ports=new int[3];
		String[] names=new String[3];
		String name="";
		for (int i = 1;i<addrs.length;i++) {
			hosts[i]=addrs[i].split(":")[0];
			ports[i]=Integer.parseInt(addrs[i].split(":")[1]);
			names[i]=addrs[i].split(":")[2];
		}

		System.out.println("Connecting to one of servers");
		kvClient.handleCommand("connect "+ hosts[1] + " "+ ports[1]);
		kvClient.handleCommand("put k1 v1");
		kvClient.handleCommand("put k2 v2");
		kvClient.handleCommand("put k3 v3");
		kvClient.handleCommand("put k4 v4");

		System.out.println("Removing Node");
        response = ecsClient.handleCommand("removeNode " + name);
        System.out.println("Remove node response: " + response);

		assertEquals(kvClient.handleCommand("get k1"), "v1");
		assertEquals(kvClient.handleCommand("get k2"), "v2");
		assertEquals(kvClient.handleCommand("get k3"), "v3");
		assertEquals(kvClient.handleCommand("get k4"), "v4");

		kvClient.handleCommand("put k1");
		kvClient.handleCommand("put k2");
		kvClient.handleCommand("put k3");
		kvClient.handleCommand("put k4");

		System.out.println("Shutting Down");
		ecsClient.handleCommand("shutDown");
		int activeServers= ecsClient.activeServers.size();
		assertEquals(0,activeServers);
	}

	public void testFlushData() throws Exception {

//        int port = 60000;
//        int cachSize = 100;
//        String serverName = "testServer";
//
//        KVServer server = new KVServer(port, cachSize, "FIFO", serverName);
//        server.start();

//        client.handleCommand("connect localhost 60000");
//        client.handleCommand("put k1 v1");
//        server.dataToFlush.add((Object) "k1");
//        client.handleCommand("put k2 v2");
//        server.dataToFlush.add((Object) "k2");
//        server.flushData();
//
//        assertNull(server.getKV("k1"));
//        assertNull(server.getKV("k2"));


//        System.out.println("Shutting Down");
//        ecsClient.handleCommand("shutDown");
//        int activeServers= ecsClient.activeServers.size();
//        assertEquals(0,activeServers);
		assertTrue(true);
	}

	public void testWriteLock(){
		assertTrue(true);
	}*/

	// Milestone 3

	@Test
	public void testMetadata() throws Exception {

		ECSClient ecsClient_metadata = new ECSClient("ecs_metadata.config");

		HashMap<String, String> readMetadataGolden = new HashMap<String, String>();
		readMetadataGolden.put("127.0.0.1:50000", "a98109598267087dfc364fae4cf24579:358343938402ebb5110716c6e836f5a2");
		readMetadataGolden.put("127.0.0.1:50001", "358343938402ebb5110716c6e836f5a3:dcee0277eb13b76434e8dcd31a387709");
		readMetadataGolden.put("127.0.0.1:50002", "dcee0277eb13b76434e8dcd31a38770a:b3638a32c297f43aa37e63bbd839fc7e");
		readMetadataGolden.put("127.0.0.1:50003", "b3638a32c297f43aa37e63bbd839fc7f:a98109598267087dfc364fae4cf24578");

		HashMap<String, String> writeMetadataGolden = new HashMap<String, String>();
		writeMetadataGolden.put("127.0.0.1:50000", "dcee0277eb13b76434e8dcd31a38770a:358343938402ebb5110716c6e836f5a2");
		writeMetadataGolden.put("127.0.0.1:50001", "b3638a32c297f43aa37e63bbd839fc7f:dcee0277eb13b76434e8dcd31a387709");
		writeMetadataGolden.put("127.0.0.1:50002", "a98109598267087dfc364fae4cf24579:b3638a32c297f43aa37e63bbd839fc7e");
		writeMetadataGolden.put("127.0.0.1:50003", "358343938402ebb5110716c6e836f5a3:a98109598267087dfc364fae4cf24578");

		String response;

		// Add the 4 nodes from ecs_metadata.config
		response = ecsClient_metadata.handleCommand("addNodes 4");
		System.out.println(response);

		HashMap<String, String> readMetadata;
		HashMap<String, String> writeMetadata;
		List<HashMap<String, String>> metadata = ecsClient_metadata.metadata;

		// Get ECS metadata
		readMetadata = metadata.get(0);
		writeMetadata = metadata.get(1);

		boolean success = true;
		String range;
		String goldenRange;

		// Compare ECS metadata with golden reference
		for (String mkey : readMetadata.keySet()) {
			range = readMetadata.get(mkey);
			goldenRange = readMetadataGolden.get(mkey);
			if (!goldenRange.equals(range)) {
				success = false;
			}
		}

		for (String mkey : writeMetadata.keySet()) {
			range = writeMetadata.get(mkey);
			goldenRange = writeMetadataGolden.get(mkey);
			if (!goldenRange.equals(range)) {
				success = false;
			}
		}

		response = ecsClient_metadata.handleCommand("shutDown");
		int activeServers = ecsClient_metadata.activeServers.size();

		assertEquals(0, activeServers);
		assertTrue(success);

		ecsClient_metadata.handleCommand("quit");
	}

	@Test
	public void testHashList() throws Exception {

		ECSClient ecsClient_metadata = new ECSClient("ecs_metadata.config");

		List<String> hashListGolden = new ArrayList<String>();
		hashListGolden.add("127.0.0.1:50000");
		hashListGolden.add("127.0.0.1:50003");
		hashListGolden.add("127.0.0.1:50002");
		hashListGolden.add("127.0.0.1:50001");

		String response;

		// Add the 4 nodes from ecs_metadata.config
		response = ecsClient_metadata.handleCommand("addNodes 4");
		System.out.println(response);

		// Get ECS hashList
		List<String> hashList = ecsClient_metadata.hashList;

		boolean success = true;
		String server;
		String goldenServer;

		// Compare ECS hashList with golden reference
		for (int i = 0; i < hashList.size(); i++) {
			server = hashList.get(i);
			goldenServer = hashListGolden.get(i);
			if (!goldenServer.equals(server)) {
				success = false;
			}

		}

		response = ecsClient_metadata.handleCommand("shutDown");
		int activeServers = ecsClient_metadata.activeServers.size();

		assertEquals(0, activeServers);
		assertTrue(success);

		ecsClient_metadata.handleCommand("quit");

	}
}
