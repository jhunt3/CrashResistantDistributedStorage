package testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import client.KVStore;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	public List<KVServer> serverList = new ArrayList<KVServer>();

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);

//			// Setup for Storage Server Test
//			// port numbers: 60001 60002
//			int NUM_SERVERS = 2;
//
//			for (int i=1; i<=NUM_SERVERS; i++){
//				// int port, int cacheSize, String strategy, String name
//				int port = 60000 + i;
//				int cacheSize = 100;
//				String serverName = "testServer" + i;
//
//				KVServer server = new KVServer(port, cacheSize, "FIFO", serverName);
//				server.start();
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
//		clientSuite.addTestSuite(StorageServerTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		return clientSuite;
	}
	
}
