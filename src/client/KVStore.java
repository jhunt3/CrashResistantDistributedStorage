package client;

import org.apache.log4j.Logger;
import shared.comm.CommModule;
import shared.messages.KVMsg;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static shared.messages.KVMessage.StatusType.*;

public class KVStore extends Thread implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();

	private String address;
	private int port;
	private Socket clientSocket;
	private CommModule clientComm;
	private List<HashMap<String, String>> metadata;

	/**
	 * Initialize KVStore with address and port of KVServer.
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.metadata = new ArrayList<>();
		HashMap<String,String> read_metadata = new HashMap<String,String>();
		HashMap<String,String> write_metadata = new HashMap<String,String>();
		read_metadata.put(address + ":" + String.valueOf(port), "00000000000000000000000000000000:ffffffffffffffffffffffffffffffff");
		write_metadata.put(address + ":" + String.valueOf(port), "00000000000000000000000000000000:ffffffffffffffffffffffffffffffff");
		this.metadata.add(read_metadata);
		this.metadata.add(write_metadata);
	}

	/**
	 * Instantiate the communication module and establish a connection with the server.
	 * @throws Exception
	 */
	@Override
	public void connect() throws Exception {

		this.clientSocket = new Socket(this.address, this.port);
		this.clientComm = new CommModule(this.clientSocket, null);
		try {
			this.clientComm.sendMsg(GET_METADATA, "nothing","nothing",this.metadata);
			KVMsg replyMsg = (KVMsg) clientComm.receiveMsg();

			if (replyMsg != null) {

				this.metadata = replyMsg.getMetadata();

			} else {
				System.out.println("Server did not reply to GET_METADATA! Trying to connect to a different server");
				throw new Exception("Server did not reply to GET_METADATA! Trying to connect to a different server");
			}

		} catch (Exception e) { // Server failed. Try connecting to another server in the metadata
			System.out.println("Server " + this.address + ":" + String.valueOf(this.port) + " did not reply to GET_METADATA! Trying to connect to new server");
//			this.connRandomServer(this.address, this.port);
//			System.out.println("PUT to new server -> " + this.address + ":" + String.valueOf(this.port));
		}

	}

	/**
	 * Close the connection with the server.
	 */
	@Override
	public void disconnect() {
		if (this.clientSocket != null) {
			this.clientSocket = null;
			this.clientComm.closeConnection();
		}
	}

	/**
	 * Issue a put operation to the server.
	 * @param key   the key that identifies the given value.
	 * @param value the value that is indexed by the given key.
	 * @return Reply message from the server.
	 * @throws Exception
	 */
	@Override
	public KVMsg put(String key, String value) throws Exception {
		System.out.println("Start PUT -> " + this.address + ":" + String.valueOf(this.port));

		connectCorrServer(key, "put");

		// 1. Forward request to server
		long timeout = 20000; // timeout = 20 s
		long t = System.currentTimeMillis();
		long f = t+timeout;

		while(System.currentTimeMillis() < f) {
			try {
				this.clientComm.sendMsg(PUT, key, value, null);
				KVMsg replyMsg = (KVMsg) clientComm.receiveMsg();

				if (replyMsg != null) {
					// If we get a SERVER_NOT_RESPONSIBLE reply, we need to update the metadata and retry
					while (replyMsg.getStatus() == SERVER_NOT_RESPONSIBLE) {
						//System.out.println("PUT Server not responsible - stale metadata");
						this.metadata = replyMsg.getMetadata();

						connectCorrServer(key, "put");

						this.clientComm.sendMsg(PUT, key, value, null);
						replyMsg = (KVMsg) clientComm.receiveMsg();
					}

					System.out.println("End PUT -> " + this.address + ":" + String.valueOf(this.port));

					return replyMsg;
				} else {
					System.out.println("Server did not reply to PUT! Trying to connect to a different server");
					throw new Exception("Server did not reply! Trying to connect to a different server");
				}
				
			} catch (Exception e) { // Server failed. Try connecting to another server in the metadata
				System.out.println("Server " + this.address + ":" + String.valueOf(this.port) + " did not reply to PUT! Trying to connect to new server");
				this.connRandomServer(this.address, this.port);
				System.out.println("PUT to new server -> " + this.address + ":" + String.valueOf(this.port));
			}
		}

		throw new Exception("Timeout! Could not connect to any servers for PUT request");

	}

	/**
	 * Issue a get operation to the server.
	 * @param key the key that identifies the value.
	 * @return Reply message from the server.
	 * @throws Exception
	 */
	@Override
	public KVMsg get(String key) throws Exception {
		System.out.println("Start GET -> " + this.address + ":" + String.valueOf(this.port));

		connectCorrServer(key, "get");

		// 1. Forward request to server
		long timeout = 20000; // timeout = 20 s
		long t = System.currentTimeMillis();
		long f = t+timeout;

		while(System.currentTimeMillis() < f) {
			try {
				this.clientComm.sendMsg(GET, key, "", null);
				KVMsg replyMsg = (KVMsg) clientComm.receiveMsg();

				if (replyMsg != null) {
					// If we get a SERVER_NOT_RESPONSIBLE reply, we need to update the metadata and retry
					while (replyMsg.getStatus() == SERVER_NOT_RESPONSIBLE) {
						//System.out.println("GET Server not responsible - stale metadata");
						this.metadata = replyMsg.getMetadata();

						connectCorrServer(key, "get");

						this.clientComm.sendMsg(GET, key, "", null);
						replyMsg = (KVMsg) clientComm.receiveMsg();
					}

					System.out.println("End GET -> " + this.address + ":" + String.valueOf(this.port));

					return replyMsg;
				} else {
					System.out.println("Server did not reply to GET! Trying to connect to a different server");
					throw new Exception("Server did not reply! Trying to connect to a different server");
				}

			} catch (Exception e) { // Server failed. Try connecting to another server in the metadata
				System.out.println("Server " + this.address + ":" + String.valueOf(this.port) + " did not reply to GET! Trying to connect to new server");
				this.connRandomServer(this.address, this.port);
				System.out.println("GET to new server -> " + this.address + ":" + String.valueOf(this.port));
			}
		}

		throw new Exception("Timeout! Could not connect to any servers for GET request");

	}

	/**
	 * Given the key value, this function performs a map lookup to determine the appropriate server to route the client
	 * request to, and connects to this appropriate server (and disconnects the previous server connection)
	 * @param key Key value.
	 * @param mode Read - put or write - get.   
	 */
	private void connectCorrServer(String key, String mode) {

		// Get key MD5 hash value as an integer (key_hash)
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		md5.update(key.getBytes());
		byte[] digest = md5.digest();
		BigInteger key_hash = new BigInteger(1, digest);

		// Lookup hashmap to find the appropriate server
		HashMap<String,String> working_metadata = null;
		if (mode == "put") {
			//System.out.println("PUT metadata");
			working_metadata = this.metadata.get(1);
		} else if (mode == "get") {
			//System.out.println("GET metadata");
			working_metadata = this.metadata.get(0);
		}

		//working_metadata.entrySet().forEach(entry->{System.out.println(entry.getKey() + " " + entry.getValue());});
		//System.out.println("Key hash: " + key_hash.toString(16));
		
		for (HashMap.Entry<String,String> map : working_metadata.entrySet()) {

			String addr_port = map.getKey();
			String range = map.getValue();
			BigInteger range_start = new BigInteger("0" + range.split(":")[0], 16);
			BigInteger range_end = new BigInteger("0" + range.split(":")[1], 16);

			boolean in_range;

			if (range_start.compareTo(range_end) == -1) { // Range start < Range end
				in_range = (key_hash.compareTo(range_start) != -1) && (key_hash.compareTo(range_end) != 1);
			} else { // Range start >= Range end. Use OR: range wraps around hash ring
				in_range = (key_hash.compareTo(range_start) != -1) || (key_hash.compareTo(range_end) != 1);
			}

			if(in_range) { // Key hash falls in this range
				String address = addr_port.split(":")[0];
				int port = Integer.parseInt(addr_port.split(":")[1]);

				if((!address.equals(this.address)) || port != this.port) { // Need to update the server connection
					this.disconnect();
					this.address = address;
					this.port = port;
					//System.out.println(address);
					try {
						this.connect();
					} catch (Exception e) {
						this.connRandomServer(this.address, this.port);
					}
				}
				break;
			}

		}

	}

	/**
	 * Disconnect from the current server and try connecting to another server (random) listed in the local metadata
	 */
	private void connRandomServer(String failed_addr, int failed_port) {
		this.disconnect();

		long timeout = 5000; // timeout = 5 s
		long t = System.currentTimeMillis();
		long f = t+timeout;

		HashMap<String, String> working_metadata = this.metadata.get(1);
		String[] server_info = working_metadata.keySet().toArray(new String[working_metadata.size()]);

		int server_idx = 0;
		while(System.currentTimeMillis() < f) {
			String server_addr_port = server_info[server_idx];
			server_idx = (server_idx+1)%server_info.length;
			String address = server_addr_port.split(":")[0];
			int port = Integer.parseInt(server_addr_port.split(":")[1]);

			if ((!address.equals(failed_addr)) || (port!=failed_port)) { // Dont try to reconnect to the failed server
				this.address = address;
				this.port = port;
				try {
					this.connect();
					return;
				} catch (Exception e) {
					continue;
				}
			}
		}
	}

}
