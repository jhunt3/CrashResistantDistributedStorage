package app_kvServer;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Initialize the KVServer with the metadata and block it for client requests,
     * i.e., all client requests are rejected with an SERVER_STOPPED error message; ECS requests have to be processed.
     * @param metadata Updated metadata provided by ECS
     */
    public void initKVServer(HashMap<String, String> metadata, String serverName);

    /**
     * Starts the KVServer, all client requests and all ECS requests are processed.
     */
    public void setStart();

    /**
     * Stops the KVServer, all client requests are rejected and only ECS requests are processed.
     */
    public void setStop();

    /**
     * Exits the KVServer application.
     */
    public void shutDown();

    /**
     * Lock the KVServer for write operations.
     */
    public void lockWrite();

    /**
     * Unlock the KVServer for write operations, and flushes data out of old server.
     */
    public void unLockWrite();

    /**
     * Returns whether or not the server has been stopped
     */
    public boolean isStopped();

    /**
     * Returns the status of write lock
     */
    public boolean isWriteLock();

    /**
     * Transfer a subset (range) of the KVServer’s data to another KVServer
     * (reallocation before removing this server or adding a new KVServer to the ring); send a notification to the ECS,
     * if data transfer is completed.
     */
    public void moveData(String range, String sendTo) throws Exception;

    /**
     * Flush not-in-range data out of storage upon unlocking write lock
     */
    public void flushData() throws Exception;

    /**
     * Updates the metadata
     */
    public void update(HashMap<String, String> metadata);

    /**
     * Given a string key, return the MD5 Hash value of the key as a BigInterger
     */


    /**
     * Returns a boolean indicating if the given key is in this storage server's range
     * @param key String key input from user
     */
    public boolean inRange(String key);

    /**
     * @return Return updated metadata hashmap to the client upon request to a key that is not
     * within the range of this server
     */
    public HashMap<String,String> getMetadata();

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

}
