package storage;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import storage.cache.ICache;

import javax.sound.midi.SysexMessage;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;


public class KVStorage {
    private static final Logger logger = Logger.getRootLogger();
    private final String fileName;
    private final String fileNameReplica_1;
    private final String fileNameReplica_2;
    private final File storage;
    private final File replica_1;
    private final File replica_2;

    public KVStorage(String strategy, int cacheSize, String storageName){
        // file names
        this.fileName = storageName + ".json";
        this.fileNameReplica_1 = storageName + "_replica_1.json";
        this.fileNameReplica_2 = storageName + "_replica_2.json";

        // files/storages
        this.storage = new File(fileName);
        this.replica_1 = new File(fileNameReplica_1);
        this.replica_2 = new File(fileNameReplica_2);
        initializeStorage();
    }

    private synchronized boolean hasCache(){
        return false;
    }

    private synchronized boolean storageExists(){
        return storage.exists();
    }

    private synchronized void createStorageFile(){
        try {
            if (storage.createNewFile()) {
                logger.info("File created: " + storage.getName());
            } else {
                logger.info("File already exists.");
            }
            if (replica_1.createNewFile()) {
                logger.info("File created: " + replica_1.getName());
            } else {
                logger.info("File already exists.");
            }
            if (replica_2.createNewFile()) {
                logger.info("File created: " + replica_2.getName());
            } else {
                logger.info("File already exists.");
            }
        } catch (IOException e) {
            logger.error("An error occurred.");
            e.printStackTrace();
        }
    }

    public synchronized void initializeStorage(){
        if (!storageExists()){
            logger.info("Creating Storage Files");
            createStorageFile();
        }
    }

    /**
     * Returns JSONObject from storage file
     */
    public synchronized JSONObject getKVObject(){
        // Read json from file
        if (storageExists()){
            try {
                JSONParser parser = new JSONParser();
                return (JSONObject) parser.parse(new FileReader(fileName));
            } catch (Exception e){
                e.printStackTrace();
            }
        } else {
            // Shouldn't get here since file is created upon instantiation; this is to double check incase the file
            // is accidentally deleted
            logger.error("Storage file not found. Initializing storage file");
            initializeStorage();
        }
        return new JSONObject();
    }

    public synchronized boolean inStorage(JSONObject kvObject, String key){
        // TODO check cache

        if (kvObject == null){
            kvObject = getKVObject();
        }

        if (!kvObject.isEmpty()) {
            return kvObject.containsKey(key);
        } else {
            return false;
        }
    }

    /**
     * Get a value from a key in the storage; checks all three storage files
     * @param key key in key-value pair
     * @return return value of associated key if exists
     *         return null otherwise
     */
    public synchronized String get(String key){
        logger.info("Get: key = " + key);

        JSONObject kvObject = getKVObject();
        JSONObject replica_1Object = getReplicaObject("replica_1");
        JSONObject replica_2Object = getReplicaObject("replica_2");

        if (inStorage(kvObject, key)){
            return (String) kvObject.get(key);
        } else if (inStorage(replica_1Object, key)){
            return (String) replica_1Object.get(key);
        } else if (inStorage(replica_2Object, key)){
            return (String) replica_2Object.get(key);
        }

        return null;
    }

    /**
     * Update/Put the corresponding value given a key
     * If the the value is null, delete the entry
     * @param key in key-value pair
     * @param value to be updated; null to delete key entry
     * @return return null if successful, else return error message
     */
    public synchronized void put(String key, String value) throws Exception{
        JSONObject kvObject = getKVObject();

        // update
        putKVHelper(key, value, kvObject, fileName);
    }

    public synchronized void flushData(List<Object> dataToFlush) throws Exception{
        if (dataToFlush!=null) {
            for (Object key : dataToFlush) {
                String keyStr = key.toString();
                try {
                    this.put(keyStr, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }
    }

    /**
     * Clear the storage of KV database
     */
    public synchronized void clearKVStorage(){
        logger.info("Clearing KV Storage...");

        // TODO clear cache
        try (FileWriter file = new FileWriter(fileName)) {
            file.write("{}");
            file.flush();
        } catch (IOException e) {
            logger.error("Unable to clear database");
        }
    }

    // Replication (Milestone 3)

    /**
     * Call this function inside predecessorChanges() to put key/val into replica storage files
     * @param key key to insert
     * @param value value to insert
     * @param replica_name name of the replica storage file (replica_1, or replica_2)
     */
    public synchronized void replicaPutKV(String key, String value, String replica_name) throws Exception {

        JSONObject kvObject = getReplicaObject(replica_name);

        String fileName;
        if (replica_name.equals("replica_1")){
            fileName = this.fileNameReplica_1;
        } else {
            fileName = this.fileNameReplica_2;
        }

        // update
        putKVHelper(key, value, kvObject, fileName);
    }

    /**
     * Reused code in both replicaPutKV and putKV functions
     */
    private void putKVHelper(String key, String value, JSONObject kvObject, String fileName) throws Exception {
        if (value != null && !value.equals("") && !value.equals("null")){
            logger.info("Put: key = " + key + ", value = " + value);

            // Update value
            kvObject.put(key, value);

            // Write updated JSON file
            try (FileWriter file = new FileWriter(fileName)) {
                file.write(kvObject.toJSONString());
                file.flush();
            } catch (IOException e) {
                logger.error("Unable to write json file");
                throw new Exception(e.toString());
            }

        } else {
            // delete key
            logger.info("Delete: key = " + key);

            String msg = (String) kvObject.remove(key);
            if (msg == null) {
                logger.error("Key does not exist.");
            }

            // Write updated JSON file
            try (FileWriter file = new FileWriter(fileName)) {
                logger.info("Updating storage");
                file.write(kvObject.toJSONString());
                file.flush();
            } catch (IOException e) {
                logger.error("Unable to write json file");
                throw new Exception(e.toString());
            }
        }
    }

    /**
     * Fetch replica objects for read (get) function
     */
    public synchronized JSONObject getReplicaObject(String replicaName){
        if (replicaName.equals("replica_1")){
            if (this.replica_1.exists()){
                try{
                    JSONParser parser = new JSONParser();
                    return (JSONObject) parser.parse(new FileReader(fileNameReplica_1));
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        } else if (replicaName.equals("replica_2")){
            if (this.replica_2.exists()){
                try{
                    JSONParser parser = new JSONParser();
                    return (JSONObject) parser.parse(new FileReader(fileNameReplica_2));
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return new JSONObject();
    }

    public synchronized void mergeReplica(String replicaName) throws Exception {
        JSONObject replicaObject = this.getReplicaObject(replicaName);
        for (Object key : replicaObject.keySet()){
            String keyStr = key.toString();
            String valStr = replicaObject.get(key).toString();
            this.put(keyStr, valStr);
        }
    }

    public synchronized void flushReplicas(){
        try (FileWriter file = new FileWriter(fileNameReplica_1)) {
            file.write("{}");
            file.flush();
        } catch (IOException e) {
            logger.error("Unable to clear database");
        }
        try (FileWriter file = new FileWriter(fileNameReplica_2)) {
            file.write("{}");
            file.flush();
        } catch (IOException e) {
            logger.error("Unable to clear database");
        }
    }
}

