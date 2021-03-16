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
    private final File storage;

    public KVStorage(String strategy, int cacheSize, String storageName){
        this.fileName = storageName + ".json";
        this.storage = new File(fileName);
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
        } catch (IOException e) {
            logger.error("An error occurred.");
            e.printStackTrace();
        }
    }

    public synchronized void initializeStorage(){
        if (!storageExists()){
            logger.info("Creating Storage File");
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
     * Get a value from a key in the storage
     * @param key key in key-value pair
     * @return return value of associated key if exists
     *         return null otherwise
     */
    public synchronized String get(String key){
        logger.info("Get: key = " + key);

        // TODO Check cache
        JSONObject kvObject = getKVObject();
        if (inStorage(kvObject, key)){
            return (String) kvObject.get(key);
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

        // TODO update cache

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
}
