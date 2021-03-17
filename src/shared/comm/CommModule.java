package shared.comm;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import shared.messages.KVAdminMsg;
import shared.messages.KVMessage;
import shared.messages.KVMsg;

import javax.swing.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import static shared.messages.KVMessage.StatusType;
import static shared.messages.KVMessage.StatusType.*;

public class CommModule implements ICommModule, Runnable {

    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private ObjectOutputStream output;
    private ObjectInputStream input;
    private KVServer server;
    private boolean isOpen;

    /**
     * @param socket Client Socket (output of socket.accept() for the server, socket for the client), or ECS Socket.
     * @param server Server object if the module is being instantiated in the server, null otherwise.
     */
    public CommModule(Socket socket, KVServer server) {
        try {
            this.output = new ObjectOutputStream(socket.getOutputStream());
            this.output.flush();
            this.input = new ObjectInputStream(socket.getInputStream());
            if (server != null) {
                logger.info("Server communication module connected");
            } else {
                logger.info("Client communication module connected");
            }
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established! ", ioe);
        }
        this.isOpen = true;
        this.server = server;
        this.socket = socket;
    }

    /**
     * The run method is intended for usage by the server (not client!). It allows each server communication
     * thread to monitor new messages, call the appropriate server methods to process requests, and send
     * reply messages to the client upon completing a put/get/delete operation.
     */
    // Server communication
    @Override
    public void run() {
        if (this.server != null) { // Only meant to run for the server comm module
            try {
                while (isOpen) {
                    KVMessage msg = receiveMsg();
                    if (msg.isAdminMessage()){
                        try {
                            System.out.println("received adminmsg");
                            KVAdminMsg in_msg = (KVAdminMsg) msg;
                            KVAdminMsg out_msg = adminServe(in_msg);
                            sendAdminMsg(null, out_msg.getStatus(), null, null, null);
                        } catch (Exception e){
                            logger.info("Server found an exception when receiving/processing/sending admin message! " + e);
                            isOpen = false;
                        }
                    } else {
                        try {
                            KVMsg in_msg = (KVMsg) msg;
                            KVMsg out_msg = serve(in_msg);
                            sendMsg(out_msg.getStatus(), out_msg.getKey(), out_msg.getValue(), out_msg.getMetadata());
                        } catch (Exception e) {
                            logger.info("Server found an exception when receiving/processing/sending message! " + e);
                            isOpen = false;
                        }
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (this.socket != null) {
                        this.input.close();
                        this.output.close();
                        this.socket.close();
                    }
                } catch (IOException ioe) {
                    logger.error("Error! Unable to tear down connection! ", ioe);
                }
            }
        }
    }

    /**
     * @return Message read in the objectInputStream
     */
    @Override
    public KVMessage receiveMsg() throws IOException, Exception {
        KVMessage msg = null;

        try {
            msg = (KVMessage) this.input.readObject();
        } catch (Exception e) {
            throw new Exception("No message received at Comm Module.");
        }

        if (msg != null){
            if (!msg.isAdminMessage()){
                KVMsg kvMsg = (KVMsg) msg;
                if (this.server != null) {
                    logger.info("Message received by server ->" + " Status: " + kvMsg.getStatus() + " Key: " + kvMsg.getKey() + " Value: " + kvMsg.getValue());
                } else {
                    logger.info("Message received by client ->" + " Status: " + kvMsg.getStatus() + " Key: " + kvMsg.getKey() + " Value: " + kvMsg.getValue());
                }
            }else {
                KVAdminMsg kvAdminMsg = (KVAdminMsg) msg;
                if (this.server != null){
                    logger.info("Message received by server ->" + " Status: " + kvAdminMsg.getStatus());
                } else {
                    logger.info("Message received by ECS ->" + " Status: " + kvAdminMsg.getStatus());
                }
            }
        }

        return msg;
    }

    /**
     * @param status status field
     * @param key key to be sent
     * @param value value to be sent. Can be left empty or null for messages that do not require a value
     * @throws IOException
     */
    @Override
    public void sendMsg(StatusType status, String key, String value, List<HashMap<String,String>> metadata) throws IOException {
        KVMsg msg = new KVMsg(status, key, value, metadata);
        this.output.writeObject(msg);
        this.output.flush();
        if (this.server != null) {
            logger.info("Message sent by server ->" + " Status: " + msg.getStatus() + " Key: " + msg.getKey() + " Value: " + msg.getValue());
        } else {
            logger.info("Message sent by client ->" + " Status: " + msg.getStatus() + " Key: " + msg.getKey() + " Value: " + msg.getValue());
        }
    }

    public void sendPropagateMsg(StatusType status, String hostPort, JSONObject obj) throws IOException {
        KVMsg msg = new KVMsg(status, hostPort, obj);
        this.output.writeObject(msg);
        this.output.flush();
        if (this.server != null) {
            logger.info("Message sent by server ->" + " Status: " + msg.getStatus());
        } else {
            logger.info("Message sent by client ->" + " Status: " + msg.getStatus());
        }
    }

    /**
     * Send input message to the Server for processing (calls the appropriate server methods),
     * and build reply message.
     * @param msg Input message (from the client)
     * @return Output Message (from the server)
     * @throws Exception Exception
     */
    @Override
    public KVMsg serve(KVMsg msg) throws Exception {

        if (msg != null) {
	    logger.debug("In serve");

            KVMessage.StatusType status = msg.getStatus();
            String key = msg.getKey();
            String value = msg.getValue();

            KVMessage.StatusType out_status = null;
            String out_value = null;

            int key_len = key.getBytes(StandardCharsets.UTF_8).length;
            int val_len = key.getBytes(StandardCharsets.UTF_8).length;

	    logger.debug("Checking if stopped or write locked");

            if (this.server.isStopped()) { // SERVER_STOPPED
                out_status = SERVER_STOPPED;
                out_value = null;
            }  else { // Server is not stopped nor write-locked
                switch (status) {

                    case GET:

                        if (!this.server.inRange(key,"get")) { // This server is not responsible for this key. Send updated metadata to KVStore
                            out_status = SERVER_NOT_RESPONSIBLE;
                            List<HashMap<String,String>> out_metadata = this.server.getMetadata();
                            return new KVMsg(out_status, key, null, out_metadata);
                        } else {
                            out_value = this.server.getKV(key);

                            if (out_value != null) { // key currently in storage
                                out_status = GET_SUCCESS;
                            } else { // key not in storage
                                out_status = GET_ERROR;
                            }
                        }

                        break;

                    case PUT:
                        if (this.server.isWriteLock()) { // SERVER_WRITE_LOCK
                            out_status = SERVER_WRITE_LOCK;
                            out_value = null;
                        }else {
                            logger.debug("PUT request being served");
                            if (key_len < 1 || key_len > 20 || val_len > 122880) { // Size constraints are violated
                                out_status = PUT_ERROR;
                            } else if (!this.server.inRange(key,"put")) { // This server is not responsible for this key. Send updated metadata to KVStore
                                logger.debug("Not in range");
                                out_status = SERVER_NOT_RESPONSIBLE;
                                List<HashMap<String,String>> out_metadata = this.server.getMetadata();
                                return new KVMsg(out_status, key, null, out_metadata);
                            } else {
                                logger.debug("In range");
                                out_value = value;
                                boolean delete = (value == null || value.equals("null") || value.equals(""));
                                if (this.server.inStorage(key)) { // key currently in storage
                                    this.server.putKV(key, value);
                                    if (delete) { // Value is null. User wants to delete the key
                                        out_status = DELETE_SUCCESS;
                                    } else {
                                        out_status = PUT_UPDATE;
                                    }
                                } else { // key not in storage
                                    if (delete) { // Value is null. User wants to delete the key, but it is not present
                                        out_status = DELETE_ERROR;
                                    } else {
                                        this.server.putKV(key, value);
                                        out_status = PUT_SUCCESS;
                                    }
                                }
                            }
                        }

                        break;

                    case PROPAGATE:
                        String hostPort = msg.hostPort;
                        JSONObject obj = msg.obj;
                        this.server.predecessorChanges(hostPort, obj);
                        out_status = PROPAGATE_SUCCESS;

                        break;




                    default:
                        break;




                }
            }

            return new KVMsg(out_status, key, out_value);
        } else { // msg == null
            return null;
        }
    }

    public void sendAdminMsg(String kvServer, StatusType status, List<HashMap<String,String>> metadata, List<String> hashList, String range) {
        // send admin msg to ECS or server
        KVAdminMsg adminMsg = new KVAdminMsg(kvServer, status, metadata, hashList, range);
        try {
            this.output.writeObject(adminMsg);
            this.output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (this.server != null){
            logger.info("Message sent by server -> " + "Status: " + adminMsg.getStatus());
        } else {
            logger.info("Message sent by ECS -> " + "Status: " + adminMsg.getStatus());
        }
    }

    /**
     * Serve the admin message send from ECS
     */
    public KVAdminMsg adminServe(KVAdminMsg adminMsg) throws Exception{
        if (adminMsg.getStatus() != null){
            KVMessage.StatusType status = adminMsg.getStatus();
            KVMessage.StatusType outStatus = null;
            switch (status){
                case INIT_SERVER:
                    if (adminMsg.getMetadata() == null){
                        outStatus = INIT_SERVER_FAILED;
                        break;
                    }
                    this.server.initKVServer(adminMsg.getMetadata(), adminMsg.getRange());
                    outStatus = INIT_SERVER_SUCCESS;
                    break;
                case START:
                    this.server.setStart();
                    outStatus = START_SUCCESS;
                    break;
                case STOP:
                    this.server.setStop();
                    outStatus = STOP_SUCCESS;
                    break;
                case SHUTDOWN:
                    this.server.shutDown();
                    outStatus = SHUTDOWN_SUCCESS;
                    break;
                case LOCK:
                    this.server.lockWrite();
                    outStatus = LOCK_SUCCESS;
                    break;
                case UNLOCK:
                    this.server.unLockWrite();
                    outStatus = UNLOCK_SUCCESS;
                    break;
                case MOVE_DATA:
                    logger.debug("Input data: ");
                    logger.debug(adminMsg.getRange());
                    logger.debug(adminMsg.getNewKvServer());
                    if (adminMsg.getRange() == null || adminMsg.getNewKvServer() == null){
                        outStatus = MOVE_DATA_FAILED;
                        break;
                    }
                    try{
                        this.server.moveData(adminMsg.getRange(), adminMsg.getNewKvServer());
                    } catch (Exception e){
                        logger.debug("Printing trace:");
                        logger.debug(e);
                        e.printStackTrace();
                        outStatus = MOVE_DATA_FAILED;
                        break;
                    }
                    outStatus = MOVE_DATA_SUCCESS;
                    break;
                case UPDATE:
                    if (adminMsg.getMetadata() == null){
                        outStatus = UPDATE_FAILED;
                        break;
                    }
                    this.server.update(adminMsg.getMetadata(), adminMsg.getHashList());
                    outStatus = UPDATE_SUCCESS;
                    break;
                case FLUSH:
                    try {
                        this.server.flushData();
                    }catch (Exception e) {
                        outStatus = FLUSH_FAILED;
                        break;
                    }
                    outStatus = FLUSH_SUCCESS;

                case PROPAGATE_ADMIN:
                    this.server.propagateOnAdminMsg();
                    outStatus = PROPAGATE_SUCCESS;

                    break;
                case MERGE_REPLICA:
                    this.server.mergeReplica(adminMsg.getRange());
                    outStatus=MERGE_REPLICA_SUCCESS;
                default:
                    break;
            }
            return new KVAdminMsg(null, outStatus, null, null, null);
        }
        return null;
    }

    /**
     * Close the communication module
     */
    @Override
    public void closeConnection() {
        this.isOpen = false;
        if (this.socket != null) {
            try {
                this.input.close();
                this.output.close();
                this.socket.close();
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
            logger.info("Connection closed!");
        }
    }
}
