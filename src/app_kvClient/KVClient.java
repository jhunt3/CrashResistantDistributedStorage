package app_kvClient;

import java.io.*;
import java.net.UnknownHostException;

import client.KVStore;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import client.KVCommInterface;
import shared.messages.KVMessage;
import shared.messages.KVMsg;

import static shared.messages.KVMessage.StatusType.*;

public class KVClient implements IKVClient {

    private Logger logger = Logger.getRootLogger();
    private boolean running;

    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore store = null;
    private boolean stop = false;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    private String serverAddress;
    private int serverPort;

    public void run() throws Exception {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                setRunning(false);
                printError("ERROR: Disconnected from server");
            }
        }
    }
    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException {
        store = new KVStore(hostname, port);

        setRunning(true);
        logger.info("Connection established");
    }

    @Override
    public KVCommInterface getStore() {

        return store;
    }
    public void printToUser(String outString) {
        System.out.println(outString);

    }

    public String handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            if(running) {
                store.disconnect();
            }
            System.out.println(PROMPT + "Application exit!");
            return "Application exit";
        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                    store.connect();
                    System.out.println("Connected to server " + tokens[1] + " at port " + tokens[2]);
                    return "Connected to server " + tokens[1] + " at port " + tokens[2];
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");

                    logger.info("Unable to parse argument <port>", nfe);
                    return "No valid address. Port must be a number!";
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");

                    logger.info("Unknown Host!", e);
                    return "Unknown Host!";
                } catch (IOException e) {
                    printError("Could not establish connection!");

                    logger.warn("Could not establish connection!", e);
                    return "Could not establish connection!";
                }
            } else {
                printError("Invalid number of parameters!");
                return "Invalid number of parameters!";
            }

        } else if(tokens[0].equals("disconnect")) {
            if(!running){
                System.out.println("ERROR: Not connected to server");

                logger.warn("Put request when not connected to server");
                return "ERROR: Not connected to server";
            }
            store.disconnect();
            setRunning(false);
            System.out.println("Disconnected from server");
            logger.info("Disconnected from server");
            return "Disconnected from server";
        } else if(tokens[0].equals("put")) {
            if(!running){
                System.out.println("ERROR: Not connected to server");

                logger.warn("Put request when not connected to server");
                return "ERROR: Not connected to server";
            } else {
                if (tokens.length >= 3 && !(tokens.length==3 && tokens[2].equals("null"))) {
                    String inputV = tokens[2];
                    for (int i = 3; i < tokens.length; i++) {
                        inputV = inputV + " "+tokens[i];
                    }
                    KVMessage.StatusType status = store.put(tokens[1], inputV).getStatus();
                    if (status == PUT_ERROR) {
                        System.out.println("PUT ERROR");
                        return "PUT ERROR";
                    } else if (status == PUT_UPDATE) {
                        System.out.println("PUT UPDATE");
                        return "PUT UPDATE";
                    } else if (status == SERVER_STOPPED) {
                        System.out.println("SERVER_STOPPED");
                        return "SERVER_STOPPED";
                    } else if (status == SERVER_WRITE_LOCK) {
                        System.out.println("SERVER_WRITE_LOCK");
                        return "SERVER_WRITE_LOCK";
                    } else {
                        System.out.println("PUT SUCCESS");
                        return "PUT SUCCESS";
                    }
                } else if (tokens.length == 2 || (tokens.length == 3 && tokens[2].equals("null"))) {
                    KVMessage.StatusType status = store.put(tokens[1], null).getStatus();
                    if (status == DELETE_ERROR) {
                        System.out.println("DELETE ERROR");
                        return "DELETE ERROR";
                    }  else if (status == SERVER_STOPPED) {
                        System.out.println("SERVER_STOPPED");
                        return "SERVER_STOPPED";
                    } else if (status == SERVER_WRITE_LOCK) {
                        System.out.println("SERVER_WRITE_LOCK");
                        return "SERVER_WRITE_LOCK";
                    } else {
                        System.out.println("DELETE SUCCESS");
                        return "DELETE SUCCESS";
                    }
                } else {
                    printError("Invalid number of parameters!");
                    return "Invalid number of parameters!";
                }

            }
        } else if(tokens[0].equals("get")) {
            if(!running) {
                System.out.println("ERROR: Not connected to server");

                logger.warn("Put request when not connected to server");
                return "ERROR: Not connected to server";
            } else{
                if (tokens.length == 2) {
                    KVMsg msg = store.get(tokens[1]);
                    if (msg.getStatus() == SERVER_STOPPED) {
                        System.out.println("SERVER_STOPPED");
                        return "SERVER_STOPPED";
                    } else if (msg.getValue() == null) {
                        System.out.println("GET ERROR - no corresponding value");
                        return "GET ERROR - no corresponding value";
                    } else {
                        System.out.println(msg.getValue());
                        return msg.getValue();
                    }


                } else {
                    printError("Invalid number of parameters!");
                    return "Invalid number of parameters!";
                }
            }
        } else if(tokens[0].equals("logLevel")) {
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
    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }
    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");

    }
    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KEY-VALUE CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t\t \nInserts a key-value pair into the storage server data structures. \n" +
                "Updates (overwrites) the current value with the given value if the server already contains the specified key. \n" +
                "Deletes the entry for the given key if <value> equals null. \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t Retrieves the value for the given key from the storage server. \n");
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }


    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean run) {
        running = run;
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logclient/ecs.log", Level.ALL);

            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
