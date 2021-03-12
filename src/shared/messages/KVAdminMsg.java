package shared.messages;


//import jdk.jshell.Snippet;

import java.io.Serializable;
import java.util.HashMap;

public class KVAdminMsg implements KVMessage, Serializable {
    private static final long serialVersionUID = 8006348832096871261L;
    private final HashMap<String, String> metadata;
    private final StatusType status;
    private final String range;
    private final String newKvServer;

    public KVAdminMsg(String kvServer, StatusType status, HashMap<String, String> metadata, String range){
        this.metadata = metadata;
        this.status = status;
        this.range = range;
        this.newKvServer = kvServer;
    }

    public HashMap<String, String> getMetadata(){
        return this.metadata;
    }

    @Override
    public boolean isAdminMessage() {
        return true;
    }

    public StatusType getStatus(){
        return this.status;
    }

    public String getRange(){ return this.range; }

    public String getNewKvServer(){ return this.newKvServer; }
}
