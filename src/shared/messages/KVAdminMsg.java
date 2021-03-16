package shared.messages;


//import jdk.jshell.Snippet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KVAdminMsg implements KVMessage, Serializable {
    private static final long serialVersionUID = 8006348832096871261L;
    private List<HashMap<String, String>> metadata = new ArrayList<HashMap<String, String>>();
    private List<String> hashList = new ArrayList<String>();
    private final StatusType status;
    private final String range;
    private final String newKvServer;

    public KVAdminMsg(String kvServer, StatusType status, List<HashMap<String,String>> metadata, List<String> hashList, String range){
        this.metadata = metadata;
        this.hashList = hashList;
        this.status = status;
        this.range = range;
        this.newKvServer = kvServer;
    }

    public List<HashMap<String, String>> getMetadata(){
        return this.metadata;
    }

    public List<String> getHashList(){ return this.hashList; }

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
