package shared.comm;

import shared.messages.KVMessage;
import shared.messages.KVMsg;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public interface ICommModule {

    public KVMessage receiveMsg() throws IOException, ClassNotFoundException, Exception;

    public void sendMsg(KVMessage.StatusType status, String key, String value, List<HashMap<String,String>> metadata) throws IOException;

    public KVMsg serve(KVMsg msg) throws Exception;

    public void closeConnection();
}
