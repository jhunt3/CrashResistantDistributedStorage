package ecs;

public class ECSNode implements IECSNode{
    private String name;
    private String host;
    private int port;
    private String[] range;

    public ECSNode(String nodeName, String nodeHost, int nodePort, String[] nodeRange){
        name=nodeName;
        host=nodeHost;
        port=nodePort;
        range=nodeRange;

    }
    public String getNodeName(){
        return name;
    }

    public String getNodeHost(){
        return host;
    }

    public int getNodePort(){
        return port;
    }

    public String[] getNodeHashRange(){
        return range;
    }
}