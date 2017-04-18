package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by vipin on 4/10/17.
 */

public class RingNode implements Comparable<RingNode> {
    private String myKeyHash;
    private String mySuccKeyHash;
    private String myPredKeyHash;

    private int myPort;
    private int mySuccPort;
    private int myPredPort;

    public RingNode() {

    }

    public RingNode(int myPort, int succPort, int predPort, String myKeyHash, String mySuccKeyHash, String myPredKeyHash) {
        this.myPort = myPort;
        this.mySuccPort = succPort;
        this.myPredPort = predPort;
        this.myKeyHash = myKeyHash;
        this.mySuccKeyHash = mySuccKeyHash;
        this.myPredKeyHash = myPredKeyHash;
    }

    public String getMyKeyHash() {
        return myKeyHash;
    }

    public void setMyKeyHash(String myKeyHash) {
        this.myKeyHash = myKeyHash;
    }

    public String getMySuccKeyHash() {
        return mySuccKeyHash;
    }

    public void setMySuccKeyHash(String mySuccKeyHash) {
        this.mySuccKeyHash = mySuccKeyHash;
    }

    public String getMyPredKeyHash() {
        return myPredKeyHash;
    }

    public void setMyPredKeyHash(String myPredKeyHash) {
        this.myPredKeyHash = myPredKeyHash;
    }

    public int getMyPort() {
        return myPort;
    }

    public void setMyPort(int myPort) {
        this.myPort = myPort;
    }

    public int getSuccPort() {
        return mySuccPort;
    }

    public void setSuccPort(int succPort) {
        this.mySuccPort = succPort;
    }

    public int getPredPort() {
        return myPredPort;
    }

    public void setPredPort(int predPort) {
        this.myPredPort = predPort;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RingNode)) {
            return false;
        }

        RingNode secondNode = (RingNode) object;

        if (this.myKeyHash.equals(secondNode.getMyKeyHash())) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        String str = myKeyHash + "\n" + mySuccKeyHash + "\n" + myPredKeyHash + "\n" + myPort + "\n" + mySuccPort
                + "\n" + myPredPort;
        return str;
    }

    @Override
    public int compareTo(RingNode obj) {
        return this.myKeyHash.compareTo(obj.getMyKeyHash());
    }

}
