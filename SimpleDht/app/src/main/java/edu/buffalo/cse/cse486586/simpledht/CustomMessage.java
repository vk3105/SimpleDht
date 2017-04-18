package edu.buffalo.cse.cse486586.simpledht;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A class for handling message communications
 * Created by vipin on 4/9/17.
 */

public class CustomMessage {

    private int senderPort;
    private int predPort;
    private int succPort;
    private int toPort;
    private String messageType;

    private String predPortHashKey;
    private String succPortHashKey;
    private String myPortHashKey;
    private String key;
    private String value;

    private ConcurrentHashMap<String, String> keyValMap;

    public CustomMessage() {

    }

    public CustomMessage(String messageType, int senderPort, int toPort, int succPort, int predPort,
                         String myPortHashKey, String succPortHashKey, String predPortHashKey,
                         String key, String value, ConcurrentHashMap<String, String> keyValMap) {
        this.senderPort = senderPort;
        this.predPort = predPort;
        this.succPort = succPort;
        this.messageType = messageType;
        this.predPortHashKey = predPortHashKey;
        this.succPortHashKey = succPortHashKey;
        this.myPortHashKey = myPortHashKey;
        this.keyValMap = keyValMap;
        this.toPort = toPort;
        this.key = key;
        this.value = value;
    }

    private String getKeyValuePair() {
        String keyValPairs = "";
        for (String key : keyValMap.keySet()) {
            keyValPairs += (key + Constants.DATADELIM + keyValMap.get(key) + Constants.DATASEPDELIM);
        }
        keyValPairs = keyValPairs.substring(0, keyValPairs.lastIndexOf(Constants.DATASEPDELIM));
        return keyValPairs;
    }

    @Override
    public String toString() {
        String customMessageStr = "";

        customMessageStr = messageType + Constants.DELIM +
                senderPort + Constants.DELIM +
                toPort + Constants.DELIM +
                succPort + Constants.DELIM +
                predPort + Constants.DELIM +
                myPortHashKey + Constants.DELIM +
                succPortHashKey + Constants.DELIM +
                predPortHashKey + Constants.DELIM +
                key + Constants.DELIM + value;
        if (keyValMap != null && keyValMap.size() != 0) {
            customMessageStr += Constants.DELIM +
                    getKeyValuePair();
        } else {
            customMessageStr += Constants.DELIM + Constants.NULLVALUE;
        }

        return customMessageStr;
    }

    public int getSenderPort() {
        return senderPort;
    }

    public void setSenderPort(int senderPort) {
        this.senderPort = senderPort;
    }

    public int getPredPort() {
        return predPort;
    }

    public void setPredPort(int predPort) {
        this.predPort = predPort;
    }

    public int getSuccPort() {
        return succPort;
    }

    public void setSuccPort(int succPort) {
        this.succPort = succPort;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getPredPortHashKey() {
        return predPortHashKey;
    }

    public void setPredPortHashKey(String predPortHashKey) {
        this.predPortHashKey = predPortHashKey;
    }

    public String getSuccPortHashKey() {
        return succPortHashKey;
    }

    public void setSuccPortHashKey(String succPortHashKey) {
        this.succPortHashKey = succPortHashKey;
    }

    public String getMyPortHashKey() {
        return myPortHashKey;
    }

    public void setMyPortHashKey(String myPortHashKey) {
        this.myPortHashKey = myPortHashKey;
    }

    public ConcurrentHashMap<String, String> getKeyValMap() {
        return keyValMap;
    }

    public void setKeyValMap(ConcurrentHashMap<String, String> keyValMap) {
        this.keyValMap = keyValMap;
    }

    public int getToPort() {
        return toPort;
    }

    public void setToPort(int toPort) {
        this.toPort = toPort;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
