package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Akash Yeleswarapu on 4/7/2017.
 */

class Operation implements Serializable{
    private String operation;
    private String op_key;
    private String op_value;

    public Operation(String operation){
        this.operation = operation;
    }

    public Operation(String operation, String key){
        this.operation = operation;
        this.op_key = key;
    }

    public Operation(String operation, String key, String value){
        this.operation = operation;
        this.op_key = key;
        this.op_value = value;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getOp_key() {
        return op_key;
    }

    public void setOp_key(String op_key) {
        this.op_key = op_key;
    }

    public String getOp_value() {
        return op_value;
    }

    public void setOp_value(String op_value) {
        this.op_value = op_value;
    }
}

public class Node implements Serializable{
    private String node_id;
    private String portNum;
    private String key;
    private String predecessor;
    private String successor;
    private HashMap<String, String> map;
    private Operation operation;
    private ArrayList<String> ports;
    private Map<String, String> hashValues;
    private String joinOp;
    Map<String, String> obj;
    List<Object> objects;


    public Node(String key) throws NoSuchAlgorithmException{
        this.key = key;
        this.node_id = genHash(key);
        this.map = new HashMap<String, String>();
        this.objects = new ArrayList<Object>();
    }

    public String getJoinOp() {
        return joinOp;
    }

    public void setJoinOp(String joinOp) {
        this.joinOp = joinOp;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public void addObj(String key, String value){
        if(!map.containsKey(key)){
            System.out.println("####Key, value, port number added in the node are :" +key + "," +value + " ," +portNum);
            map.put(key, value);
        }
    }
    public void addObject(Object object){
        objects.add(object);
    }
    public List<Object> getObjects() {
        return objects;
    }


    public synchronized void putObj(String key, String value){
        obj = new ConcurrentHashMap<String, String>();
        obj.put(key, value);
    }

    public synchronized Map<String, String> getObj(){
        return obj;
    }
    public void addPortsList(ArrayList<String> ports){
        this.ports = new ArrayList<String>(ports);
    }

    public ArrayList<String> getPortsList(){
        return ports;
    }

    public HashMap<String, String> getHashValues(){
        return (HashMap<String, String>)hashValues;
    }

    public void addHashValueMap(Map<String, String> hashValues){
        this.hashValues = new HashMap<String, String>(hashValues);
    }


    public String getPortNum() {
        return portNum;
    }

    public void setPortNum(String portNum) {
        this.portNum = portNum;
    }

    public synchronized HashMap<String, String> getMap(){
            return map;
    }

    public String getNode_id() {
        return node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
