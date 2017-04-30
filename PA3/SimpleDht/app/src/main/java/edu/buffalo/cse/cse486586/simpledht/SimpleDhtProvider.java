package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static android.content.ContentValues.TAG;
import static android.content.Context.MODE_PRIVATE;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    static int sequence = 0;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String OPERATION_DELETE = "delete";
    static final String OPERATION_INSERT = "insert";
    static final String OPERATION_JOIN2 = "OPERATION_JOIN2";
    static final String OPERATION_JOIN3 = "OPERATION_JOIN3";

    static final String OPERATION_QUERY = "query";
    static final String OPERATION_QUERY_GA = "query_global";
    static final String OPERATION_QUERY_LA = "query_local";
    static final String OPERATION_JOIN = "join";
    static final String OPERATION_HASHCHECK = "OPERATION_HASHCHECK";
    static boolean isSocketOpen = false;
    CopyOnWriteArrayList<String> ports = new CopyOnWriteArrayList<String>();
    static final String FIRST_PORT = "5554";
    Map<String, Node> map = new ConcurrentHashMap<String, Node>();
    HashMap<String, String> hashValues = new LinkedHashMap<String, String>();
    static String portStr  = null;
    static String value = null;
    Node result = null;

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        hashValues = sortByValue(hashValues);
        Log.v("insert", values.toString());
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        Set<String> ports = hashValues.keySet();
        Node node = map.get(portStr);
        Operation op = new Operation(OPERATION_INSERT, key, value);
        node.setOperation(op);
        try{
            String hashKey = genHash(key);
            node.putObj(key, value);
            String finalPort = null;
            String firstPort = ports.iterator().next();;
            for(String port: ports){
//                System.out.println("hashvalue comparision of key: " +key + " with port :" +port +" and the value is : " +hashKey.compareTo(hashValues.get(port)));
                if(hashKey.compareTo(hashValues.get(port)) < 0){
                    finalPort = port;
                    break;
                }
            }
            if(finalPort == null) {
                finalPort = firstPort;
            }
//            System.out.println("The key used for insert is " +key+ " and the value  is " +value+ " and the port  is " +finalPort);
            new ClientTask(node, values, finalPort).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            Thread.sleep(300);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return uri;
    }

    //  source - http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
    public LinkedHashMap<String, String> sortByValue(
            HashMap<String, String> map) {
        LinkedHashMap<String, String> result =
                new LinkedHashMap<String,String>();
        List<String> keys = new ArrayList<String>(map.keySet());
        List<String> values = new ArrayList<String>(map.values());
        Collections.sort(keys);
        Collections.sort(values);
        for(String val: values){
            for(String key: keys){
                String c1 = map.get(key);
                String c2 = val;
                if (c1.equals(c2)) {
                    keys.remove(key);
                    result.put(key, val);
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public boolean onCreate() {
        // Source - PA2A
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        ports.add(myPort);
        try {
            Node node = new Node(portStr);
            node.setPredecessor(myPort);
            node.setSuccessor(myPort);
            node.setPortNum(portStr);
            Operation op = new Operation(OPERATION_JOIN);
            node.setOperation(op);
            map.put(portStr, node);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask(node, REMOTE_PORT0).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Error in onCreate method");
            e.printStackTrace();
        }
        return false;
    }

    public synchronized MatrixCursor queryAL(){
        String[] req = new String[2];
        req[0] = "key";
        req[1] = "value";
        MatrixCursor matrixCursor = new MatrixCursor(req);
        try {
            File path = getContext().getFilesDir();
            for (File file : path.listFiles()) {
                String key = file.getName();
                FileInputStream input = getContext().openFileInput(key);
                InputStreamReader inputReader = new InputStreamReader(input);
                BufferedReader reader = new BufferedReader(inputReader);
                String value = reader.readLine();
                String[] in = new String[2];
                in[0] = key;
                in[1] = value;
                matrixCursor.addRow(in);
            }
        }   catch (IOException e) {
            e.printStackTrace();
        }
        return matrixCursor;
    }

    public synchronized MatrixCursor queryAG(){
        String[] req = new String[2];
        req[0] = "key";
        req[1] = "value";
        MatrixCursor matrixCursor = new MatrixCursor(req);
        Set<String> ports = hashValues.keySet();
        Operation operation = new Operation(OPERATION_QUERY_GA);

        try {
            for(String port : ports){
                if(result == null){
                    result = new Node(hashValues.get(portStr));
                }
                result.setOperation(operation);
                new ClientTask(result , Integer.toString(Integer.parseInt(port) * 2)).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                Thread.sleep(1000);
            }
            List<Object> maps = result.getObjects();
            for(int i =0; i < maps.size() ; i++){
                Map<String, String> curMap = (ConcurrentHashMap) maps.get(i);
                Set<String> keys = curMap.keySet();
                System.out.println("Keyset in queryAG : " +keys);
                for(String key : keys){
                    String[] input = new String[2];
                    input[0] = key;
                    input[1] = curMap.get(key);
                    matrixCursor.addRow(input);
                }
            }
        }   catch (Exception e) {
            e.printStackTrace();
        }
        return matrixCursor;
    }

    private int deleteAG(){
        return 0;
    }

    private int deleteAL(){
        return 0;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("*")) {
            return deleteAG();
        }
        if(selection.equals("@")){
            return deleteAL();
        }
        try{
            getContext().deleteFile(selection);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        FileInputStream input = null;
        if(selection.equals("*")) {
            return queryAG();
        }
        if(selection.equals("@")){
            return queryAL();
        }

        try {
//            source - PA2A documentation
            String hashKey = genHash(selection);
            Set<String> ports = hashValues.keySet();
            String finalPort = null;
            String firstPort = ports.iterator().next();;
            for(String port: ports){
                if(hashKey.compareTo(hashValues.get(port)) < 0){
                    finalPort = port;
                    break;
                }
            }
            if(finalPort == null) {
                finalPort = firstPort;
            }
            Node node = new Node(portStr);
            Operation op = new Operation(OPERATION_QUERY, selection);
            node.setOperation(op);
            new ClientTask(node , Integer.toString(Integer.parseInt(finalPort) * 2)).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            Thread.sleep(500);
            String[] req = new String[2];
            req[0] = "key";
            req[1] = "value";
            MatrixCursor matrixCursor = new MatrixCursor(req);
            String[] in = new String[2];
            in[0] = selection;
            in[1] = value;
            System.out.println("The key used for query is " +selection + " and the value retrieved is " +value);
            matrixCursor.addRow(in);
            return matrixCursor;

        }  catch(NoSuchAlgorithmException e){
            Log.e(SimpleDhtProvider.class.getName(), "Query failed - Hash error");
            e.printStackTrace();
        }   catch(Exception e){
            Log.e(SimpleDhtProvider.class.getName(), "Query failed");
            e.printStackTrace();
        }
        Log.v("query", selection);
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    //      source - PA1
    //      source - PA2A documentation
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

            @Override
            protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            System.out.println("###### In server method");
            String key_delete = null;
            Node predecessor_node = (Node) map.get(FIRST_PORT);
            Node successor_node = null;
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    Node node = (Node) in.readObject();
//                    System.out.println("###### We are here");
                    Operation op = node.getOperation();
//                    System.out.println("##### node.getPortNum() :" +node.getKey());
//                    System.out.println("##### first port :" +FIRST_PORT);
                    String operation = op.getOperation();
                    if(operation.equals(OPERATION_HASHCHECK)){
                        hashValues = node.getHashValues();
                    }
                    if(operation.equals(OPERATION_JOIN)) {
                        hashValues.put(node.getPortNum(), genHash(node.getPortNum()));
                        Set<String> keySet = hashValues.keySet();
                        for(String port: keySet){
//                            System.out.println("### port num :" +port);
                            callClient(node, port, hashValues);
                        }
                    }
                    if(operation.equals(OPERATION_DELETE)){
                        key_delete = node.getOperation().getOp_key();
                        delete(key_delete, node);
                    }
                    if(operation.equals(OPERATION_INSERT)){
                        Map<String, String> pair = node.getObj();
                        String key = pair.keySet().iterator().next();
                        String value = pair.get(key);
                        System.out.println("key inserted is " +key + " value inserted is " +value);
//                        System.out.println("##### insert runner : value " +value);
                        FileOutputStream output = getContext().openFileOutput(key, MODE_PRIVATE);
                        output.write(value.getBytes());
                        output.flush();
                        output.close();
                    }
                    if(operation.equals(OPERATION_QUERY)){
                        FileInputStream input = getContext().openFileInput(op.getOp_key());
                        InputStreamReader inputReader = new InputStreamReader(input);
                        BufferedReader reader = new BufferedReader(inputReader);
                        String value = reader.readLine();
                        out.write(value + "\n");
                        out.flush();
                        out.close();
                        socket.close();
                    }
                    if(operation.equals(OPERATION_QUERY_GA)){
                        Map<String, String> ag = new ConcurrentHashMap<String, String>();
                        try {
                            File path = getContext().getFilesDir();
                            for (File file : path.listFiles()) {
                                String key = file.getName();
                                FileInputStream input = getContext().openFileInput(key);
                                InputStreamReader inputReader = new InputStreamReader(input);
                                BufferedReader reader = new BufferedReader(inputReader);
                                String value = reader.readLine();
                                ag.put(key, value);
                            }
                            node.addObject(ag);
                            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                            output.writeObject(node);
                            output.flush();
                            out.close();
                            socket.close();

                        }   catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                    out.write("OK");
                    out.flush();
                    out.close();
                    socket.close();
                }


            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }

        private void callClient(Node node, String port, Map<String, String> hash) {

            new ClientTask(node, port, hash).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }

        private void delete(String key_delete, Node node){
            Map<String, String> map = (HashMap<String, String>)node.getMap();
            if(map.containsKey(key_delete)){
                map.remove(key_delete);
            }
        }

    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    // source - PA2A documentation
    // source - PA1
    private class ClientTask extends AsyncTask<String, Void, Void> {

        private boolean join = false;
        private boolean insert = false;
        private boolean query = false;
        private boolean query_ga = false;
        private Node node;
        private String port;
        Map<String, String> hash;
        ContentValues values;

        public ClientTask(Node node, String port){
            this.node = node;
            this.port = port;
            if(node.getOperation() != null && node.getOperation().getOperation() != null
                    && node.getOperation().getOperation().equals(OPERATION_QUERY)){
                query = true;
            }
            else if(node.getOperation() != null && node.getOperation().getOperation() != null
                    && node.getOperation().getOperation().equals(OPERATION_QUERY_GA)){
                query_ga = true;
            }
        }

        public ClientTask(Node node, ContentValues values, String port){
            this.node = node;
            this.values = values;
            this.port = Integer.toString(Integer.parseInt(port) * 2);
            insert = true;
        }

        public ClientTask(Node node, String port, Map<String, String> hash){
            this.node = node;
            this.port = Integer.toString(Integer.parseInt(port) * 2);
            this.hash = new HashMap<String, String>(hash);
            join = true;
        }

        @Override
        protected Void doInBackground(String... msgs) {
            try {
//                System.out.println("reached client");
                String msgReceived = null;
                if(join){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(port)));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    Operation op = new Operation(OPERATION_HASHCHECK);
                    node.setOperation(op);
                    node.addHashValueMap(hash);
                    out.writeObject(node);
                    out.flush();
                    while (true) {
                        InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStream);
                        msgReceived = bufferedReader.readLine();
//                        System.out.println("msgReceived :" +msgReceived);
                        if (msgReceived != null && !msgReceived.isEmpty()) {
                            value = msgReceived;
                            bufferedReader.close();
                            inputStream.close();
                            out.close();
                            socket.close();
                            break;
                        }
                    }
                    join = false;
                }
                else if(insert){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(port)));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    System.out.println("The key value pair " +node.getObj()+ " is going to port " +port);
                    out.writeObject(node);
                    out.flush();
                    while (true) {
                        InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStream);
                        msgReceived = bufferedReader.readLine();
//                        System.out.println("msgReceived :" +msgReceived);
                        if (msgReceived != null && !msgReceived.isEmpty()) {
                            value = msgReceived;
                            bufferedReader.close();
                            inputStream.close();
                            out.close();
                            socket.close();
                            break;
                        }
                    }
                    insert = false;
                }
                else if(query){
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (Integer.parseInt(port)));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(node);
                        out.flush();
                        while (true) {
                            InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                            BufferedReader bufferedReader = new BufferedReader(inputStream);
                            msgReceived = bufferedReader.readLine();
                            if (msgReceived != null && !msgReceived.isEmpty()) {
                                value = msgReceived;
                                bufferedReader.close();
                                inputStream.close();
                                out.close();
                                socket.close();
                                break;
                            }
                        }
                        query = false;
                    }
                }
                else if(query_ga){
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (Integer.parseInt(port)));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(node);
                        out.flush();
                        while (true) {
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            Node node = (Node) in.readObject();
                            if (node != null) {
                                result = node;
                                in.close();
                                out.close();
                                socket.close();
                                break;
                            }
                        }
                        query = false;
                    }
                }
                else {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(port)));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(node);
//                    System.out.println("### message flushed from client");
                    out.flush();
                    while (true) {
                        InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStream);
                        msgReceived = bufferedReader.readLine();
//                        System.out.println("msgReceived :" + msgReceived);
                        if (msgReceived != null && !msgReceived.isEmpty()) {
                            value = msgReceived;
                            bufferedReader.close();
                            inputStream.close();
                            out.close();
                            socket.close();
                            break;
                        }
                    }
                }
                return null;
            }  catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                callClient();
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }

        private void callClient(){
            new ClientTask(node, node.getSuccessor()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
    }
}
