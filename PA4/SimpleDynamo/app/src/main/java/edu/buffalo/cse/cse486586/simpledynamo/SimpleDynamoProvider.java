package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static android.content.Context.MODE_PRIVATE;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static final String OPERATION_INSERT = "insert";
	static final String OPERATION_QUERY = "query";
	static final String OPERATION_QUERY_GA = "query_global";
    static final String OPERATION_DAMAGECONTROL = "OPERATION_DAMAGECONTROL";
	HashMap<String, String> hashValues = new LinkedHashMap<String, String>();
	static String portStr  = null;
    ArrayList<String> missedDataList;
    Map<String, ArrayList<String>> missedDataMap = new HashMap<String, ArrayList<String>>();
    String current_port = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try{
            getContext().deleteFile(selection);

        } catch (Exception e) {
//            e.printStackTrace();
        }
        return 0;
    }


    @Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public Uri insert(Uri uri, ContentValues values) {
//                Log.v("insert", values.toString());
        String key = values.get("key").toString();
        if(key.equalsIgnoreCase("other_medium")){
            try{
                String value = values.get("value").toString();
                String[] arr = value.split(" ");
                FileOutputStream output = getContext().openFileOutput(arr[0], MODE_PRIVATE);
                output.write(arr[1].getBytes());
                Log.v("###inserting:", arr[0]+ " " +arr[1]);
                output.flush();
                output.close();
            } catch (Exception e){
//                e.printStackTrace();
            }
        }
        else {
            Set<String> ports = hashValues.keySet();
            try {
                String hashKey = genHash(key);
                String finalPort = null;
                String firstPort = ports.iterator().next();
                for (String port : ports) {
                    if (hashKey.compareTo(hashValues.get(port)) < 0) {
                        finalPort = port;
                        break;
                    }
                }
                if (finalPort == null) {
                    finalPort = firstPort;
                }
                List<String> portList = getPortsForReplication(finalPort);
//                System.out.println("#### The key value " + values + " are sent to the ports" + portList);
                new ClientTask(values, portList).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            } catch (Exception e) {
//                e.printStackTrace();
            }
        }
        return uri;
        }

    private List<String> getPortsForReplication(String port){
        List<String> list = new ArrayList<String>();
        if(port.compareTo("5562") == 0){
            list.add("5562");
            list.add("5556");
            list.add("5554");
        }
        else if(port.compareTo("5556") == 0){
            list.add("5556");
            list.add("5554");
            list.add("5558");
        }
        else if(port.compareTo("5554") == 0){
            list.add("5554");
            list.add("5558");
            list.add("5560");
        }
        else if(port.compareTo("5558") == 0){
            list.add("5558");
            list.add("5560");
            list.add("5562");
        }
        else if(port.compareTo("5560") == 0){
            list.add("5560");
            list.add("5562");
            list.add("5556");
        }
        return list;
    }

    private List<String> getPortsForMissingData(){
        List<String> list = new ArrayList<String>();
        list.add("5554");
        list.add("5556");
        list.add("5558");
        list.add("5560");
        list.add("5562");
        return list;
    }

    @Override
	public boolean onCreate() {
        // Source - PA2A
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            current_port = myPort;
            hashValues.put("5562", genHash("5562"));
            hashValues.put("5556", genHash("5556"));
            hashValues.put("5554", genHash("5554"));
            hashValues.put("5558", genHash("5558"));
            hashValues.put("5560", genHash("5560"));
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            damageControl();
        }catch (IOException e) {
//            Log.e(TAG, "Can't create a ServerSocket");
//            e.printStackTrace();
        } catch (Exception e) {
//            Log.e(TAG, "Error in onCreate method");
//            e.printStackTrace();
        }
        return false;
    }

    private void damageControl(){
            List<String> ports = getPortsForMissingData();
                try {
                    new ClientTask(ports).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                }  catch (Exception e) {
//                    e.printStackTrace();
                }
        }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
//            System.out.println("The selection used for query is " + selection);
            if (selection.equals("*")) {
                return queryAG();
            }
            if (selection.equals("@")) {
                return queryAL();
            }

            try {
//            source - PA2A documentation
                String hashKey = genHash(selection);
                Set<String> ports = hashValues.keySet();
                String finalPort = null;
                String firstPort = ports.iterator().next();
                for (String port : ports) {
                    if (hashKey.compareTo(hashValues.get(port)) < 0) {
                        finalPort = port;
                        break;
                    }
                }
                if (finalPort == null) {
                    finalPort = firstPort;
                }

                String data = OPERATION_QUERY + " " + selection+ " " + portStr;
                List<String> portsList = getPortsForReplication(finalPort);
                String value = (String)new ClientTask(data, portsList).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
                String[] req = new String[2];
                req[0] = "key";
                req[1] = "value";
                Log.v("###query result:", selection + " " + value);
                MatrixCursor matrixCursor = new MatrixCursor(req);
                String[] in = new String[2];
                in[0] = selection;
                in[1] = value;
                matrixCursor.addRow(in);
                return matrixCursor;

            } catch (NoSuchAlgorithmException e) {
//                Log.e(SimpleDynamoProvider.class.getName(), "Query failed - Hash error");
//                e.printStackTrace();
            } catch (Exception e) {
//                Log.e(SimpleDynamoProvider.class.getName(), "Query failed");
//                e.printStackTrace();
            }
//            Log.v("query", selection);
            return null;
        }

    @Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

    public synchronized MatrixCursor queryAL(){
        String[] req = new String[2];
        req[0] = "key";
        req[1] = "value";
        MatrixCursor matrixCursor = new MatrixCursor(req);
        try {
            File path = getContext().getFilesDir();
            for (File file : path.listFiles()) {
                String key = file.getName();
//                System.out.println("The key found in query @ is " +key);
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
//            e.printStackTrace();
        }
        return matrixCursor;
    }

    public synchronized MatrixCursor queryAG(){
        String[] req = new String[2];
        req[0] = "key";
        req[1] = "value";
        MatrixCursor matrixCursor = new MatrixCursor(req);
        List<ConcurrentHashMap<String,String>> maps = null;
        try {
            String query_data = OPERATION_QUERY_GA;
            maps = (ArrayList<ConcurrentHashMap<String,String>>) new ClientTask(query_data).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
            for(int i =0; i < maps.size() ; i++){
                Map<String, String> curMap = (ConcurrentHashMap) maps.get(i);
                Set<String> keys = curMap.keySet();
//                System.out.println("Keyset in queryAG : " +keys);
                for(String key : keys){
                    String[] input = new String[2];
                    input[0] = key;
                    input[1] = curMap.get(key);
                    matrixCursor.addRow(input);
                }
            }
        }   catch (Exception e) {
//            e.printStackTrace();
        }
        return matrixCursor;
    }

    //      source - PA1
    //      source - PA2A documentation
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStream);
                    String data = bufferedReader.readLine();
//                    bufferedReader.close();
//                    inputStream.close();
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
//                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
//                    Node node = (Node) in.readObject();
//                    Operation op = node.getOperation();
                    String[] values = data.split(" ");
                    String operation = values[0];
//                    String port = node.getPortNum();
                    if(operation.equals(OPERATION_DAMAGECONTROL)) {
//                        System.out.println("### The datamap retrieved from port " +current_port+ " is " +missedDataMap);
                        ArrayList<String> dataList = missedDataMap.get(values[1]);
//                        System.out.println("### The datalist retrieved from port " +current_port + " is " +dataList);
                        if(dataList != null && dataList.size() > 0) {
                            StringBuilder hello = new StringBuilder();
                            for(String keyValue: dataList){
                                hello.append(keyValue);
                                hello.append("#");
                            }
                            out.write(hello + "\n");
                        }
                        else{
                            out.write("OK" + "\n");
                        }
                        out.flush();
                        out.close();
                        missedDataMap.put(values[1], null);
                        socket.close();
                    }
                    if(operation.equals(OPERATION_INSERT)){
                        ContentValues content = new ContentValues();
                        content.put("key", "other_medium");
                        content.put("value", values[1] + " " +values[2]);
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
                        insert(uriBuilder.build(), content);
                        out.write("OK" + " \n");
                        out.flush();
                        out.close();
                        socket.close();
                    }
                    if(operation.equals(OPERATION_QUERY)){
                        Log.v("###querying in server:", values[1]);
                        FileInputStream input = getContext().openFileInput(values[1]);
                        InputStreamReader inputReader = new InputStreamReader(input);
                        BufferedReader reader = new BufferedReader(inputReader);
                        String value = reader.readLine();
//                        System.out.println("result retrieved " + value);
                        out.write(value + "\n");
                        out.flush();
                        out.close();
                        socket.close();
                    }
                    if(operation.equals(OPERATION_QUERY_GA)){
                        ConcurrentHashMap<String, String> ag = new ConcurrentHashMap<String, String>();
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
                            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                            output.writeObject(ag);
                            output.flush();
                            out.close();
                            socket.close();

                        }   catch (IOException e) {
//                            e.printStackTrace();
                        }

                    }
                    out.write("OK" + "\n");
                    out.flush();
                    out.close();
                    socket.close();
                }
            } catch (IOException e) {
//                e.printStackTrace();
            } catch (Exception e) {
//                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Object> {
        private boolean insert = false;
        private boolean query = false;
        private boolean query_ga = false;
        private boolean damage_control = false;
        ContentValues values1;
        List<String> portList;
        List<String> allPorts;
        String[] query_stuff;
        String query_data;
        String query_ga_data;

        ClientTask(String query_data, List<String> portList){
            this.query_data = query_data;
            query_stuff = query_data.split(" ");
            query = true;
            this.portList = portList;
        }

        ClientTask(String query_ga_data){
            query_ga = true;
            this.query_ga_data = query_ga_data;
        }

        ClientTask(List<String> allPorts){
            this.allPorts = allPorts;
            this.damage_control = true;
        }

        ClientTask(ContentValues values, List<String> portList){
            this.values1 = values;
            this.portList = portList;
            insert = true;
        }

        @Override
        protected Object doInBackground(String... msgs) {
            try {
                if(damage_control){
                    String value = null;
                    List<String> total = new ArrayList<String>();
                    for(String port1: allPorts) {
                        try {
                            port1 = Integer.toString(Integer.parseInt(port1) * 2);
//                            System.out.println("Port tried is " +port1);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(port1)));
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            out.write(OPERATION_DAMAGECONTROL + " " + current_port + "\n");
                            out.flush();
                            InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                            BufferedReader bufferedReader = new BufferedReader(inputStream);
                            value = bufferedReader.readLine().toString();
                            if(value != null && value.length()>0 && !value.equalsIgnoreCase("OK")) {
                                total.add(value);
                            }
                            bufferedReader.close();
                            inputStream.close();
                            out.close();
                            socket.close();
                        } catch (Exception e) {
//                            System.out.println("Exception caught");
                        }
                    }
                    if(total != null && total.size() > 0) {
                        for (String data : total) {
                            String[] keyValueList = data.split("#");
                            for (String keyValue : keyValueList) {
                                String[] arr = keyValue.split(" ");
                                ContentValues content = new ContentValues();
                                content.put("key", "other_medium");
                                content.put("value", arr[0] + " " + arr[1]);
                                Uri.Builder uriBuilder = new Uri.Builder();
                                uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
                                insert(uriBuilder.build(), content);
                            }
                        }
                    }
//                    System.out.println("total list retrieved from damage control block is " +total);
                    damage_control = false;
                    return total;
                }
                else if(insert){
                        String key1 = values1.get("key").toString();
                        String value1 = values1.get("value").toString();
                        String value = null;
                        for (String port : portList) {
                            String port1 = Integer.toString(Integer.parseInt(port) * 2);
                            try {
//                                System.out.println("calling the port " + port1);
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(port1)));
                                PrintWriter out = new PrintWriter(socket.getOutputStream());
                                out.write(OPERATION_INSERT +" "+ key1 +" "+ value1 + "\n");
                                out.flush();
                                InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                                BufferedReader bufferedReader = new BufferedReader(inputStream);
                                value = bufferedReader.readLine().toString();
                                bufferedReader.close();
                                inputStream.close();
                                out.close();
                                socket.close();
                            } catch (Exception exception) {
                                exception.printStackTrace();
//                                System.out.println("data added to missedDataList for port " + port1 + " is " + key1 + ", " + value1);
                                if (missedDataMap.get(port1) != null) {
                                    missedDataList = missedDataMap.get(port1);
                                    missedDataList.add(key1 + " " + value1);
                                } else {
                                    missedDataList = new ArrayList<String>();
                                    missedDataList.add(key1 + " " + value1);
                                }
                                missedDataMap.put(port1, missedDataList);
//                                System.out.println("missedDataMap: " + missedDataMap);
                            }
                        }
                        insert = false;
                }
                else if(query){
                        List<String> values = new ArrayList<String>();
                        String final_value = new String();
                        String non_null_port = null;
                        for(String port: portList) {
                            String value = null;
                            port = Integer.toString(Integer.parseInt(port) * 2);
                            if(port.compareTo(current_port) == 0){
                                Log.v("###querying in client:", query_stuff[1]);
                                FileInputStream input = getContext().openFileInput(query_stuff[1]);
                                InputStreamReader inputReader = new InputStreamReader(input);
                                BufferedReader reader = new BufferedReader(inputReader);
                                value = reader.readLine();
                                if(value != null){
                                    values.add(value);
                                    if(non_null_port != null){
                                        non_null_port = port;
                                    }
                                }
                                continue;
                            }
                            Log.v("###querying in client :", query_stuff[1]+" "+port);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(port)));
//                            System.out.println("port used for query " + port + " and selection is " + query_stuff[1]);
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            out.write(query_data + "\n");
                            out.flush();
                            InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                            BufferedReader bufferedReader = new BufferedReader(inputStream);
                            value = bufferedReader.readLine();
                            values.add(value);
                            if(value != null){
                                final_value = value;
                                if(non_null_port != null){
                                    non_null_port = port;
                                }
                            }
                            bufferedReader.close();
                            inputStream.close();
                            out.close();
                            socket.close();
                        }
                        int a = 0;
                        for(String val : values){
                            if(Collections.frequency(values, val) > a){
                                a = Collections.frequency(values, val);
                                if(val != null){
                                final_value = val;
                                }
                            }
                        }
                        query = false;
//                        System.out.println("result retrieved in client " + final_value);
                        return final_value;
                }
                else if(query_ga) {
                    ArrayList<ConcurrentHashMap<String, String>> result = new ArrayList<ConcurrentHashMap<String, String>>();
                    for (String port : hashValues.keySet()) {
                        try {
                            port = Integer.toString(Integer.parseInt(port) * 2);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(port)));
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            out.write(query_ga_data + "\n");
                            out.flush();
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) in.readObject();
//                            System.out.println("data for query global in client is" + map);
                            result.add(map);
                            in.close();
                            out.close();
                            socket.close();
                        }catch(Exception e){
//                            e.printStackTrace();
                            continue;
                        }
                    }
                    query = false;
                    return result;
                }
            }  catch (UnknownHostException e) {
//                Log.e(TAG, "ClientTask UnknownHostException");
            }  catch (IOException e) {
//                e.printStackTrace();
//                Log.e(TAG, "ClientTask socket IOException");
            } catch(Exception e){
//                e.printStackTrace();
            }
            return null;
        }
    }
}