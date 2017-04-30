package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.buffalo.cse.cse486586.groupmessenger2.OnPTestClickListener;
import edu.buffalo.cse.cse486586.groupmessenger2.R;

import static android.R.id.message;


/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */


class Message implements Serializable {
    private int messageId;
    private String message;
    private boolean delivered = false;
    private String senderPort;
    private String receiverPort;
    private List<Integer> proposedPrioirities;
    private int agreedPriority;
    private List<String> ports;
    private int portLength;

    Message(){
        proposedPrioirities = new ArrayList<Integer>();
        ports = new ArrayList<String>();
    }

    Message(String message){
        this.message = message;
        proposedPrioirities = new ArrayList<Integer>();
        ports = new ArrayList<String>();
    }

    public synchronized String getSenderPort() {
        return senderPort;
    }

    public synchronized void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }

    public synchronized int portsLength(){
        return ports.size();
    }

    public synchronized String getReceiverPort() {
        return receiverPort;
    }

    public synchronized void setReceiverPort(String receiverPort) {
        this.receiverPort = receiverPort;
        ports.add(receiverPort);
    }

    public synchronized boolean isDelivered() {
        return delivered;
    }

    public synchronized void setDelivered(boolean delivered) {
        this.delivered = delivered;
    }

    public synchronized void addProposedPriority(int priority){
        proposedPrioirities.add(priority);
        Collections.sort(proposedPrioirities);
        if(proposedPrioirities != null){
            agreedPriority = proposedPrioirities.get(proposedPrioirities.size()-1);
        }
    }


    public synchronized List<Integer> getPriorities(){
        return proposedPrioirities;
    }

    public synchronized int getAgreedPriority(){
        return agreedPriority;
    }

    public synchronized int getMessageId() {
        return messageId;
    }

    public synchronized void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public synchronized String getMessage() {
        return message;
    }

    public synchronized void setMessage(String message) {
        this.message = message;
    }

    public synchronized String getSomePort(){
        String port = ports.get(portsLength()-1);
        ports.remove(port);
        return port;
    }
}

public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final int SERVER_PORT = 10000;
    static int sequence = 0;
    static int seqNum = 0;
    volatile int failedPort = 0;
    int currentPort = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
//      source - PA1
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

/*
        source - implementation technique learnt from the description and example provided in the following site
        http://stackoverflow.com/questions/23771902/how-to-implement-onclicklistener-on-android
*/
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    //      source - PA1
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            boolean run = true;
            Socket socket;
            while(run) {
                try {
                    while (true) {
                        socket = serverSocket.accept();
                        System.out.println("seqNum in server before sending to client: " + seqNum);
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        ObjectOutputStream out = null;
                        Message message = (Message) in.readObject();
                        int proposal = message.getAgreedPriority() + 1;
                        message.addProposedPriority(proposal);
                        System.out.println("seqNum in server after adding proposed priority: " + proposal);
                        out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(message);
                        out.flush();
                        InputStreamReader inputStream = new InputStreamReader(socket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStream);
                        String msgReceived = bufferedReader.readLine();
                        if (msgReceived != null) {
                            System.out.println("reached here in server");
                            publishProgress(message.getMessage());
                            storeMessages(message.getMessage());
                            bufferedReader.close();
                            inputStream.close();
                            socket.close();
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }

        //      source - PA2A documentation
        private void storeMessages(String... strings){
            ContentValues data = new ContentValues();
            String key = Integer.toString(sequence++);
            String value = strings[0].trim();
            data.put("key", key);
            data.put("value", value);
            /*source - uri building implementation learnt from description and example provided in the following link:
            http://stackoverflow.com/questions/19167954/use-uri-builder-in-android-or-create-url-with-variables
            */
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
            getBaseContext().getContentResolver().insert(uriBuilder.build(), data);
        }

        //        source - PA1
        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
//            String strReceived = "akash";
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     */

//      source - PA1
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            boolean run = true;
            int i = 0, j = 0;
            List<Socket> sockets = new ArrayList<Socket>();
            List<Integer> ports = new ArrayList<Integer>();
            int initial = 11108;
            for(int k = 0; k < 5; k++){
                ports.add(initial + 4*k);
            }
            while(run) {
                try {
                    System.out.println("$$$###ports :" +ports);
                    Socket socket = null;
                    Message messageObj = new Message();
                    Message message = null;
//                    List<ObjectOutputStream> streams = new ArrayList<ObjectOutputStream>();
                    messageObj.addProposedPriority(seqNum++);
                    while (i <= 4) {
                        currentPort = Integer.parseInt(REMOTE_PORT0) + 4 * i++;
                        if(failedPort == currentPort){
                            continue;
                        }
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (currentPort));
                        messageObj.setMessage(msgs[0]);
                        messageObj.setDelivered(false);
                        System.out.println("i value in client before sending to server: " + i);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(messageObj);
                        out.flush();
                        while(true) {
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            message = (Message) in.readObject();
                            if (message != null) {
                                System.out.println("socket size " + sockets.size());
                                sockets.add(socket);
//                                streams.add(out);
                            }
//                        in.close();
                            break;
//                        in.close();
                        }
                        seqNum = message.getAgreedPriority() + 1;
                        if (i > 4) {
                       /* System.out.println("sock size" +sockets.size());
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        message = (Message) in.readObject();
                        seqNum = message.getAgreedPriority() + 1 ;
                        System.out.println("sock size" +sockets.size());
                        System.out.println("message here :" +message.getMessage());*/
                            for (j = 0; j < sockets.size(); j++) {
                                /*if(failedPort == currentPort){
                                    break;
                                }*/
                                socket = sockets.get(j);
                                System.out.println("socket size in client while iterating through sockets: " + sockets.size());
                                PrintWriter outw = new PrintWriter(socket.getOutputStream());
                                outw.write(messageObj.getMessage());
//                            out.flush();
                                outw.flush();
//                                outw.close();
                                socket.close();
                            }
//                        in.close();
                        }

                    }

                    run = false;

//                    System.out.println("sockets size " + sockets.size());

                   /* for(int k = 0; k < sockets.size(); k++)
                        sockets.get(k).close();*/
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }catch (NullPointerException e) {
                    continue;
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                    e.printStackTrace();
                    if(failedPort == 0) {
                        failedPort = currentPort;
                        System.out.println("######$$$$$$$ :" + failedPort);
//                    ports.remove(failedPort);
                    }
                    continue;
                }
            }
            return null;
        }
    }
}