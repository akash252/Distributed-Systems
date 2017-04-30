package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static android.content.Context.*;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * <p>
 * Please read:
 * <p>
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * <p>
 * before you start to get yourself familiarized with ContentProvider.
 * <p>
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 *
 * @author stevko
 */
public class GroupMessengerProvider extends ContentProvider {


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    /*
    source - idea learnt from the following link:
    https://developer.android.com/guide/topics/providers/content-provider-basics.html
    */
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v("insert", values.toString());
        /*Uri newUri = null;
        Context context = getContext();
        System.out.println("#######################" +context);
        */
        Set<String> keys = values.keySet();
        System.out.println("######################## keys: " + keys);
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        try {
//            newUri = getContext().getContentResolver().insert(uri, content);
            FileOutputStream output = getContext().openFileOutput(key, MODE_PRIVATE);
            output.write(value.getBytes());
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(GroupMessengerProvider.class.getName(), "File write failed");
        }
        return uri;
    }


    @Override
    public boolean onCreate() {

        // If you need to perform any one-time initialization task, please do it here.
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

/*
    source - idea learnt from the following link:
    https://developer.android.com/guide/topics/providers/content-provider-basics.html
    */

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        FileInputStream input = null;
        try {
//            source - PA2A documentation
            input = getContext().openFileInput(selection);
            InputStreamReader inputReader = new InputStreamReader(input);
            BufferedReader reader = new BufferedReader(inputReader);
            String value = reader.readLine();
            String[] req = new String[2];
            req[0] = "key";
            req[1] = "value";
            MatrixCursor matrixCursor = new MatrixCursor(req);
            String[] in = new String[2];
            in[0] = selection;
            in[1] = value;
            matrixCursor.addRow(in);
            return matrixCursor;
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(GroupMessengerProvider.class.getName(), "query call failed");
        }
        Log.v("query", selection);
        return null;
    }
}
