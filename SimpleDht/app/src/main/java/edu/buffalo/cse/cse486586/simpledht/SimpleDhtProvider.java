package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;


/**
 * SimpleDhtProvider is a key-value table. Once again, please note that we do not implement
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
 * <p>
 * Edited by vipin on 4/9/17.
 *
 * @author stevko and vkumar25
 *         <p>
 *         References :
 *         1) https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html
 *         2) https://developer.android.com/guide/topics/providers/content-provider-basics.html
 *         3) https://developer.android.com/guide/topics/providers/content-provider-creating.html
 *         4) https://developer.android.com/reference/android/database/sqlite/SQLiteQueryBuilder.html
 *         5) https://developer.android.com/training/basics/data-storage/databases.html
 *         6) https://docs.oracle.com/javase/tutorial/networking/sockets/index.html ( How Server Sockets work )
 *         7) http://developer.android.com/reference/android/os/AsyncTask.html ( Read AsyncTask Life Cycle )
 *         8) https://docs.oracle.com/javase/tutorial/essential/concurrency/locksync.html
 *         9) https://developer.android.com/guide/topics/providers/content-provider-basics.html
 *         10) https://developer.android.com/guide/topics/providers/content-provider-creating.html
 *         11) http://www.cse.buffalo.edu/~stevko/courses/cse486/spring17/files/chord_sigcomm.pdf
 *         12) http://www.cse.buffalo.edu/~stevko/courses/cse486/spring17/lectures/15-dht.pdf
 */

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getName();
    private Context mContext;

    // For Databases
    private SQLiteDatabase mSqLiteDatabase;
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_NAME = "DHT";
    private static final String KEY_COLUMN_NAME = "key";
    private static final String VALUE_COLUMN_NAME = "value";
    private static final String TABLE_NAME = "KeyValueTable";
    private static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME +
            "( " + KEY_COLUMN_NAME + " TEXT PRIMARY KEY NOT NULL, " + VALUE_COLUMN_NAME + " TEXT NOT NULL);";

    // SERVER CLIENT FIELDS START
    private static final int SERVER_PORT = 10000;
    private static final int HEAD_NODE_PORT = 11108;

    private int MY_PORT;
    private int MY_SUCC_PORT;
    private int MY_PRED_PORT;

    private String MY_PORT_HASHKEY;
    private String MY_SUCC_PORT_HASHKEY;
    private String MY_PRED_PORT_HASHKEY;

    // Maintains the nodes order in the ring
    private ArrayList<RingNode> nodeList = new ArrayList<RingNode>();

    // An object to wait on in case of query
    private CustomMessage waitObj = createNewCustomMessage(Constants.QUERY);

    // SERVER CLIENT FIELDS ENDS

    /**
     * A class for handling Sqlite Database
     */
    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

        public MainDatabaseHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public SQLiteDatabase getWritableDatabase() {
            return super.getWritableDatabase();
        }

        @Override
        public SQLiteDatabase getReadableDatabase() {
            return super.getReadableDatabase();
        }

        @Override
        public synchronized void close() {
            super.close();
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }

        @Override
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            super.onDowngrade(db, oldVersion, newVersion);
        }

        @Override
        public void onOpen(SQLiteDatabase db) {
            super.onOpen(db);
        }
    }

    // OVERRIDDEN FUNCTIONS START

    @Override
    public boolean onCreate() {
        mContext = getContext();
        boolean isDatabaseCreated;
        isDatabaseCreated = createDataBase();
        initializeServer();
        return isDatabaseCreated;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        callInsert(values.getAsString(KEY_COLUMN_NAME), values.getAsString(VALUE_COLUMN_NAME));
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        CustomMessage newCustomMessage = createNewCustomMessage(Constants.QUERY);
        newCustomMessage.setToPort(MY_SUCC_PORT);
        newCustomMessage.setKey(selection);
        return callQuery(newCustomMessage);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        CustomMessage newCustomMessage = createNewCustomMessage(Constants.DELETE);
        newCustomMessage.setToPort(MY_SUCC_PORT);
        newCustomMessage.setKey(selection);
        callDelete(newCustomMessage);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    // OVERRIDDEN FUNCTIONS ENDS

    // ****************************** HELPER FUNCTIONS START ***************************************

    /**
     * A SHA-1 String Encoder Function
     *
     * @param input
     * @return Hashed String
     */
    private String genHash(String input) {
        Formatter formatter = new Formatter();
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return formatter.toString();
        }
    }

    /**
     * A function to create Database
     *
     * @return true if Database is created else false
     */
    private boolean createDataBase() {
        MainDatabaseHelper databaseHelper = new MainDatabaseHelper(mContext);

        mSqLiteDatabase = databaseHelper.getWritableDatabase();
        if (mSqLiteDatabase == null) {
            Log.e(TAG, "Function : onCreate, database is null.!!! Have Fun Fixing it now :P");
            return FALSE;
        } else {
            return TRUE;
        }
    }

    /**
     * Initialize Server and send the message to Head Node for join.
     */
    private void initializeServer() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "BAZOOKAAAA : " + e.getMessage());
            e.printStackTrace();
        }

        TelephonyManager tel = (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        MY_PORT = Integer.parseInt(myPort);

        MY_PRED_PORT = MY_PORT;
        MY_SUCC_PORT = MY_PORT;

        MY_PORT_HASHKEY = genHash(String.valueOf(MY_PORT / 2));
        MY_PRED_PORT_HASHKEY = MY_PORT_HASHKEY;
        MY_SUCC_PORT_HASHKEY = MY_PORT_HASHKEY;

        RingNode ringNode = new RingNode(MY_PORT, MY_PORT, MY_PORT, MY_PORT_HASHKEY, MY_PORT_HASHKEY, MY_PORT_HASHKEY);
        nodeList.add(ringNode);

        if (MY_PORT != HEAD_NODE_PORT) {
            CustomMessage customMessage = createNewCustomMessage(Constants.JOIN);
            sendMessage(customMessage);
        }
    }

    /**
     * Creates an Object of CustomMessage class.
     * Dummy Object
     *
     * @param messageType : Type of Message
     * @return Object of CustomMessage class
     */
    private CustomMessage createNewCustomMessage(String messageType) {
        CustomMessage customMessage = new CustomMessage(messageType, MY_PORT,
                HEAD_NODE_PORT, MY_SUCC_PORT, MY_PRED_PORT,
                MY_PORT_HASHKEY, MY_SUCC_PORT_HASHKEY, MY_PRED_PORT_HASHKEY,
                Constants.NULLVALUE, Constants.NULLVALUE, null);
        return customMessage;
    }

    /**
     * A function which sends the message to Client
     *
     * @param customMessage
     */
    private void sendMessage(CustomMessage customMessage) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, customMessage);
    }

    /**
     * Creates a function by breaking the input String
     *
     * @param message
     * @return An object of CustomMessage
     */
    private CustomMessage decomposeMessage(String message) {

        String messageArr[] = message.split(Constants.DELIM);

        String messageType = messageArr[0].trim();
        int senderPort = Integer.parseInt(messageArr[1].trim());
        int toPort = Integer.parseInt(messageArr[2].trim());
        int succPort = Integer.parseInt(messageArr[3].trim());
        int predPort = Integer.parseInt(messageArr[4].trim());


        String myPortHashKey = messageArr[5].trim();
        String succPortHashKey = messageArr[6].trim();
        String predPortHashKey = messageArr[7].trim();
        String key = messageArr[8].trim();
        String value = messageArr[9].trim();

        String hashData = messageArr[10].trim();

        ConcurrentHashMap<String, String> keyValMap = null;

        if (!hashData.equals(Constants.NULLVALUE) && hashData.length() != 0) {

            keyValMap = new ConcurrentHashMap<String, String>();
            String hashDataArr[] = hashData.split(Constants.DATASEPDELIM);
            int len = hashDataArr.length;
            for (int i = 0; i < len; i++) {
                String hashDataSepArr[] = hashDataArr[i].split(Constants.DATADELIM);
                String hashKey = hashDataSepArr[0];
                String hashValue = hashDataSepArr[1];
                keyValMap.put(hashKey, hashValue);
            }
        }

        CustomMessage composeMessage = new CustomMessage(messageType, senderPort, toPort, succPort, predPort,
                myPortHashKey, succPortHashKey, predPortHashKey, key, value, keyValMap);

        return composeMessage;
    }

    /**
     * If there is only one node in the ring
     *
     * @return true or false
     */
    private boolean isOnlyNode() {
        return (MY_PORT_HASHKEY.equals(MY_SUCC_PORT_HASHKEY) &&
                MY_PORT_HASHKEY.equals(MY_PRED_PORT_HASHKEY));
    }

    /**
     * If the key is after the last node of the ring
     *
     * @return true or false
     */
    private boolean isKeyAfterLastNode(String key) {
        return (((key.compareTo(MY_PORT_HASHKEY) > 0 && key.compareTo(MY_SUCC_PORT_HASHKEY) > 0) ||
                (key.compareTo(MY_PORT_HASHKEY) < 0 && key.compareTo(MY_SUCC_PORT_HASHKEY) < 0)) &&
                MY_PORT_HASHKEY.compareTo(MY_SUCC_PORT_HASHKEY) > 0);
    }

    /**
     * If the key is in middle of the two nodes
     *
     * @return true or false
     */
    private boolean isKeyMiddleNode(String key) {
        return (key.compareTo(MY_PORT_HASHKEY) > 0 &&
                key.compareTo(MY_SUCC_PORT_HASHKEY) < 0);
    }

    /**
     * Sort the nodes of the ring on the basis of their hashed node id(emulator ids)
     * and set their successor and predecessor.
     * <p>
     * How it works ?
     * <p>
     * Since we always know that the successor node value is always greater than current node value
     * and predecessor node value is always less than current node.
     * <p>
     * Only exception is when the node is the last node where we need to join it with the first node.
     * This is somewhat similar to circular queue data structures. And mathematically a modulo operation
     * <p>
     * I have added N so that i-1 will not be negative in case of i = 0;
     */
    private void sortAndSet() {
        Collections.sort(nodeList);
        int N = nodeList.size();
        for (int i = 0; i < N; i++) {
            RingNode node = nodeList.get(i);
            node.setPredPort(nodeList.get((N + i - 1) % N).getMyPort());
            node.setSuccPort(nodeList.get((N + i + 1) % N).getMyPort());
            node.setMyPredKeyHash(nodeList.get((N + i - 1) % N).getMyKeyHash());
            node.setMySuccKeyHash(nodeList.get((N + i + 1) % N).getMyKeyHash());
            nodeList.set(i, node);
        }
    }

    /**
     * New node joining request to the head node(5554)
     * We than sort the nodes and send new updated values to all the nodes in the ring
     *
     * @param message : CustomMessage
     */
    private void callJoin(CustomMessage message) {
        RingNode newJoinee = new RingNode(message.getSenderPort(), message.getSuccPort(), message.getPredPort(),
                message.getMyPortHashKey(), message.getSuccPortHashKey(), message.getPredPortHashKey());
        nodeList.add(newJoinee);
        sortAndSet();
        message.setMessageType(Constants.JOINUPDATE);
        sendMessage(message);
    }

    /**
     * Update of the successor and predecessor
     *
     * @param message
     */
    private void callJoinUpdate(CustomMessage message) {
        MY_SUCC_PORT = message.getSuccPort();
        MY_PRED_PORT = message.getPredPort();
        MY_SUCC_PORT_HASHKEY = message.getSuccPortHashKey();
        MY_PRED_PORT_HASHKEY = message.getPredPortHashKey();
    }

    /**
     * A function to insert a single key-value pair
     *
     * @param key
     * @param value
     */
    private void callSingleInsert(String key, String value) {
        ContentValues values = new ContentValues();
        values.put(KEY_COLUMN_NAME, key);
        values.put(VALUE_COLUMN_NAME, value);
        mSqLiteDatabase.insert(TABLE_NAME, "", values);
    }

    /**
     * A function to control the insert operations
     *
     * @param key   : key to be inserted
     * @param value : value to be inserted
     */
    private void callInsert(String key, String value) {

        String hashedKey = genHash(key);

        if (isOnlyNode()) {

            callSingleInsert(key, value);

        } else if (isKeyMiddleNode(hashedKey) || isKeyAfterLastNode(hashedKey)) {
            // We need to insert on the next port
            CustomMessage newCustomMessage = createNewCustomMessage(Constants.INSERTFINAL);
            newCustomMessage.setToPort(MY_SUCC_PORT);
            newCustomMessage.setKey(key);
            newCustomMessage.setValue(value);
            sendMessage(newCustomMessage);

        } else {
            // Keep on passing the values to successor node
            CustomMessage newCustomMessage = createNewCustomMessage(Constants.INSERT);
            newCustomMessage.setToPort(MY_SUCC_PORT);
            newCustomMessage.setKey(key);
            newCustomMessage.setValue(value);
            sendMessage(newCustomMessage);
        }
    }

    /**
     * A function when there is a single query operation
     *
     * @param customMessage
     * @return Cursor
     */
    private Cursor callSingleQuery(CustomMessage customMessage) {
        String newSelection = KEY_COLUMN_NAME + " = ?";
        String[] newSelectionArgs = {customMessage.getKey()};

        Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null, newSelection, newSelectionArgs, null, null, null);

        // If key is present on current node
        if (cursor != null && cursor.getCount() > 0) {

            cursor.moveToFirst();

            // if current node is not the sender node but we have found the key then send
            // the values to the sender
            if (MY_PORT != customMessage.getSenderPort()) {
                customMessage.setValue(cursor.getString(cursor.getColumnIndex(VALUE_COLUMN_NAME)));
                customMessage.setToPort(customMessage.getSenderPort());
                customMessage.setMessageType(Constants.QUERYRESULT);
                sendMessage(customMessage);
            } else {
                return cursor;
            }
        } else {
            // if key is not present on the current node

            // send the query to successor node
            customMessage.setToPort(MY_SUCC_PORT);
            sendMessage(customMessage);

            // we need to wait for the query result to arrive since the key is not present on the current node
            if (MY_PORT == customMessage.getSenderPort()) {
                synchronized (waitObj) {
                    try {
                        waitObj.wait();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // we build a matrix cursor from the returned values to return values
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME, VALUE_COLUMN_NAME});
                matrixCursor.addRow(new Object[]{waitObj.getKey(), waitObj.getValue()});
                return matrixCursor;
            }
        }
        return null;
    }

    /**
     * A function for returning all the locally stored messages in the node
     *
     * @return Cursor
     */
    private Cursor callLocalAllQuery() {
        Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null,
                null, null, null, null, null);
        return cursor;
    }

    /**
     * A function for returning all the stored messages in the ring
     *
     * @param customMessage
     * @return Cursor
     */
    private Cursor callGlobalAllQuery(CustomMessage customMessage) {

        // query the local database
        Cursor localDumpCursor = mSqLiteDatabase.query(TABLE_NAME, null,
                null, null, null, null, null);

        // get the hashmap values from the message.
        ConcurrentHashMap<String, String> localKeyValMap = customMessage.getKeyValMap();

        if (localKeyValMap == null) {
            localKeyValMap = new ConcurrentHashMap<String, String>();
        }

        // collect all the database values and pack them in message
        if (localDumpCursor != null && localDumpCursor.getCount() > 0) {
            localDumpCursor.moveToFirst();
            while (localDumpCursor.isAfterLast() == false) {
                String key = localDumpCursor.getString(localDumpCursor.getColumnIndex(KEY_COLUMN_NAME));
                String value = localDumpCursor.getString(localDumpCursor.getColumnIndex(VALUE_COLUMN_NAME));
                localKeyValMap.put(key, value);
                localDumpCursor.moveToNext();
            }
        }

        // if the successor node of the current node is the one who generated the query
        // mark the message type so that to stop the message from looping
        if (MY_SUCC_PORT == customMessage.getSenderPort()) {
            customMessage.setMessageType(Constants.QUERYRESULT);
        }

        // otherwise keep forwarding the message to all the nodes of the ring
        customMessage.setKeyValMap(localKeyValMap);
        customMessage.setToPort(MY_SUCC_PORT);
        sendMessage(customMessage);

        // we need to wait for the query result to arrive since the key is not present on the current node
        if (MY_PORT == customMessage.getSenderPort()) {
            synchronized (waitObj) {
                try {
                    waitObj.wait();
                } catch (Exception e) {
                    Log.e(TAG, "Good Lord!!! Enemy Thread attack : " + e.getMessage());
                    e.printStackTrace();
                }
            }
            // we build a matrix cursor from the returned values to return values
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME, VALUE_COLUMN_NAME});

            // collect all the values and pack them in matrix cursor
            ConcurrentHashMap<String, String> finalKeyValMap = waitObj.getKeyValMap();

            if (finalKeyValMap != null && finalKeyValMap.size() != 0) {
                for (String mapKey : finalKeyValMap.keySet()) {
                    matrixCursor.addRow(new Object[]{mapKey, finalKeyValMap.get(mapKey)});
                }
            }
            return matrixCursor;
        }
        return null;
    }

    /**
     * A function to control the query operations
     *
     * @param customMessage
     * @return Cursor
     */
    private Cursor callQuery(CustomMessage customMessage) {
	//@TODO Optimization needed !!!
        if (customMessage.getKey().equals(Constants.QUERYLOCALALL) ||
                (isOnlyNode() && customMessage.getKey().equals(Constants.QUERYALL))) {
            return callLocalAllQuery();
        } else if (customMessage.getKey().equals(Constants.QUERYALL)) {
            return callGlobalAllQuery(customMessage);
        } else {
            return callSingleQuery(customMessage);
        }
    }

    /**
     * A function to delete a single key
     *
     * @param customMessage
     * @return
     */
    private int callSingleDelete(CustomMessage customMessage) {
        String newSelection = KEY_COLUMN_NAME + " = ?";
        String[] newSelectionArgs = {customMessage.getKey()};

        // check if the data is present in the node database to delete
        Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null, newSelection, newSelectionArgs, null, null, null);

        if (cursor != null && cursor.getCount() > 0) {

            cursor.moveToFirst();
            String key = cursor.getString(cursor.getColumnIndex(KEY_COLUMN_NAME));
            String value = cursor.getString(cursor.getColumnIndex(VALUE_COLUMN_NAME));
            // if the successor node is the request generator
            if (MY_PORT != customMessage.getSenderPort()) {
                customMessage.setValue(cursor.getString(cursor.getColumnIndex(VALUE_COLUMN_NAME)));
                customMessage.setToPort(customMessage.getSenderPort());
                customMessage.setMessageType(Constants.DELETERESULT);
                sendMessage(customMessage);
            } else {
                return mSqLiteDatabase.delete(TABLE_NAME, newSelection, newSelectionArgs);
            }
        } else {
            // if the successor node is the request generator
            if (MY_SUCC_PORT == customMessage.getSenderPort()) {
                customMessage.setToPort(customMessage.getSenderPort());
                customMessage.setMessageType(Constants.DELETERESULT);
                sendMessage(customMessage);
            } else {
                // propagate the message
                customMessage.setToPort(MY_SUCC_PORT);
                sendMessage(customMessage);
                // wait till the delete operations are over in ring
                if (MY_PORT == customMessage.getSenderPort()) {
                    synchronized (waitObj) {
                        try {
                            waitObj.wait();
                        } catch (Exception e) {
                            Log.e(TAG, "Good Lord!!! Thread Attack" + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        return 0;
    }

    /**
     * A function to delete all the data from the ring
     *
     * @param customMessage
     * @return int value of delete operation
     */
    private int callGlobalDelete(CustomMessage customMessage) {
        // delete from current node first and then send the delete message to its successor
        int result = mSqLiteDatabase.delete(TABLE_NAME, null, null);

        // if the successor node is the request generator
        if (MY_SUCC_PORT == customMessage.getSenderPort()) {
            customMessage.setMessageType(Constants.DELETERESULT);
        }

        // send the message to successor
        customMessage.setToPort(MY_SUCC_PORT);
        sendMessage(customMessage);

        // wait till the delete operations are over in ring
        if (MY_PORT == customMessage.getSenderPort()) {
            synchronized (waitObj) {
                try {
                    waitObj.wait();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return result;
        }
        return 0;
    }

    /**
     * A function to delete all the local node data
     *
     * @return int value of delete operation
     */
    private int callLocalAllDelete() {
        return mSqLiteDatabase.delete(TABLE_NAME, null, null);
    }

    /**
     * A function to control the delete operations
     *
     * @param customMessage
     * @return int value of delete operation
     */
    private int callDelete(CustomMessage customMessage) {
	//@TODO Optimization needed !!!
        if (customMessage.getKey().equals(Constants.QUERYLOCALALL) || (isOnlyNode() && customMessage.getKey().equals(Constants.QUERYALL))) {
            return callLocalAllDelete();
        } else if (customMessage.getKey().equals(Constants.QUERYALL)) {
            return callGlobalDelete(customMessage);
        } else {
            return callSingleDelete(customMessage);
        }
    }

    // ****************************** HELPER FUNCTIONS ENDS ****************************************


    /**
     * Server for all the commuications
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {

                while (true) {

                    Socket clientSocket = serverSocket.accept();

                    OutputStream serverSocketOutputStream = clientSocket.getOutputStream();
                    InputStreamReader serverSocketInputStreamReader = new InputStreamReader(clientSocket.getInputStream());

                    PrintWriter serverOutputPrintWriter = new PrintWriter(serverSocketOutputStream, true);
                    BufferedReader serverInputBufferedReader = new BufferedReader(serverSocketInputStreamReader);

                    String commMessage;
                    String clientMessage = "";

                    while ((commMessage = serverInputBufferedReader.readLine()) != null) {
                        if (commMessage.equals(Constants.SYN)) {
                            serverOutputPrintWriter.println(Constants.SYNACK);
                        } else if (commMessage.equals(Constants.ACK)) {
                            serverOutputPrintWriter.println(Constants.ACK);
                        } else if (commMessage.equals(Constants.STOP)) {
                            serverOutputPrintWriter.println(Constants.STOPPED);
                            break;
                        } else {
                            if (commMessage.length() != 0) {
                                clientMessage = commMessage;
                                serverOutputPrintWriter.println(Constants.OK);
                            }
                        }
                    }
                    serverSocketOutputStream.close();
                    serverInputBufferedReader.close();
                    serverOutputPrintWriter.close();
                    serverSocketInputStreamReader.close();

                    CustomMessage message = decomposeMessage(clientMessage);

                    if (message.getMessageType().equals(Constants.JOIN)) {
                        callJoin(message);
                    } else if (message.getMessageType().equals(Constants.JOINUPDATE)) {
                        callJoinUpdate(message);
                    } else if (message.getMessageType().equals(Constants.INSERTFINAL)) {
                        callSingleInsert(message.getKey(), message.getValue());
                    } else if (message.getMessageType().equals(Constants.INSERT)) {
                        callInsert(message.getKey(), message.getValue());
                    } else if (message.getMessageType().equals(Constants.QUERY)) {
                        callQuery(message);
                    } else if (message.getMessageType().equals(Constants.QUERYRESULT)) {
                        synchronized (waitObj) {
                            waitObj.notify();
                            waitObj = message;
                        }
                    } else if (message.getMessageType().equals(Constants.DELETE)) {
                        callDelete(message);
                    } else if (message.getMessageType().equals(Constants.DELETERESULT)) {
                        synchronized (waitObj) {
                            waitObj.notify();
                            waitObj = message;
                        }
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException : " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "ServerTask socket Exception : " + e.getMessage());
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko and vkumar25
     */

    /**
     * A simple message sending protocol
     *
     * @param message
     */
    private void sendMessageForClient(CustomMessage message) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.getToPort());

            OutputStream clientOutputStream = socket.getOutputStream();
            InputStreamReader clientInputStreamReader = new InputStreamReader(socket.getInputStream());

            PrintWriter clientOutputPrintWriter = new PrintWriter(clientOutputStream, true);
            BufferedReader clientInputBufferReader = new BufferedReader(clientInputStreamReader);

            String msgFromServer;

            clientOutputPrintWriter.println(Constants.SYN);

            while ((msgFromServer = clientInputBufferReader.readLine()) != null) {

                if (msgFromServer.equals(Constants.SYNACK)) {
                    clientOutputPrintWriter.println(Constants.ACK);
                } else if (msgFromServer.equals(Constants.ACK)) {
                    clientOutputPrintWriter.println(message.toString());
                } else if (msgFromServer.equals(Constants.OK)) {
                    clientOutputPrintWriter.println(Constants.STOP);
                } else if (msgFromServer.equals(Constants.STOPPED)) {
                    break;
                }
            }
            clientOutputPrintWriter.close();
            clientInputBufferReader.close();
            socket.close();
        } catch (Exception e) {
            Log.e(TAG, "This is Client Sparta : " + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * Client for sending data
     */
    private class ClientTask extends AsyncTask<CustomMessage, Void, Void> {

        @Override
        protected Void doInBackground(CustomMessage... msgs) {

            CustomMessage message = msgs[0];

            if (message.getMessageType().equals(Constants.JOIN) ||
                    message.getMessageType().equals(Constants.INSERT) ||
                    message.getMessageType().equals(Constants.INSERTFINAL) ||
                    message.getMessageType().equals(Constants.QUERY) ||
                    message.getMessageType().equals(Constants.QUERYRESULT) ||
                    message.getMessageType().equals(Constants.DELETE) ||
                    message.getMessageType().equals(Constants.DELETERESULT)) {

                sendMessageForClient(message);
            } else if (message.getMessageType().equals(Constants.JOINUPDATE)) {

                int size = nodeList.size();

                for (int i = 0; i < size; i++) {
                    RingNode node = nodeList.get(i);
                    message.setToPort(node.getMyPort());
                    message.setPredPort(node.getPredPort());
                    message.setSuccPort(node.getSuccPort());
                    message.setSuccPortHashKey(node.getMySuccKeyHash());
                    message.setPredPortHashKey(node.getMyPredKeyHash());
                    sendMessageForClient(message);
                }
            }
            return null;
        }
    }
}
