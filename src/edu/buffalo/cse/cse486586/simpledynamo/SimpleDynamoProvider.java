package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String[] REMOTE_PORT = {"11108","11112","11116","11120","111124"};
	private static final String TAG = "SimpleDynamo";
	private static String MY_PORT ;
	private static String MY_AVD_NUM ;
	private static final String TABLE_NAME = "messageTable";
	private static final String KEY_FIELD = "key",VALUE_FIELD="value";
	private static final String SQL_CREATE_MAIN = "CREATE TABLE " +
		    TABLE_NAME +                       // Table's name
		    " (" +                           // The columns in the table
		    " key STRING, " +
		    " value STRING )";
	private static String MY_PORT_HASH = null;
	private static ArrayList<String> MEMBER_LIST = new ArrayList<String>();
	private DatabaseHelper mOpenHelper;
	private SQLiteDatabase messageTable;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	public boolean[] isAlive = {true,true,true,true,true};
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		int i =0;
		messageTable = mOpenHelper.getWritableDatabase();
		String keyHash = genHash(values.getAsString(KEY_FIELD));
		for(String temp: MEMBER_LIST){
			if(keyHash.compareTo(temp)<0){
				break;
			}
			i++;
		}
		Message message = new Message("insert", REMOTE_PORT[i]);
		message.setKey(values.getAsString(KEY_FIELD));
		message.setValue(values.getAsString(VALUE_FIELD));
		if(MY_PORT.equalsIgnoreCase(message.getDestination_Port())){
			messageTable.insert(TABLE_NAME, null, values);
		}
		else{
			Message M[] = new Message[1];
			M[0]= message;
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, M);
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		mOpenHelper = new DatabaseHelper(getContext(),TABLE_NAME+".db");
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));
        MY_AVD_NUM = portStr;
        MY_PORT_HASH =genHash(String.valueOf(MY_AVD_NUM));
        createMemList();
		return false;
	}
	private void createMemList(){
		for(String temp: REMOTE_PORT){
			String tempHash = genHash(temp);
			MEMBER_LIST.add(tempHash);
		}
		Collections.sort(MEMBER_LIST);
	}
	
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) {
        MessageDigest sha1 = null;
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        byte[] sha1Hash = sha1.digest(input.getBytes());
        @SuppressWarnings("resource")
		Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    protected static final class DatabaseHelper extends SQLiteOpenHelper {

		//private SQLiteDatabase messageTable;
    	
		private static final String TAG = "DatabaseHelper"; 
		
		public DatabaseHelper(Context context,String tableName){
			super(context,tableName,null,1);
		}
		
		@Override
		public void onCreate(SQLiteDatabase database) {
			Log.i(TAG,"inside onCreate" );
			database.execSQL(SQL_CREATE_MAIN);
		}
		
		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// TODO Auto-generated method stub
			Log.i(TAG,"inside onUpgrade" );
		}
	}
    private class ClientTask extends AsyncTask<Message, Void, Void>{

		@SuppressWarnings("resource")
		@Override
		protected Void doInBackground(Message... messages) {
			Socket socket = null;
			ObjectOutputStream out= null;
			ObjectInputStream in = null;
			try {
				 socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				        Integer.parseInt(messages[0].getDestination_Port()));
				 out = new ObjectOutputStream(socket.getOutputStream());
				 out.writeObject(messages[0]);
				 socket.setSoTimeout(1000);
				 in = new ObjectInputStream(socket.getInputStream());
				 Message reply = (Message)in.readObject();
			} catch (NumberFormatException | IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
			if(messages[0].getType().equals("insert")){
				
			}else if(messages[0].getType().equals("query")){
				
			}else if(messages[0].getType().equals("delete")){
				
			}
			return null;
		}
    	
    }
}
