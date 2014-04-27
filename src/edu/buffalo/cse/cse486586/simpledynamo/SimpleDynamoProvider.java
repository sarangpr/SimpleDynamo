package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

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

public class SimpleDynamoProvider extends ContentProvider {
	private static boolean isFirstLaunch = false;
	static String[] REMOTE_PORT ={"11124","11112","11108","11116","11120"};
	static final String[] AVD_NUM_LIST = {"5562","5556","5554","5558","5560"};
	private static int position=0;
	private static final String TAG = "SimpleDynamo";
	private static String MY_PORT ;
	private static String MY_AVD_NUM ;
	private static final String TABLE_NAME = "dynamoTable";
	private static final String KEY_FIELD = "key",VALUE_FIELD="value";
	static final int SERVER_PORT = 10000;
	private static final String SQL_CREATE_MAIN = "CREATE TABLE " +
		    TABLE_NAME +                       // Table's name
		    " (" +                           // The columns in the table
		    " key STRING, " +
		    " value STRING )";
	private static String MY_PORT_HASH = null;
	private static ArrayList<String> MEMBER_LIST_HASH = new ArrayList<String>();
	private DatabaseHelper mOpenHelper;
	private SQLiteDatabase messageTable;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
    	messageTable = mOpenHelper.getWritableDatabase();
    	if(selection.equalsIgnoreCase("@")){
    		messageTable.delete(TABLE_NAME, null, null);
    	}else if(selection.equalsIgnoreCase("*")){
    		messageTable.delete(TABLE_NAME, null, null);
    		for (int i =0; i<REMOTE_PORT.length;i++){
				if(REMOTE_PORT[i].equals(MY_PORT)){
					//skip the current node
					Log.e("queryAll", "skip loop");
					continue;
				}
			Message message = new Message("deleteAll",REMOTE_PORT[i]);
			message.setKey(null);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(message.getDestination_Port()));
				ObjectOutputStream out= new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(message);
				out.flush();
				socket.close();
			}catch(SocketTimeoutException ste){
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			
    		}
    	}else {
    		int i =0;
    		String keyHash = genHash(selection);
    		for(String temp: MEMBER_LIST_HASH){
    			if(temp.compareTo(keyHash)>=0){
    				break;
    			}
    			i++;	
    		}
    		i=i%5;
    		Message message = new Message("delete",REMOTE_PORT[i]);
    		if(MY_PORT.equalsIgnoreCase(message.getDestination_Port())){
    			messageTable.delete(TABLE_NAME, KEY_FIELD+"='"+selection+"'", null);
    		}else{
    			sendDelete(i, message);
    			sendDelete((i+1)%5, message);
    			sendDelete((i+2)%5, message);
    		}
    	}
    	
		return 0;
	}
	public void sendDelete(int i,Message message){
		message.setDestination_Port(REMOTE_PORT[i]);
		try{
		Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(message.getDestination_Port()));
		ObjectOutputStream out= new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(message);
		out.flush();
		socket.close();
		}catch(IOException e){
		}
	}
	public boolean[] isAlive = {true,true,true,true,true};
	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.d("insert", values.getAsString(VALUE_FIELD));
		int i =0;
		long insertTime = System.nanoTime();
		
		String keyHash = genHash(values.getAsString(KEY_FIELD));
		for(String temp: MEMBER_LIST_HASH){
			if(temp.compareTo(keyHash)>=0){
				break;
			}
			i++;	
		}
		i=i%5;
		Log.d("insert","insert at position "+i);
		Message message = new Message("insertHere", null);
		message.setOriginTime(insertTime);
		message.setKey(values.getAsString(KEY_FIELD));
		message.setValue(values.getAsString(VALUE_FIELD)+"##"+message.getOriginTime()+"##"+i);
		sendInsert(i, message);
		sendInsert((i+1)%5, message);
		sendInsert((i+2)%5, message);
		return null;
	}
	public void sendInsert(int i,Message message){
		message.setDestination_Port(REMOTE_PORT[i]);
		Log.d("sendInsert", "insert "+message.getKey()+" at "+ i+" and position is "+ position);
		Log.d("sendInsert","MY_PORT = "+MY_PORT+" destination port = "+message.getDestination_Port());
		if(MY_PORT.equalsIgnoreCase(message.getDestination_Port())){
			messageTable = mOpenHelper.getWritableDatabase();
			ContentValues value = new ContentValues();
			value.put(KEY_FIELD, message.getKey());
			value.put(VALUE_FIELD, message.getValue());
			messageTable.insert(TABLE_NAME, null, value);
			Log.d("sendInsert", "successfully inserted "+message.getKey()+" at "+position);
		}
		else{
			Log.d("sendInsert", "send to port "+message.getDestination_Port());
			 try {
				 synchronized(this){
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					        Integer.parseInt(message.getDestination_Port()));
				
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(message);
				out.flush();
				socket.close();
				 }
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public boolean onCreate() {
		Log.d("onCreate","entered");
		mOpenHelper = new DatabaseHelper(getContext(),TABLE_NAME+".db");
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));
        MY_AVD_NUM = portStr;
        MY_PORT_HASH =genHash(String.valueOf(MY_AVD_NUM));
        createMemList();
		for (String temp: AVD_NUM_LIST){
			if(temp.equals(MY_AVD_NUM)){
				break;
			}
			position++;
		}
		
        ServerSocket serverSocket=null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			Log.d("onCreate","serversocket created");
		} catch (IOException e) {
			e.printStackTrace();
		}
		Log.d("onCreate","exiting");
		new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		return false;
	}
	private synchronized HashMap<String, String> sendMessage(String port,Message message) throws SocketTimeoutException,IOException,
	ClassCastException,NumberFormatException, ClassNotFoundException, StreamCorruptedException{
		
		Socket socket=null;
        ObjectOutputStream out=null;
        ObjectInputStream in = null;
		socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(port));
		out= new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(message);
		out.flush();
		socket.setSoTimeout(1000);
		in = new ObjectInputStream(socket.getInputStream());
		HashMap<String, String>hashmap = (HashMap<String,String>)in.readObject();
		socket.close();
		return hashmap;
	}
	private HashMap<String, String> recover(int currPosition)throws SocketTimeoutException, StreamCorruptedException{
		HashMap<String, String> hashMap = null;
		Message recoveryMessage = new Message("recovery", REMOTE_PORT[currPosition]);
			try {
				hashMap=sendMessage(REMOTE_PORT[currPosition], recoveryMessage);
			}
			catch (NumberFormatException | IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return hashMap;
	}		
		
	public void populateDB(HashMap<String,String> hashMap){	
		for(Map.Entry<String,String> entry : hashMap.entrySet()){
			String [] tempTuple = entry.getValue().split("##");
			if(tempTuple[2].equalsIgnoreCase(String.valueOf(position))||
					tempTuple[2].equalsIgnoreCase(String.valueOf((position+4)%5))||
					tempTuple[2].equalsIgnoreCase(String.valueOf((position+3)%5))){
				ContentValues cV = new ContentValues();
				cV.put(KEY_FIELD, entry.getKey());
				cV.put(VALUE_FIELD, entry.getValue());
				messageTable = mOpenHelper.getWritableDatabase();
				Cursor c = messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE "+KEY_FIELD+"='"+entry.getKey()+"'", null);
				c.moveToFirst();
				if(c.getCount()==0){
					//if you don't have the particular key then insert
					messageTable.insert(TABLE_NAME, null, cV);
				}else if((c.getString(1).split("##"))[1].compareTo(tempTuple[1])<0){
					//insert only if it is the latest copy
					messageTable.insert(TABLE_NAME, null, cV);
				}
			}
		}
		if(hashMap.size()==0){
			messageTable.delete(TABLE_NAME, null, null);
		}
	}
	
	private void createMemList(){
		for(String temp: AVD_NUM_LIST){
			String tempHash = genHash(temp);
			MEMBER_LIST_HASH.add(tempHash);
			Log.d("createMemList", temp+" "+tempHash);
		}
	}
	
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Cursor cursor=null;
		MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD,VALUE_FIELD});
		HashMap<String, String> hashmap= new HashMap<String,String>();
		if(selection.equalsIgnoreCase("@")){
			cursor=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME, null);
			Log.d("query", "the number of elements found "+cursor.getCount());
			cursor.moveToFirst();
			while(!cursor.isAfterLast()){
				hashmap.put(cursor.getString(0),(cursor.getString(1).split("##"))[0]);
				cursor.moveToNext();
			}
		}else if(selection.equalsIgnoreCase("*")){
			// get all keys from local and then move on to querying from other nodes
			cursor = messageTable.rawQuery("SELECT * FROM "+TABLE_NAME, null);
			cursor.moveToFirst();
			while(!cursor.isAfterLast()){
				//load it into the cursor
				Log.d("queryAll", "inserted key= "+cursor.getString(0)+" in hashtable");
				hashmap.put(cursor.getString(0),cursor.getString(1));
				cursor.moveToNext();
			}
			for (int i =0; i<REMOTE_PORT.length;i++){
				if(REMOTE_PORT[i].equals(MY_PORT)){
					//skip the current node
					Log.e("queryAll", "skip loop");
					continue;
				}
				try {
					Message message = new Message("queryAll", REMOTE_PORT[i]);
					HashMap<String,String> inputHashMap = sendMessage(REMOTE_PORT[i], message);
					for(Map.Entry<String,String> entry : inputHashMap.entrySet()){
						if(hashmap.containsKey(entry.getKey())){
								if(((hashmap.get(entry.getKey())).split("##"))[1].
								compareTo((entry.getValue().split("##"))[1])<0){
									//insert only the latest copy
									hashmap.put(entry.getKey(), entry.getValue());
									Log.d("queryAll", "inserted key= "+entry.getKey()+" in hashtable");
								}
						}else {
							//or if the key isn't present already
							Log.d("queryAll", "inserted key= "+entry.getKey()+" in hashtable");
							hashmap.put(entry.getKey(), entry.getValue());
						}
					}
				}catch(SocketTimeoutException ste){
					//TODO: place holder for node failure handling
					Log.e("timeout", REMOTE_PORT[i]+" has timed out in querryAll");
					
				}
				catch (NumberFormatException | IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
			for(Map.Entry<String,String> entry : hashmap.entrySet()){
				hashmap.put(entry.getKey(), entry.getValue().split("##")[0]);
			}
		}else{
			//if single query
			Log.d("query","query for key "+selection);
			int i=0;
			String keyHash = genHash(selection);
			for(String temp: MEMBER_LIST_HASH){
				if(temp.compareTo(keyHash)>=0){
					break;
				}
				i++;	
			}
			i=i%5;
			if(REMOTE_PORT[i].equalsIgnoreCase(MY_PORT)){
				cursor = messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE "+KEY_FIELD+" = '"+selection+"'", null);
				cursor.moveToFirst();
				while(!cursor.isAfterLast()){
					//load it into the cursor
					hashmap.put(cursor.getString(0),(cursor.getString(1).split("##"))[0]);
					cursor.moveToNext();
				}
			}else{
				try {
						Log.d("query", "query for key "+selection+ " found at "+i);
						Message message = new Message("query", REMOTE_PORT[i]);
						message.setKey(selection);
						hashmap = sendMessage(message.getDestination_Port(), message);
				} catch ( SocketTimeoutException | StreamCorruptedException e){
					Log.wtf("timeout", REMOTE_PORT[i]+" has timed out in query");
					//try the next node 
						i=(i+1)%5;
						try {
							Message message = new Message("query", REMOTE_PORT[i]);
							message.setKey(selection);
							hashmap =sendMessage(message.getDestination_Port(), message);
						}catch(SocketTimeoutException | StreamCorruptedException ste){
							Log.wtf("timeout", REMOTE_PORT[i]+" has timed out in query");
							//try the next node 
								i=(i+1)%5;
								try {
									Message message = new Message("query", REMOTE_PORT[i]);
									message.setKey(selection);
									hashmap =sendMessage(message.getDestination_Port(), message);
								} catch(SocketTimeoutException ste1){
									Log.wtf("timeout", REMOTE_PORT[i]+" has timed out in query again!");
								}
								catch (NumberFormatException | IOException | ClassNotFoundException e1) {
									e1.printStackTrace();
								}
						}
						catch (NumberFormatException | IOException | ClassNotFoundException e1) {
							e1.printStackTrace();
						}
				}catch (NumberFormatException  | ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}
		}
		//convert the hashmap to cursor so that it can be returned
		for(Map.Entry<String,String> entry : hashmap.entrySet()){
			matrixCursor.addRow(new String[]{entry.getKey(),entry.getValue()});
			Log.d("query", "the received key: "+entry.getKey()+" value: "+entry.getValue());
		}
		matrixCursor.moveToFirst();
		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
			isFirstLaunch = true;
		}
		
		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		}
	}
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket socket=null;
			Message inputString =null;
			ObjectInputStream input =null;
			ObjectOutputStream out = null;
			messageTable = mOpenHelper.getWritableDatabase();
			if(!isFirstLaunch){
				HashMap<String, String> hashMap= null;
				Log.d("onCreate","not the first launch");
				try{
					hashMap = recover((position+4)%5);
				}catch(SocketTimeoutException | StreamCorruptedException ste ){
					
						try {
							hashMap = recover((position+3)%5);
						} catch (SocketTimeoutException | StreamCorruptedException e) {}
				}
				populateDB(hashMap);
				hashMap = null;
				try{
					hashMap = recover((position+1)%5);
				}catch(SocketTimeoutException | StreamCorruptedException ste){
					try {
						hashMap = recover((position+2)%5);
					} catch (SocketTimeoutException | StreamCorruptedException e) {}
				}
				populateDB(hashMap);
			}
			else{
				Log.d("onCreate","first launch");
			}
			while(true){
				Log.d("ServerTask","in ServerTask");
				synchronized(this){
					try {
						socket = serverSocket.accept();
						input = new ObjectInputStream(socket.getInputStream());
						inputString=(Message)input.readObject();
						Log.d("server", "message received");
					} catch (IOException | ClassNotFoundException e) {
						e.printStackTrace();
					}
					if(inputString.getType().equalsIgnoreCase("insertHere")){
						//check if the same key is already present in your content provider and insert only if latest
						Log.d("serverside", "insertHere with key: "+inputString.getKey());
						Cursor c=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE "+KEY_FIELD+"='"+inputString.getKey()+"'", null);
						c.moveToFirst();
						ContentValues cV = new ContentValues();
						String temp[]=null;
						cV.put(KEY_FIELD, inputString.getKey());
						cV.put(VALUE_FIELD, inputString.getValue());
						Log.d("serverside","cursor count is"+c.getCount());
						if(c.getCount()==0){
							//insert if key is not present
							messageTable.insert(TABLE_NAME,null,cV);
						}else{
							temp=(c.getString(1)).split("##");
							if(temp[1].compareToIgnoreCase(c.getString(1).split("##")[1])>0){
								//insert only if latest copy 
								messageTable.insert(TABLE_NAME,null,cV);
							}
						}
						
					}else if(inputString.getType().equalsIgnoreCase("query")){
						//TODO: query
						Log.d("server", "query for "+inputString.getKey());
						Cursor  cursor = messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE "+KEY_FIELD+" = '"+inputString.getKey()+"'",null);
						try {
							HashMap<String,String> hashmap = new HashMap<String,String>();
							out = new ObjectOutputStream(socket.getOutputStream());
							cursor.moveToFirst();
							Log.wtf("server", "key: "+inputString.getKey()+" found cursor size = "+cursor.getCount());
							while(!cursor.isAfterLast()){
								hashmap.put(cursor.getString(0),(cursor.getString(1).split("##"))[0]);
								cursor.moveToNext();
							}
							out.writeObject(hashmap);
							out.flush();
							socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						} 
					}else if(inputString.getType().equalsIgnoreCase("queryAll")||
							inputString.getType().equalsIgnoreCase("recovery")){
						try{
							Cursor cursor =messageTable.rawQuery("SELECT * FROM "+TABLE_NAME, null);
							HashMap<String,String> hashmap = new HashMap<>();
							out = new ObjectOutputStream(socket.getOutputStream());
							cursor.moveToFirst();
							Log.d("server", "key: "+inputString.getKey()+" found cursor size = "+cursor.getCount());
							while(!cursor.isAfterLast()){
								hashmap.put(cursor.getString(0),cursor.getString(1));
								cursor.moveToNext();
							}
							out.writeObject(hashmap);
							out.flush();
							socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						} 
					}else if(inputString.getType().equalsIgnoreCase("deleteAll")){
						messageTable.delete(TABLE_NAME, null, null);
					}else if(inputString.getType().equalsIgnoreCase("delete")){
						messageTable.delete(TABLE_NAME, inputString.getKey(), null);
					}
					try {
						socket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
    	
    }
}
