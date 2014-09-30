package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import org.w3c.dom.NodeList;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MatrixCursor.RowBuilder;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {
	
	DHTDatabase db;
	static final int LISTENPORT = 10000;
	SQLiteQueryBuilder queryBuilder;
	SQLiteDatabase dynamoDB;
	Cursor cursor = null;
	String myOwnPort;
	Node distroSys;
	static Node head;
	Node cood;
	public static Node mynode;
	NodeMap nm;
	String TAG = "ServerTask";
	int quorum = 3;
	static final String INSERT = "insert";
    static final String LQUERY = "@";
    static final String GQUERY = "*";
    static final String FINDKEY = "findKey";
    static final String FOUNDKEY = "foundKey";
    static final String UPDSUC = "upS";
    static final String UPDPRE = "upP";
    static final String GDEL = "gDel";
    static final String SDEL = "sDel";
    static final String LDEL = "lDel";
    static String data;
    public static boolean isDumpComplete = false;
    public static boolean isKeyFindComplete = false;
	public static String retVal = "";
	public static Object lockObj = new Object();
	public static boolean flagReQuery = false;
	
	
    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
    	synchronized (lockObj) {
			
	
    	if(selection.equals(LQUERY)){
					dynamoDB.delete(DHTable.TAB_DHT, null, null);
	    	}
			else if (selection.equals(GQUERY)){
					dynamoDB.delete(DHTable.TAB_DHT, null, null);

					new Thread(new ClientTask(mynode.succ.getAssocPort(), GDEL, "", "")).start();	
			}
			else {
			
				dynamoDB.delete(DHTable.TAB_DHT, null, null);
				new Thread(new ClientTask(mynode.succ.getAssocPort(), SDEL, "", "")).start();
				new Thread(new ClientTask(mynode.succ.succ.getAssocPort(), SDEL, "", "")).start();

				/*	 int countRows = dynamoDB.delete(DHTable.TAB_DHT, DHTable.COL_KEY+"='"+selection+"'", null);
				
				 if(countRows>0)
				 {
				 
				 }
				 else{
						new Thread(new ClientTask(mynode.succ.getAssocPort(), SDEL, selection, "")).start();

				 }*/
			}	
    	
		return 0;
    	}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
	synchronized (lockObj) {
		while(RecoveryTask.QueryBlock);

		Node cood = findPartition(values.getAsString("key"));
		
		//quorum used to replicate to the suppose to partition 
				
		for(int i = 0 ; i < quorum ; i++){
			Log.v("Check cood->" , cood.getAssocPort());
			//new Thread(new ClientTask(cood.getAssocPort(), INSERT, values.getAsString("key"), values.getAsString("value"), Integer.toString(i+1))).start();;
			new Thread(new ClientTask(cood.getAssocPort(), INSERT, values.getAsString("key"), values.getAsString("value"))).start();;

			cood = cood.succ;
		}
		
	
		return uri;
	}
	}
	
	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
	
		
		
		nm = new NodeMap();
		ArrayList<String> nodes = new ArrayList<String>();
		nodes.add("5554");
		nodes.add("5556");
		nodes.add("5558");
		nodes.add("5560");
		nodes.add("5562");
		for(String reqPort : nodes){
			nm.returnJoin(reqPort);
		}
        
		Log.v("InOnCreate", "Im in create of content provider");
    	db = new DHTDatabase(getContext());
    	queryBuilder = new SQLiteQueryBuilder();
    	queryBuilder.setTables(DHTable.TAB_DHT);
    	dynamoDB = db.getReadableDatabase();
    	dynamoDB.delete(DHTable.TAB_DHT, null, null);
    	/*************************************/
		/*
		 * Generate the port number for the given port 
		 */
        //getting the values for connections via connecting Hack! for multiple avd's  
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
         
        myOwnPort = portStr;
        Log.v("InOnCreate", "My Port Number is"+ myOwnPort);
        

        head = nm.getNodeList();
        distroSys = nm.getNodeList();
		mynode = myNode();
		
		Log.v(TAG, "My Node is : " + mynode.getAssocPort());
		//synchronized (lockObj) {
			new Thread(new RecoveryTask(SimpleDynamoProvider.mynode,db,queryBuilder,dynamoDB)).start();
		//}
		

        
        try {
       /*--------*/    
        	ServerSocket serverSocket = new ServerSocket(LISTENPORT);
            new ServerTask(db,dynamoDB,queryBuilder).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            
        } catch (IOException e) {
            e.printStackTrace();
        	Log.e("InOnCreate", "Can't create a ServerSocket");
        }
		

		
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		
	synchronized (lockObj) {
	
		while(RecoveryTask.QueryBlock);
		
		
		
		if(selection.equals(LQUERY)){
			Log.v("@SPC","@ query called!!");
			Cursor lcursor = queryBuilder.query(dynamoDB, null, null, null, null, null, null);
			return lcursor;
    	}
		else if (selection.equals(GQUERY)){
			Cursor gcursor = queryBuilder.query(dynamoDB, null, null, null, null, null, null);
			int keyIndex = gcursor.getColumnIndex("key");
			int valueIndex = gcursor.getColumnIndex("value");
			
			
			
			data = "@" + mynode.getAssocPort() + "|";
			gcursor.moveToFirst();
		//	Log.v(TAG,"Adding my shit to dump - >!" + mynode.getAssocPort());

			if(gcursor.getCount()>0){
				while (!gcursor.isAfterLast()) {
					String key = gcursor.getString(keyIndex);
					String value = gcursor.getString(valueIndex);

					data = data + key + "," + value + "|";
						
				gcursor.moveToNext();
				}
			}
			gcursor.close();
			new Thread(new ClientTask(mynode.succ.getAssocPort(), GQUERY, data, "")).start();	
			while(!isDumpComplete){
				
			}
			Log.v(TAG,"Ok busy wait for the Global dump!!!");
			Log.v(TAG," Global dump!!!  is " + data);

			gcursor = resolveCursor(data);
			isDumpComplete = false;
			
			
			return gcursor;	
		}
		else {
			
			/*Node queryNode = findPartition(selection);
			
			if(queryNode.getAssocPort().equals(mynode.getAssocPort())){
				Cursor kcursor = queryBuilder.query(dynamoDB, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
				if(kcursor.getCount() > 0){
					return kcursor;
				}
				else{
					new Thread(new ClientTask(mynode.succ.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
					new Thread(new ClientTask(mynode.succ.succ.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
				}
			}
			else{
				for(int i = 0 ; i < quorum ; i++){
					//Log.v("Check cood->" , cood.getAssocPort());
					new Thread(new ClientTask(queryNode.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();

					queryNode = queryNode.succ;
				}
			}*/
			
			Cursor kcursor = queryBuilder.query(dynamoDB, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
			if(kcursor.getCount() > 0){
				return kcursor;
			}
			else{
				new Thread(new ClientTask(mynode.succ.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
				//new Thread(new ClientTask(mynode.succ.succ.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
				//new Thread(new ClientTask(mynode.pred.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
				//new Thread(new ClientTask(mynode.pred.pred.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
			}
			long timer = System.currentTimeMillis();	
				
				while(!isKeyFindComplete){
					
					if((System.currentTimeMillis() - timer) >= 5000){
						new Thread(new ClientTask(mynode.succ.getAssocPort(), FINDKEY, selection, mynode.getAssocPort())).start();
						timer = System.currentTimeMillis();
					}
						
				}
				
								
				
				//Log.v(TAG,"Ok busy wait for find key is over!!");
				MatrixCursor findkey = null;
				if(!retVal.equals("")){
					
					findkey = new MatrixCursor(new String[] {"key","value"});
					findkey.addRow(new String[] {selection,retVal});
				}
				
				findkey.moveToFirst();
				
		    	isKeyFindComplete = false;
		    	retVal = "";
				return findkey;
				
			}
		}
	}
	

	Cursor resolveCursor(String gdump){
	    	gdump = gdump.substring(0, gdump.length()-1);
	    	//Log.v("GGGDUMP","The list to split is  : " + gdump);
	    	String list[] = gdump.split("\\|");
	    	MatrixCursor gcur = new MatrixCursor(new String[] {"key","value"});
	    	
	    	for(String filterKV : list ){
	    		
	    		Log.v("Key val pair is : " , filterKV);
	    		
	    		if(filterKV.contains(",")){
	    			String kv [] = filterKV.split(",");
	    			RowBuilder rb = gcur.newRow();
	    			rb.add("key", kv[0]);
	    			rb.add("value",kv[1]);
	       		}
	    	}
	    	
	    	
	    	return gcur;
	    	
	    }
	
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    protected static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    protected static Node findPartition(String key){
		String myInitKey = key;
		Node partitionIs = null;
		Node distroSys = head;
	//	Log.v(TAG, "Im in node : " + myOwnPort + "and Key is : " + myInitKey );

		//find the partition it is supposed to go in...
		do{
			
			try {
	    		
	    		if(distroSys.pred.getNodeKey().compareTo(distroSys.getNodeKey()) > 0 ){//&& (genHash(myLocNode.getSucc()).compareTo(myLocNode.getNodeid()) > 0)){
	    			if(genHash(myInitKey).compareTo(distroSys.pred.getNodeKey()) > 0 || genHash(myInitKey).compareTo(distroSys.getNodeKey()) <= 0){
	        		//	Log.v(TAG, " im the smallest and the value is largest than the largest so i will save it");
	        			partitionIs = distroSys;
	        			break;
	     			}
	      		}else if((genHash(myInitKey).compareTo(distroSys.getNodeKey()) <= 0) && (genHash(myInitKey).compareTo(distroSys.pred.getNodeKey()) > 0)){
		    	//	Log.v(TAG, " is less than me ");
					partitionIs = distroSys;
					break;
    			}
	    		distroSys = distroSys.succ;
	    	}catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}		
		}while(distroSys!= head);
		
		distroSys = head;
		return partitionIs;
    }
 
    protected Node myNode(){
    	
		distroSys = head;
		//find the my node partition it is supposed to go in...
		do{
			if(distroSys.getAssocPort().compareTo(myOwnPort) == 0 ){
	    		return distroSys;
	    	}
	    	distroSys = distroSys.succ;
		}while(distroSys!= head);
		return null;
	}
    
    
 
   


}
