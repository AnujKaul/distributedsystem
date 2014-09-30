package edu.buffalo.cse.cse486586.simpledht;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
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


public class SimpleDhtProvider extends ContentProvider {

	static final String TAG = SimpleDhtActivity.class.getSimpleName();
	
	private DHTDatabase db;
	static final int LISTENPORT = 10000;
    static final String HEADPORT = "5554";
    static final String JOIN = "requestJoin";
    static final String INSERTNEXT = "insertNext";
    static final String NODEINFO = "nodeInfo";
    static final String LQUERY = "@";
    static final String GQUERY = "*";
    static final String FINDKEY = "findKey";
    static final String FOUNDKEY = "foundKey";
    static final String UPDSUC = "upS";
    static final String UPDPRE = "upP";
    static final String GDEL = "gDel";
    static final String SDEL = "sDel";
    static final String LDEL = "lDel";
    
    static String retVal;
    SQLiteDatabase dhtdb;
	
	public LocalNode headNode;
	public LocalNode myLocNode;
	public String myOwnPort;
	public String myInitKey;
	public Uri myUri;
	String data;
	boolean isDumpComplete = false;
	boolean isKeyFindComplete = false;
	SQLiteQueryBuilder queryBuilder;
	SQLiteDatabase qmdb;
	Cursor cursor = null;
	
	// new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, msgtype, key/pred, value/succ);	

	 @Override
	    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
	        // TODO Auto-generated methstatic final String od stub
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
	    
	  	
	    @Override
	    public int delete(Uri uri, String selection, String[] selectionArgs) {
	       
	    
	    	if (myLocNode.getSucc().equals("")){
	    		//cursor = queryBuilder.query(qmdb, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
					if(selection.equals("*")){
						qmdb.delete(DHTable.TAB_DHT, null, null);
			    	}
					else if (selection.equals("@")){
						qmdb.delete(DHTable.TAB_DHT, null, null);	
			    	}
					else{
						qmdb.delete(DHTable.TAB_DHT, DHTable.COL_KEY+"='"+selection+"'", null);
				}
	    	}
	    	else{
	    		if(selection.equals(LQUERY)){
						qmdb.delete(DHTable.TAB_DHT, null, null);
		    	}
				else if (selection.equals(GQUERY)){
						qmdb.delete(DHTable.TAB_DHT, null, null);

						new Thread(new ClientTask(myLocNode.getSucc(), GDEL, "", "")).start();	
				}
				else {
					
					 int countRows = qmdb.delete(DHTable.TAB_DHT, DHTable.COL_KEY+"='"+selection+"'", null);
					
					 if(countRows>0)
					 {
					 
					 }
					 else{
							new Thread(new ClientTask(myLocNode.getSucc(), SDEL, selection, "")).start();

					 }
											
				}
	    	}
	    	
	    	// TODO Auto-generated method stub
	        return 0;
	    }
	
	
	    @Override
	    public String getType(Uri uri) {
	        // TODO Auto-generated method stub
	        return null;
	    }
	    
	    @Override
	    public boolean onCreate() {
	       
	    	Log.v(TAG, "Im in create of content provider");
	    	db = new DHTDatabase(getContext());
	    	queryBuilder = new SQLiteQueryBuilder();
	    	queryBuilder.setTables(DHTable.TAB_DHT);
	    	qmdb = db.getReadableDatabase();
	    	/*************************************/
			/*
			 * Generate the port number for the given port 
			 */
	        //getting the values for connections via connecting Hack! for multiple avd's  
	        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
	        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
	        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
	         
	        myOwnPort = myPort;
	        Log.v(TAG, "My Port Number is"+ myOwnPort);
	       
	        try {
	           
	        	ServerSocket serverSocket = new ServerSocket(LISTENPORT);
	            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
	            //System.out.println("I am here!!");
	        } catch (IOException e) {
	            e.printStackTrace();
	        	Log.e(TAG, "Can't create a ServerSocket");
	            return false;
	        }
	       
	       requestForJoin();
	        
	       return false;
	    }
	   
	    
	    
	    private void requestForJoin(){
     		try{

	    	
     			Integer myport = Integer.parseInt(myOwnPort) / 2;
    			String chckPort = myport.toString().trim();
     			myLocNode = new LocalNode(chckPort,genHash(chckPort));
//				
//					
			    	if(!myLocNode.getMyPort().equals(HEADPORT.toString())){
			    		new Thread (new ClientTask(HEADPORT, JOIN, myOwnPort, "")).start();
			        }
//			        else{
//			        	headNode = new LocalNode(HEADPORT,genHash(HEADPORT));
//			        }
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
//	
     	}

	    @Override
	    public Uri insert(Uri uri, ContentValues values) {
	    	
	    	try {
	    		myInitKey = values.getAsString("key");
	    		//Log.v(TAG, "Im in node : " + myOwnPort + "and Key is : " + myInitKey );
	    		//Log.v(TAG, "My node id: " + myLocNode.getNodeid() + "and Key ID  is : " + genHash(values.getAsString("key")) );

	    	if(myLocNode.getPred() == "" || myLocNode.getSucc() == "" ){
	    		Log.v(TAG, " i dont have a root node save everything");
				
				dhtdb = db.getWritableDatabase();
				dhtdb.insertWithOnConflict(DHTable.TAB_DHT, null, values, SQLiteDatabase.CONFLICT_REPLACE);
				Log.v("insert", values.toString());
			
	    	}
	    	else{
	    		if(genHash(myLocNode.getPred()).compareTo(myLocNode.getNodeid()) > 0 ){//&& (genHash(myLocNode.getSucc()).compareTo(myLocNode.getNodeid()) > 0)){
	    			if(genHash(myInitKey).compareTo(genHash(myLocNode.getPred())) > 0 || genHash(myInitKey).compareTo(myLocNode.getNodeid()) <= 0){
	        			Log.v(TAG, " im the smallest and the value is largest than the largest so i will save it");
	        			
						dhtdb = db.getWritableDatabase();
						dhtdb.insertWithOnConflict(DHTable.TAB_DHT, null, values, SQLiteDatabase.CONFLICT_REPLACE);
						Log.v("insert", values.toString());
	     			}
	        		else{
	        				Log.v(TAG, " Pass it to  " + myLocNode.getSucc());
							new Thread (new ClientTask( myLocNode.getSucc(), INSERTNEXT, values.getAsString("key"), values.getAsString("value"))).start();
		        		}
	    		}
	    		else{
		    			if(genHash(values.getAsString("key")).compareTo(myLocNode.getNodeid()) <= 0 && (genHash(values.getAsString("key")).compareTo(genHash(myLocNode.getPred())) > 0)){
				    		Log.v(TAG, " is less than me ");
							dhtdb = db.getWritableDatabase();
							dhtdb.insertWithOnConflict(DHTable.TAB_DHT, null, values, SQLiteDatabase.CONFLICT_REPLACE);
							
							Log.v("insert", values.toString());
		    			}
						else{
							Log.v(TAG, " Pass it to  " + myLocNode.getSucc());
							new Thread (new ClientTask( myLocNode.getSucc(), INSERTNEXT, values.getAsString("key"), values.getAsString("value"))).start();
							//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myLocNode.getSucc(), INSERTNEXT, values.getAsString("key"), values.getAsString("value"));	
				    	}
							   		
					} 
	    		}
	    	}catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				}
				    		
	       	  return uri;
	    }

	    
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder) {
    	
    	//for the case when i have no other avd in loop...
		if (myLocNode.getSucc().equals("")){
    		//cursor = queryBuilder.query(qmdb, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
				if(selection.equals(LQUERY)){
					Cursor lcursor = queryBuilder.query(qmdb, null, null, null, null, null, null);
					return lcursor;
		    	}
				else if (selection.equals(GQUERY)){
					Cursor gcursor = queryBuilder.query(qmdb, null, null, null, null, null, null);
					return gcursor;	
		    	}
				else{
					Cursor kcursor = queryBuilder.query(qmdb, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
					//	cursor.setNotificationUri(getContext().getContentResolver(), uri);
					return kcursor;
			}
    	}
    	else{
    		if(selection.equals(LQUERY)){
				Cursor lcursor = queryBuilder.query(qmdb, null, null, null, null, null, null);
				return lcursor;
	    	}
			else if (selection.equals(GQUERY)){
				Cursor gcursor = queryBuilder.query(qmdb, null, null, null, null, null, null);
				int keyIndex = gcursor.getColumnIndex("key");
				int valueIndex = gcursor.getColumnIndex("value");
				
				
				data = "@" + myOwnPort + "|";
				gcursor.moveToFirst();
				Log.v(TAG,"Adding my shit to dump - >!" + myLocNode.getMyPort());

				if(gcursor.getCount()>0){
					while (!gcursor.isAfterLast()) {
						String key = gcursor.getString(keyIndex);
						String value = gcursor.getString(valueIndex);

						data = data + key + "," + value + "|";
							
					gcursor.moveToNext();
					}
				}
				else{
					//check for null values 
				}
				gcursor.close();
				//data = data + "|";
				new Thread(new ClientTask(myLocNode.getSucc(), GQUERY, data, "")).start();	
				while(!isDumpComplete){
					
				}
				Log.v(TAG,"Ok busy wait for the Global dump!!!");
				Log.v(TAG," Global dump!!!  is " + data);

				gcursor = resolveCursor(data);
				isDumpComplete = false;
				
				
				return gcursor;	
			}
			else {
				
				
				Cursor kcursor = queryBuilder.query(qmdb, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
				if(kcursor.getCount() > 0){
//					cursor.setNotificationUri(getContext().getContentResolver(), uri);
					return kcursor;
				}
				else{
					new Thread(new ClientTask(myLocNode.getSucc(), FINDKEY, selection, myLocNode.getMyPort())).start();
					while(!isKeyFindComplete){
						
					}
					
					Log.v(TAG,"Ok busy wait for find key is over!!");
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
			
    
    }

    Cursor resolveCursor(String gdump){
    	gdump = gdump.substring(0, gdump.length()-1);
    	Log.v("GGGDUMP","The list to split is  : " + gdump);
    	String list[] = gdump.split("\\|");
    	MatrixCursor gcur = new MatrixCursor(new String[] {"key","value"});
    	//int i=0;
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
     private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
 	    	
 	    	ObjectInputStream in;
 	    	MyMessage messageObject;
 	    	
 	    	//String msgType;
 	    	ContentValues values = new ContentValues();
 	    	OnTestClickListener t = new OnTestClickListener(null,null);
 	    	//Cursor cursor = null;
 	    	
 			
 			

 	    	 @Override
 	        protected Void doInBackground(ServerSocket... sockets) {
 	 	    	Log.v(TAG, "Im in do in background of server socket of content provider");
 	 	    	ServerSocket serverSocket = sockets[0];
 	 	    	//requestForJoin();
 	 	    	
 	    		
 	            if(serverSocket != null)
 	            {
 					while(true){  
 						try {
 						Socket cliOnSock = serverSocket.accept();
 						if(cliOnSock != null)
 						{
 							in  = new ObjectInputStream(cliOnSock.getInputStream());
 							messageObject = (MyMessage) in.readObject();
 							in.close();
 							cliOnSock.close();
 						}
 		     			
 							if(messageObject != null) // check to find if object is not null stupid it may seem but imp 
 							{	
 					        	 String msgType = messageObject.getMsgType();
 					        	 String msgKey = messageObject.getKey();
 					        	 String msgValue = messageObject.getValue();
 					        	 if(msgType.equals(JOIN)) 	
					        	 {
					     	    	
					        		Log.v(TAG, "I got a request for join ...");
					        		returnJoin(msgKey);
					        		//returnPosition(msgKey);
					        		 	       
							     }
					        	 else if(msgType.equals(NODEINFO)){

					        		 myLocNode.setPred(msgKey);
					        		 myLocNode.setSucc(msgValue);
					        		//new ClientTask( myLocNode.getSucc(), INSERTNEXT, values.getAsString("key"), values.getAsString("value")).start();

							     	Log.v(TAG, "Update for my node :" + myOwnPort + " UP Prev :" + myLocNode.getPred() + " UP Succ :" + myLocNode.getSucc());

					        		  
					        	 }
					        	 else if(msgType.equals(INSERTNEXT)){
				        		 	
					        			 if(genHash(myLocNode.getPred()).compareTo(myLocNode.getNodeid()) > 0 ){//&& (genHash(myLocNode.getSucc()).compareTo(myLocNode.getNodeid()) > 0)){
					        	        		if(genHash(msgKey).compareTo(genHash(myLocNode.getPred())) > 0 || genHash(msgKey).compareTo(myLocNode.getNodeid()) <= 0){
					        	        			values.put("key", msgKey);
							     	       			values.put("value", msgValue);
							     	       			getContext().getContentResolver().insert(t.mUri, values);
							     				}
					        	        		else{
								     					Log.v(TAG, " pass it to the next here in the  ");
						 			           				new Thread (new ClientTask(myLocNode.getSucc(), INSERTNEXT, msgKey, msgValue)).start();

								     						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myLocNode.getSucc(), INSERTNEXT, msgKey, msgValue);	
									     	    }
					        	    		}
					        			 else{
					        				 if(genHash(msgKey).compareTo(myLocNode.getNodeid()) <= 0 && genHash(msgKey).compareTo(genHash(myLocNode.getPred())) > 0){
						     					values.put("key", msgKey);
						     	       			values.put("value", msgValue);
						     	       			getContext().getContentResolver().insert(t.mUri, values);
						     				}
						     				else{
						     					Log.v(TAG, " pass it to the next here in the  ");
				 			           				new Thread (new ClientTask(myLocNode.getSucc(), INSERTNEXT, msgKey, msgValue)).start();

						     						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myLocNode.getSucc(), INSERTNEXT, msgKey, msgValue);	
							     	        	}
						     	        	}
					        	}else if (msgType.equals(GQUERY)) {
									
 					        		String msg = "@" + myOwnPort;
					        		Log.v("GQUERY  - > at ", myOwnPort + " is " + msgKey);
 					        		if(msgKey.startsWith(msg)){ 
 					        			data = msgKey;
 					        			Log.v(TAG,"GDump is : " + data);
 					        			isDumpComplete = true;
 					        			//getContext().getContentResolver().query(qm, null, GQREPLY, null, null);
 					        		}
 					        		else{ 
 					        			Log.v(TAG,"GDump till now is is : " + msgKey + "at freakin "+ myLocNode.getMyPort());

 					        			appendLocalDump(msgKey);
 					        		}//getContext().getContentResolver().query(t.mUri, null, GQUERY, null, null);
 					        		 
 					        		 
								}
					        	else if (msgType.equals(FINDKEY)){
					        	
					        			Log.v(TAG, "Im in the find next for !! - >" + myOwnPort);
					        			Cursor kcursor = queryBuilder.query(qmdb, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+msgKey+"'", null, null, null, null);
						        		String kVal = "";
					        			kcursor.moveToFirst();
										if(kcursor.getCount() > 0){
												//int k = kcursor.getColumnIndex("key");
												int v = kcursor.getColumnIndex("value");
							        			//String kVal = kcursor.getString(val);
							        			while (!kcursor.isAfterLast()) {
							    						//String key = kcursor.getString(k);
							    						String value = kcursor.getString(v);
	
							    						kVal = value;
							    							
							    					kcursor.moveToNext();
							    				}
							        			kcursor.close();
							    			
						        			Log.v(TAG, "##I found the" + msgKey + " here with value " + kVal );
						        			new Thread(new ClientTask(msgValue, FOUNDKEY, kVal, msgValue)).start();
						        			
						        		}
						        		else{
											new Thread(new ClientTask(myLocNode.getSucc(), FINDKEY, msgKey, msgValue)).start();
						        		}
					        	}
					        	else if(msgType.equals(FOUNDKEY)){
					        		 // if the sender recv the request back with a reply
					        		
					        			retVal = msgKey;
					        			Log.v("Query", "some one found the key -- >" + retVal);
					        			isKeyFindComplete = true;
					        			
					        	}
					        	else if (msgType.equals(UPDPRE)) {
				       				myLocNode.setPred(msgKey);
							     	Log.v(TAG, "Update for my node :" + myOwnPort + " UP Prev :" + myLocNode.getPred() + " UP Succ :" + myLocNode.getSucc());


								}
					        	else if (msgType.equals(UPDSUC)) {
					        		myLocNode.setSucc(msgKey);
							     	Log.v(TAG, "Update for my node :" + myOwnPort + " UP Prev :" + myLocNode.getPred() + " UP Succ :" + myLocNode.getSucc());

								}
					        	else if (msgType.equals(GDEL)){
					        		qmdb.delete(DHTable.TAB_DHT, null, null);

									new Thread(new ClientTask(myLocNode.getSucc(), GDEL, "", "")).start();	
							
					        	}
					        	else if (msgType.equals(SDEL)){
					        		int countRows = qmdb.delete(DHTable.TAB_DHT, DHTable.COL_KEY+"='"+msgKey+"'", null);
									
									 if(countRows>0)
									 {
									 
									 }
									 else{
											new Thread(new ClientTask(myLocNode.getSucc(), SDEL, msgKey, "")).start();

									 }
					        	}
 					        	
 					       	}
 						}catch (NoSuchAlgorithmException e) {
 			     			// TODO Auto-generated catch block
 			     			e.printStackTrace();
 			     		}catch (Exception e) {
 		 					// TODO Auto-generated catch block
 		 					Log.e(TAG, "Something is Wrong!!");
 		 					e.printStackTrace();
 		 				}
 						}
 	            }
 				
 	            return null;
 	          
 	        }
 	    /*****************************************************************************************************/	 
 	    	 
 	    void appendLocalDump(String myDump) {
 	       	
 	       	SQLiteQueryBuilder queryBuilder;
 	       	SQLiteDatabase qmdb;
 	       	queryBuilder = new SQLiteQueryBuilder();
 	   		queryBuilder.setTables(DHTable.TAB_DHT);
 	   		qmdb = db.getReadableDatabase();
 	       	String ldump = "";
 	   		Cursor resultCursor = queryBuilder.query(qmdb, null, null, null, null, null, null);
 	   		int keyIndex = resultCursor.getColumnIndex("key");
 	   		int valueIndex = resultCursor.getColumnIndex("value");
 	   		resultCursor.moveToFirst();
 	   		if(resultCursor.getCount()>0){
	 	   		while (!resultCursor.isAfterLast()) {
	 	   			String key = resultCursor.getString(keyIndex);
	 	   			String val = resultCursor.getString(valueIndex);
	 	   			String d = key + "," + val + "|";
	 	   			ldump = ldump + d;
	 	   			resultCursor.moveToNext();
	 	   		}
	 	   		resultCursor.close();
	 	   		myDump = myDump + ldump;
 	   		}
 	   		
 	   		
 	   		new Thread(new ClientTask(myLocNode.getSucc(),GQUERY, myDump, "")).start();
 	   	}	 
 	    
 	   /* checks the position of the current node at the leader node 
 		 * returns the update to all the members in the ring */
 		 
 	    public void returnJoin(String reqPort){
 	    	try{
	           		Integer mappedPort = (Integer.parseInt(reqPort) / 2);
	           		String newPort = mappedPort.toString().trim();
    			    Log.v(TAG, "Ok check for join chord by " + myLocNode.getMyPort());

    			    //check for the first join 
	           		if(myLocNode.getSucc().equals("")){
	        	    	Log.v(TAG, "Handling the frst member ==>" + newPort);

	        	    	myLocNode.setPred(newPort);
	        	    	myLocNode.setSucc(newPort);
	           			new Thread (new ClientTask(newPort, NODEINFO, myLocNode.getMyPort(), myLocNode.getMyPort())).start();

	        	    }
	           		else 
	           		{
		           		if(myLocNode.getNodeid().compareTo(genHash(myLocNode.getPred())) < 0){
		           		
		           			if(genHash(newPort).compareTo(genHash(myLocNode.getPred())) > 0 || genHash(newPort).compareTo(myLocNode.getNodeid()) <= 0){
		           				new Thread (new ClientTask(myLocNode.getPred(), UPDSUC, newPort, "")).start();
			           			new Thread (new ClientTask(newPort, NODEINFO, myLocNode.getPred(), myLocNode.getMyPort())).start();
			           			myLocNode.setPred(newPort);
			           			
	 		        	    	Log.v(TAG, "Handling the other member ==>" + newPort);

			           		}
		           			else{
			           			new Thread (new ClientTask(myLocNode.getSucc(), JOIN, reqPort,"")).start();
	
		           			}
	
		           		}
		           		else{
		           			if(myLocNode.getNodeid().compareTo(genHash(newPort)) >= 0 && genHash(myLocNode.getPred()).compareTo(genHash(newPort)) < 0){
		           		   			new Thread (new ClientTask(myLocNode.getPred(), UPDSUC, newPort, "")).start();
		           		   			new Thread (new ClientTask(newPort, NODEINFO, myLocNode.getPred(), myLocNode.getMyPort())).start();
		           		   			myLocNode.setPred(newPort);
		           			}
		           			else{
			           			new Thread (new ClientTask(myLocNode.getSucc(), JOIN, reqPort, "")).start();
	
		           			}
		           		}	
	           		}
	           		
	           } catch (NoSuchAlgorithmException e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		}
 	    }
     }
 	    
 	
	    
	    
	    
	    /*===============================================================================================*/
	   
}
	    /*===============================================================================================*/
	    
  /*
    * Send the message Object over the network..
    * with any or every information needed
    * */ 
			    class MyMessage implements Serializable{
			    	private String msgType;
			    	private String key;
			    	private String value;
			    	static final long serialVersionUID = 1L;
			    	MyMessage(String msgType, String keyExt, String valueExt){
			    		this.setMsgType(msgType);
			    		this.setKey(keyExt);
			    		this.setValue(valueExt);
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
			
					public String getMsgType() {
						return msgType;
					}
			
					public void setMsgType(String msgType) {
						this.msgType = msgType;
					}
			    
				
			}

/*
 * Local Node Information stored on every Node
 * 
 * */    
    
			class LocalNode{
				private String nodeid;
				private String myPort;
				private String succ="";
				private String pred="";
				
				public LocalNode(String myport, String nodeid) {
					this.setMyPort(myport);
					this.setNodeid(nodeid);
				}
				
				
				public String getNodeid() {
					return nodeid;
				}
				public void setNodeid(String nodeid) {
					this.nodeid = nodeid;
				}
				public String getSucc() {
					return succ;
				}
				public void setSucc(String succ) {
					this.succ = succ;
				}
				public String getPred() {
					return pred;
				}
				public void setPred(String pred) {
					this.pred = pred;
				}
				public String getMyPort() {
					return myPort;
				}
				public void setMyPort(String myPort) {
					this.myPort = myPort;
				}
				
				
			}
			 
			/*
 * Global Node Information stored on the Leader maintaining Node joins
 * 
 * */

			    
			class Node{
			    	
			    	private String nodeid;
			    	private String assocPort;
			    	Node pred;
			    	Node succ;
			    	
			    	Node(String assocPort,String nodeId){
			    		this.setNodeid(nodeId);
			    		this.setAssocPort(assocPort);
			    	}
			    	
			    	public String getNodeid() {
						return nodeid;
					}
					public void setNodeid(String nodeid) {
						this.nodeid = nodeid;
					}
					public String getAssocPort() {
						return assocPort;
					}
					public void setAssocPort(String assocPort) {
						this.assocPort = assocPort;
					}    		
			    }


			