package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.os.AsyncTask;
import android.util.Log;

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
	 /*
     * ServerTask ... starts here!
     */	    	
	    	ObjectInputStream in;
	    	MyMessage messageObject;
	    	
	    	ContentValues values = new ContentValues();
	    	static final String INSERT = "insert";
	        static final String LQUERY = "@";
	        static final String GQUERY = "*";
	        static final String FINDKEY = "findKey";
	        static final String FOUNDKEY = "foundKey";
	        static final String UPDSUC = "upS";
	        static final String RETDUMP = "returnDump";
	        static final String GDEL = "gDel";
	        static final String SDEL = "sDel";
	        static final String LDEL = "lDel";
	        static final String RDUMP = "rdump";
	        DHTDatabase db;
	    	static final int LISTENPORT = 10000;
	    	SQLiteQueryBuilder queryBuilder;
	    	SQLiteDatabase dynamoDB;
	    	Cursor cursor = null;
	    	String TAG = "ServerTask";
	    	String myData="";
	    	String parentData="";
	    	String grandpaData="";
	        
	    	
	    	public ServerTask(DHTDatabase db,SQLiteDatabase dyna, SQLiteQueryBuilder qur){
	    		this.db = db;
	    		this.dynamoDB = dyna;
	    		this.queryBuilder = qur;
	    	}
	        
	        /*Function to put values in a given data base.
	         * try modifiers "public synchronized"
	         * 
	         * */ 
	        public void put(ContentValues values){
	        	//synchronized (dynamoDB) {
	        		dynamoDB = db.getWritableDatabase();
		        	dynamoDB.insertWithOnConflict(DHTable.TAB_DHT, null, values, SQLiteDatabase.CONFLICT_REPLACE);
		    		Log.v("insert", values.toString());
		    	
				//}
	        	
	        }
	      
	        
	        /*Function to get values from the DB.
	         * try modifiers "public synchronized"
	         * 
	         * */  
	       public Cursor get(String selection){
	        	
	        	if(selection.equals("@")){
	    			Cursor lcursor = queryBuilder.query(dynamoDB, null, null, null, null, null, null);
	    			return lcursor;
	        	}
	    		else if (selection.equals("*")){
	    			Cursor gcursor = queryBuilder.query(dynamoDB, null, null, null, null, null, null);
	    			return gcursor;	
	        	}
	    		else{
	    			Cursor kcursor = queryBuilder.query(dynamoDB, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+selection+"'", null, null, null, null);
	    			return kcursor;
	    		}
	        	
	        }
	        
	       void appendLocalDump(String myDump) {
	 	       	
//	 	       Log.v(TAG, "Im apending  my dunp " + SimpleDynamoProvider.mynode.getAssocPort());	
	    	   queryBuilder = new SQLiteQueryBuilder();
	 	   		queryBuilder.setTables(DHTable.TAB_DHT);
	 	   		dynamoDB = db.getReadableDatabase();
	 	       	String ldump = "";
	 	   		Cursor resultCursor = queryBuilder.query(dynamoDB, null, null, null, null, null, null);
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
	 	   		
	 	   		
	 	   		new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.getAssocPort(),GQUERY, myDump, "")).start();
	 	   	}
	       
	       
			@Override
	        protected Void doInBackground(ServerSocket... sockets) {
	 	    	Log.v("ServerTask", "Im in do in background of server socket of content provider");
	 	    	ServerSocket serverSocket = sockets[0];
	 	    	
	 	    	

	            if(serverSocket != null)
	            {
					while(true){  
						try {
							Socket cliOnSock = serverSocket.accept();
							Log.v("ServerTask", cliOnSock.toString());
							if(cliOnSock != null)
							{
								in  = new ObjectInputStream(cliOnSock.getInputStream());
								
								messageObject = (MyMessage) in.readObject();
								in.close();
								cliOnSock.close();
							}
		     			   
							//synchronized (serverSocket) {	
		     			    	if(messageObject != null) // check to find if object is not null stupid it may seem but imp 
								{	
									
						        	String msgType = messageObject.getMsgType();
						        	String msgKey = messageObject.getKey();
						        	String msgValue = messageObject.getValue();
						        	Log.v(TAG, "we get " +  msgType );//+ msgKey + msgValue) ;
						        	
						        	if(msgType.equals(INSERT)){
					        		 //	String msgAux = messageObject.getAux();
						        		values.put("key", msgKey);
								     	values.put("value", msgValue);
								     	put(values);
								  		
								     	
						        	}else if (msgType.trim().equalsIgnoreCase(GQUERY)) {
										
						        		
						        	//	Log.v("ServerTask","FUCK it At for gdump->" + SimpleDynamoProvider.mynode.getAssocPort());

	 					        		String msg = "@" + SimpleDynamoProvider.mynode.getAssocPort();
						        		Log.v("GQUERY  - > at ", SimpleDynamoProvider.mynode.getAssocPort() + " is " + msgKey);
	 					        		if(msgKey.startsWith(msg)){ 
	 					        			SimpleDynamoProvider.data = msgKey;
	 					        			Log.v(TAG,"GDump is : " + SimpleDynamoProvider.data);
	 					        			SimpleDynamoProvider.isDumpComplete = true;
	 					        		}
	 					        		else{ 
	 					        			//Log.v(TAG,"GDump till now is is : " + msgKey + "at freakin "+ SimpleDynamoProvider.mynode.getAssocPort());

	 					        			appendLocalDump(msgKey);
	 					        		}
	 					        		 
									}
						        	else if (msgType.equals(FINDKEY)){
						        	
						        			//Log.v(TAG, "Im in the find next for !! - >" + SimpleDynamoProvider.mynode.getAssocPort());
						        			Cursor kcursor = queryBuilder.query(dynamoDB, null, DHTable.TAB_DHT +"."+DHTable.COL_KEY+"='"+msgKey+"'", null, null, null, null);
							        		String kVal = "";
						        			kcursor.moveToFirst();
											if(kcursor.getCount() > 0){
													int v = kcursor.getColumnIndex("value");
								        			while (!kcursor.isAfterLast()) {
								    						//String key = kcursor.getString(k);
								    						String value = kcursor.getString(v);
		
								    						kVal = value;
								    							
								    					kcursor.moveToNext();
								    				}
								        			kcursor.close();
								    			
							        			//Log.v(TAG, "##I found the" + msgKey + " here with value " + kVal );
							        			new Thread(new ClientTask(msgValue, FOUNDKEY, kVal, msgValue)).start();
							        			
							        		}
							        		else{
												new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.getAssocPort(), FINDKEY, msgKey, msgValue)).start();
							        		}
						        	}
						        	else if(msgType.equals(FOUNDKEY)){
						        		 // if the sender recv the request back with a reply
						        		
						        			SimpleDynamoProvider.retVal = msgKey;
						        			SimpleDynamoProvider.isKeyFindComplete = true;
						        			
						        	}
						        	else if(msgType.equals(RDUMP)){
						        		
					    				Log.v("RecoveryDump ->", "Sending "+ SimpleDynamoProvider.mynode.getAssocPort() +" -> " + "");

						        		
						        		findKV(msgValue);
						        		
/*//						        		if(msgValue.equals("s2")){
//						    				Log.v("RecoveryDump ->", "My Grand parent is:  -> " + grandpaData);
//
//						        			new Thread(new ClientTask(msgKey, RETDUMP, grandpaData, msgValue,SimpleDynamoProvider.mynode.getAssocPort())).start();
//						        		}else*/	  if(msgValue.equals("s1")){
					    				Log.v("RecoveryDump ->", "My  Parent data is:  -> " + parentData);

						        			new Thread(new ClientTask(msgKey, RETDUMP, parentData, msgValue,SimpleDynamoProvider.mynode.getAssocPort())).start();
						        		}
						        		else{
						    				Log.v("RecoveryDump ->", "My data is:  -> " + myData);

						        			new Thread(new ClientTask(msgKey, RETDUMP, myData, msgValue,SimpleDynamoProvider.mynode.getAssocPort())).start();
						        		}
						    			//Thread.sleep(1000);
						   
						        		/*}
						    			else{
						    				
						    				Log.v("RecoveryDump ->", "Sending "+ SimpleDynamoProvider.mynode.getAssocPort() +" -> " + "");

							    			new Thread(new ClientTask(msgKey, RETDUMP, "", msgValue)).start();
						    			}*/
						    			
						    			
						        		
						        		
						        	}else if(msgType.equals(RETDUMP)){
						        		
						        		RecoveryTask.ackCounter--;
						        		RecoveryTask.data = RecoveryTask.data + msgKey;
						        		
						        							        		
						        	}else if (msgType.equals(GDEL)){
						        		dynamoDB.delete(DHTable.TAB_DHT, null, null);

						        		new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.getAssocPort(), GDEL, "", "")).start();	

						        	}
					        		else if (msgType.equals(SDEL)){
						        		
					        			dynamoDB.delete(DHTable.TAB_DHT, null, null);
					        			
								}	
							}    	
						        	
						}catch(Exception e){
							
						}
    
					}
	            }
	 	  //  }
	    	 return null;
	    }
			
		protected void findKV(String type){
		//synchronized (SimpleDynamoProvider.lockObj) {
			
		
			myData = "";
			parentData = "";
			grandpaData = "";
			Cursor gcursor = queryBuilder.query(dynamoDB, null, null, null, null, null, null);
			int keyIndex = gcursor.getColumnIndex("key");
			int valueIndex = gcursor.getColumnIndex("value");
			gcursor.moveToFirst();

			if(gcursor.getCount()>0){

		//		try {
					while (!gcursor.isAfterLast()) {
					String key = gcursor.getString(keyIndex);
					String value = gcursor.getString(valueIndex);
				
						Node keyNode = SimpleDynamoProvider.findPartition(key);
				    	Log.v("Node returned is", keyNode.getAssocPort());

				    	if(type.equals("p1") || type.equals("p2")){
							if(keyNode.getAssocPort().equals(SimpleDynamoProvider.mynode.getAssocPort())){
								Log.v(TAG, " As Predecessor : Value i have to save so its my value ");
			    				myData = myData + key + "," + value + "|";
			     			}
				    	}else if(type.equals("s1")){
				    		 if(keyNode.getAssocPort().equals(SimpleDynamoProvider.mynode.pred.getAssocPort())){
				    	
							Log.v(TAG, " As Succecessor : Value i have to save for my parent");
		    				parentData = parentData + key + "," + value + "|";
			    		
				    		 }
				    	}/*else if(type.equals("s2")){
				    		if(keyNode.getAssocPort().equals(SimpleDynamoProvider.mynode.pred.pred.getAssocPort())){
						    								    	
							Log.v(TAG, " As Succecessor : Value i have to save for my parent");
		    				grandpaData = grandpaData + key + "," + value + "|";
			    		
				    		 }
				    	
				    	}
				    	*/
						
					gcursor.moveToNext();
				}
			//} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
			
		//	}
			gcursor.close();
		}
	    
		} 		
			
			
	
}


/*
 * Send the message Object over the network..
 * with any or every information needed
 * */ 
			  
class MyMessage implements Serializable{
			    	private String msgType;
			    	private String key;
			    	private String value;
			    	private String aux;
			    	static final long serialVersionUID = 1L;
			    	MyMessage(String msgType, String keyExt, String valueExt){
			    		this.setMsgType(msgType);
			    		this.setKey(keyExt);
			    		this.setValue(valueExt);
			    	}
			    	
			    	MyMessage(String msgType, String keyExt, String valueExt, String aux){
			    		this.setMsgType(msgType);
			    		this.setKey(keyExt);
			    		this.setValue(valueExt);
			    		this.setAux(aux);
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

					
					public String getAux() {
						return aux;
					}


					public void setAux(String aux) {
						this.aux = aux;
					}
			    
				
}

