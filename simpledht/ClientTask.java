package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import android.util.Log;

public class ClientTask extends Thread{

	static final String JOIN = "requestJoin";
    static final String INSERTNEXT = "insertNext";
    static final String NODEINFO = "nodeInfo";
    static final String LQUERY = "@";
    static final String GQUERY = "*";
    static final String FINDKEY = "findKey";
    static final String FOUNDKEY = "foundKey";
    static final String UPDSUC = "upS";
    static final String UPDPRE = "upP";

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
	
	
	    	Socket socket; 
	 		OutputStream os;
	 		ObjectOutputStream msgObject;
	 		String port,type,key,value;
	 		
	 		
	 		public ClientTask(String sport, String stype, String skey, String svalue) {
				this.port = sport;
				this.type = stype;
				this.key = skey;
				this.value = svalue;
				
 		   }
	 		
	    	
	    	public void run(){
	    		
	    		 try {
	    			 	Integer sendPort = Integer.parseInt(port.trim()) * 2;
						socket	= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), sendPort);
						
						os = socket.getOutputStream();
						msgObject = new ObjectOutputStream(os);
						msgObject.reset();
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
	    		
	    		try {
					if(socket!=null){   	      	    	        
		                if(type.equals(JOIN)){
		                	Log.v("join message", key);
		    	           	msgObject.writeObject(new MyMessage(type, key, value));	
		       			}
		       			else if(type.equals(NODEINFO)){
		
		                    msgObject.writeObject( new MyMessage(type, key, value));
		       			}
		       			else if (type.equals(UPDSUC)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		       			else if (type.equals(UPDPRE)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		       			else if(type.equals(INSERTNEXT)){
		                    
		       				msgObject.writeObject( new MyMessage(type, key, value));

		       			}
		       			else if (type.equals(GQUERY)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		       			else if (type.equals(FINDKEY)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		       			else if (type.equals(FOUNDKEY)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		                
					} 						
						
		           
		    	} catch (UnknownHostException e) {
		   			Log.e(TAG, "ClientTask UnknownHostException");
		   		} catch (IOException e) {
		   			e.printStackTrace();
		   			e.getMessage();
		   			Log.e(TAG, "ClientTask socket IOException");
		   		}
		    	finally{
		    		
	                try {
	                	//msgObject.flush();
						msgObject.close();
						os.close();
			            socket.close();
	                } catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}        
		    	}
	    	}	
	
}
