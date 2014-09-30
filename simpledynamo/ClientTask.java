package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import android.util.Log;

public class ClientTask extends Thread{

	static final String JOIN = "requestJoin";
    static final String INSERT = "insert";
    static final String NODEINFO = "nodeInfo";
    static final String LQUERY = "@";
    static final String GQUERY = "*";
    static final String FINDKEY = "findKey";
    static final String FOUNDKEY = "foundKey";
    static final String RDUMP = "rdump";
    static final String RETDUMP = "returnDump";
    static final String GDEL = "gDel";
    static final String SDEL = "sDel";
    static final String LDEL = "lDel";
  
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	
	
	    	Socket socket; 
	 		OutputStream os;
	 		ObjectOutputStream msgObject;
	 		String port,type,key,value,aux;
	 		
	 		
	 		public ClientTask(String sport, String stype, String skey, String svalue) {
				this.port = sport;
				this.type = stype;
				this.key = skey;
				this.value = svalue;
				
 		   }
	 		public ClientTask(String sport, String stype, String skey, String svalue, String aux) {
				this.port = sport;
				this.type = stype;
				this.key = skey;
				this.value = svalue;
				this.aux = aux;
 		   }
	 		
	    	
	    	public void run(){
	    		
	    		//Log.v(TAG, "I am in fucking client!! @" + SimpleDynamoProvider.mynode.getAssocPort());
	    		//Log.v(TAG, "Sending to!! @" + port);
		 
	    		try {
	    			 	Integer sendPort = Integer.parseInt(port.trim()) * 2;
	    			 //	socket.setSoTimeout(100);
	    			 	socket	= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), sendPort);
						
						os = socket.getOutputStream();
						msgObject = new ObjectOutputStream(os);
						msgObject.reset();
					}catch(SocketException se){
						//here you get to know if the node you are sending to has died!!!
					}catch (UnknownHostException e) {
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
		       			else if(type.equals(RETDUMP)){
		
		                    msgObject.writeObject( new MyMessage(type, key, value));
		       			}
		       			else if (type.equals(RDUMP)) {
		       				msgObject.writeObject( new MyMessage(type, key, value,aux));

						}
		       			else if(type.equals(INSERT)){
		                    
		       				msgObject.writeObject( new MyMessage(type, key, value));

		       			}
		       			else if (type.equals(GQUERY)) {
		       				//Log.v(TAG, "Im herer in GQUERY CLIENT ");
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		       			else if (type.equals(FINDKEY)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}
		       			else if (type.equals(FOUNDKEY)) {
		       				msgObject.writeObject( new MyMessage(type, key, value));

						}else if (type.equals(GDEL)){
		       				msgObject.writeObject( new MyMessage(type, key, value));

			   			}else if (type.equals(SDEL)){
		       				msgObject.writeObject( new MyMessage(type, key, value));

			   			}
		                
					} 						
							           
		    	} catch (UnknownHostException e) {
		   			Log.e(TAG, "ClientTask UnknownHostException");
		   		} catch (IOException e) {
		   			if(type.equals(RDUMP))
		   			{
		   				RecoveryTask.ackCounter--;	
		   			}
		   			if (type.equals(GQUERY)) {
	       				new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.succ.getAssocPort(), type, key, value)).start();

		   			}
		   			if (type.equals(FINDKEY)) {
	       				new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.succ.getAssocPort(), type, key, value)).start();

					}
		   			if (type.equals(GDEL)){
		   				new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.succ.getAssocPort(), type, key, value)).start();

		   			}
		   		/*	if (type.equals(SDEL)){
		   				new Thread(new ClientTask(SimpleDynamoProvider.mynode.succ.succ.getAssocPort(), type, key, value)).start();

		   			}	*/	   			
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