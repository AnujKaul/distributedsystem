package edu.buffalo.cse.cse486586.groupmessenger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
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

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

	  static final String TAG = GroupMessengerActivity.class.getSimpleName();
	  //defining the given ports for avd's that i am using !! :)
	  static final String REMOTE_PORT0 = "11108";
	  static final String REMOTE_PORT1 = "11112";
	  static final String REMOTE_PORT2 = "11116";
	  static final String REMOTE_PORT3 = "11120";
	  static final String REMOTE_PORT4 = "11124";
	  static final int MULTICAST_RECV_SOCKET = 10000;
	  //signifies the sequencer port
	  static String seqPort;
	  //my sequencer and causal classes !! hope they work on the execution during grading! 
	  Sequencer mySeq = new Sequencer(); 	//total orders
      Causal causal = new Causal();			//causal orders 
     
      @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        seqPort = REMOTE_PORT0;
        System.out.println("Port number is SEQ1:" + seqPort);
        
 /**************************NO SEEE    ************************************************************/      
       
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
/**********************************************************....***************************************/
  
//getting the values for connections via connecting Hack! for multiple avd's  
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
           
        	ServerSocket serverSocket = new ServerSocket(MULTICAST_RECV_SOCKET);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            //System.out.println("I am here!!");
        } catch (IOException e) {
            e.printStackTrace();
        	Log.e(TAG, "Can't create a ServerSocket");
            return;

        }
   
        Button Compose =(Button) findViewById(R.id.button4);
        final EditText editText = (EditText) findViewById(R.id.editText1);
      //on the button click message is being sent from the client to rest other!! which ever avd is sending! 
        Compose.setOnClickListener(new Button.OnClickListener() 
        {
            public void onClick(View v)
            {
                
            	String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                Log.v(TAG, "Going to excute clientask");
               //call the async executor passing key and value(key = null , msg is from the edit text)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, null, msg);
            }
        });        
        
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
    
    
    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     */ 
     
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    	Socket cliOnSock;
    	String msgKey;
    	MyMessage mltcstMsg,finalMessage;
    	 ContentValues values = new ContentValues();
         OnPTestClickListener p = new OnPTestClickListener(null, null);

    	 @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
				while(true){  
					
					cliOnSock = serverSocket.accept();
     				ObjectInputStream in  = new ObjectInputStream(cliOnSock.getInputStream());
					MyMessage messageObject = (MyMessage) in.readObject();
					
	     				 
						if(messageObject != null) // check to find if object is not null stupid it may seem but imp 
						{	
				        	 msgKey = messageObject.getKey();
				        	 if(msgKey == null) 	// message by others to sequencer and now i the almighty sequencer will totally order them..
						        {
				        		 	// sending msgs to my delivery queue in my Seq class
				        		 	mySeq.getDelivery(messageObject);
				        		 	//sending multicast after ordering  
						        	if((mltcstMsg = mySeq.multicastMessage())!=null)
						        	{
						        		//sending asynchronously!! 
						        		new ClientTask().executeOnExecutor(SERIAL_EXECUTOR,seqPort,mltcstMsg.getKey(),mltcstMsg.getValue() );
						        	}
						       
						        }
						        else{
							        //now that all the clients receive their ordered msgs we have to see for causal
						        	if((finalMessage = causal.checkCausal(messageObject)) != null){
						        		//my causal function returns me cusally arranged items which i throw to content provider and let the mf save it!! 
						        		values.put(GroupMessengerTable.COL_KEY, finalMessage.getKey());
						        		values.put(GroupMessengerTable.COL_VALUE, finalMessage.getValue());
						        		//small hack i was given the uri string in one of the classes why not use
						        		//the available resources from OnPtest than waste doing redundant work
						        		getContentResolver().insert(p.mUri, values);
						        		publishProgress(finalMessage.getKey() +" <> " +finalMessage.getValue());
						        	}
						        }
				        	
				        	
				       	}
	     				
	     				in.close();
					}	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				Log.e(TAG, "Something is Wrong!!");
			}
            
           return null;
        }


		protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
        	String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
                
            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;
        }
    }

      
    private class ClientTask extends AsyncTask<String, Void, Void> {

       	@Override
        protected Void doInBackground(String... msgs) {
       	
         	 	if(msgs[1] != null){
     				//System.out.println("I am a msg being sent by the sequencer to all including self!!!");
       				sendMulticast(msgs);	
       			}
       			else{
     				//System.out.println("I am a msg being sent to the sequencer by everyone including self :P!!!");

       				sendToSequencer(msgs);
       			}	
            return null;
        }
      	/*
      	 * function to send message object to sequence for ordering
      	 * 
      	 * */
       	
       	protected void sendToSequencer(String...msgs){
       		try{
       		String sequncerPort = seqPort;
   			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sequncerPort));
           
            String msgToSend = msgs[2];
				             
            ObjectOutputStream msgObject = new ObjectOutputStream(socket.getOutputStream());
            msgObject.writeObject( new MyMessage(null, msgToSend));
            socket.close();
       		
       		} catch (UnknownHostException e) {
       			Log.e(TAG, "ClientTask UnknownHostException");
       		} catch (IOException e) {
       			Log.e(TAG, "ClientTask socket IOException");
       		}
       	}
      	
       	/*
       	 * function to multicast the message to everyone including self!!
       	 * 
       	 * */
       	
       	
       	protected void sendMulticast(String...msgs){
       		try{
       		
       			ArrayList<String> multicastGroup = new ArrayList<String>();
       			multicastGroup.add(REMOTE_PORT0);
       			multicastGroup.add(REMOTE_PORT1);
       			multicastGroup.add(REMOTE_PORT2);
       			multicastGroup.add(REMOTE_PORT3);
       			multicastGroup.add(REMOTE_PORT4);

       			
	       		for(String nodeIn : multicastGroup)
	   			{

	       			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeIn));
	   			    String msgToSend = msgs[2];
	   			   
	   			    ObjectOutputStream msgObject = new ObjectOutputStream(socket.getOutputStream());
	                msgObject.writeObject( new MyMessage(msgs[1], msgToSend));
	                socket.close();
	   			}
       		} catch (UnknownHostException e) {
       			Log.e(TAG, "ClientTask UnknownHostException");
       		} catch (IOException e) {
       			Log.e(TAG, "ClientTask socket IOException");
       		}
       	}       	
       	
    }  
   
	
}
/*
 * creating a message object in the <key, vaule> format... and of course
 * the serialization is to send the object on the network. 
 * :P i thought u knew that !!
 */

class MyMessage implements Serializable{
	private String key;
	private String value;
	static final long serialVersionUID = 101101L;
	protected MyMessage(String keyExt, String valueExt){
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
	    
}
