package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.HashMap;

import android.content.ContentValues;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.util.Log;

public class RecoveryTask extends Thread{
	Node myNode;
	public final String RDUMP = "rdump";
	static String data = "";

	DHTDatabase db;
	SQLiteQueryBuilder queryBuilder;
	SQLiteDatabase dynamoDB;
	ContentValues hmapVal = new ContentValues();
	public static int ackCounter = 3;
	public static boolean QueryBlock = true;
	
	public RecoveryTask(Node myPort,DHTDatabase db,SQLiteQueryBuilder queryBuilder,	SQLiteDatabase dynamoDB) {
		this.myNode = myPort;
		this.db = db;
		this.dynamoDB = dynamoDB;
		this.queryBuilder = queryBuilder;
	}
	HashMap<String, String> recoverMap = new HashMap<String, String>();	
	//HashMap<String, String> succMap = new HashMap<String, String>();	

	public void run(){
		Log.v("RecoveryTask", "############I AM RECOVERING " + SimpleDynamoProvider.mynode.getAssocPort());
		getDump();
		
		
		//synchronized (SimpleDynamoProvider.lockObj) {
		Log.v("RecoveryDump ->", "Pred 1 -> " + data);
		
			if(data.equals("")){
			}
			else{
	
				resolveCursor(data);
			}
			
			
			QueryBlock = false;
		//}
	}	
	
	public void  getDump(){
	//synchronized (SimpleDynamoProvider.lockObj) {
		
		new ClientTask(myNode.pred.getAssocPort(), RDUMP, myNode.getAssocPort(), "p1").start();
		new ClientTask(myNode.pred.pred.getAssocPort(), RDUMP, myNode.getAssocPort(), "p2").start();
		new ClientTask(myNode.succ.getAssocPort(), RDUMP, myNode.getAssocPort(), "s1").start();
		//new ClientTask(myNode.succ.succ.getAssocPort(), RDUMP, myNode.getAssocPort(), "s2").start();

		while(ackCounter>0){
			
		}	
			
		
	}
		
	void resolveCursor(String gdump){
	//	synchronized (dynamoDB) {

			gdump = gdump.substring(0, gdump.length()-1);
	    	Log.v("RecoveryTask","The list to split is  : " + gdump);
	    	String list[] = gdump.split("\\|");
	    	
	    	for(String filterKV : list ){
	    		Log.v("Key val pair is : " , filterKV);
	    		
	    		if(filterKV.contains(",")){
	    			String kv [] = filterKV.split(",");
	    			hmapVal.put("key", kv[0]);
		    		hmapVal.put("value", kv[1]);
		    		putInDB(hmapVal);
		    	}
	    	}  
		Log.v("RecoverySPC" , "Entering data into the DB finished");
	///	}
    }
	
	
	 public void putInDB(ContentValues values){
     	dynamoDB = db.getWritableDatabase();
     	dynamoDB.insertWithOnConflict(DHTable.TAB_DHT, null, values, SQLiteDatabase.CONFLICT_REPLACE);
 		Log.v("RecoveryTask", values.toString());
 	
     }
   
	
		

}
	
