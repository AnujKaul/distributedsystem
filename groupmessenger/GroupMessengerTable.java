package edu.buffalo.cse.cse486586.groupmessenger;

import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class GroupMessengerTable {

	// Database table
	  public static final String TAB_GRPMSG = "grpmsg"; 	// name of the table
	  public static final String COL_KEY = "key";
	  public static final String COL_VALUE = "value";
	 
	// Creating the database using SQL statement with <key,value> entries!!
	  private static final String DATABASE_CREATE = "create table " 
	      + TAB_GRPMSG
	      + "(" 
	      + COL_KEY + " text primary key, " 
	      + COL_VALUE + " text not null "  
	      + ");";

	  public static void onCreate(SQLiteDatabase database) {
	    database.execSQL(DATABASE_CREATE);			//creates new table
	  }

	  public static void onUpgrade(SQLiteDatabase database, int oldVersion,
	      int newVersion) {
	    Log.w(GroupMessengerTable.class.getName(), "Upgrading database from version "
	        + oldVersion + " to " + newVersion
	        + ", which will destroy all old data");
	    database.execSQL("DROP TABLE IF EXISTS " + TAB_GRPMSG);
	    onCreate(database);
	  }
	
}
