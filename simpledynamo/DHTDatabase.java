package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.*;

public class DHTDatabase extends SQLiteOpenHelper {

	
	  private static final String DATABASE_NAME = "dhtDB.db";
	  private static final int DATABASE_VERSION = 2;

	  public DHTDatabase(Context context) {
	    super(context, DATABASE_NAME, null, DATABASE_VERSION);
	  }

	  // Method is called during creation of the database
	  @Override
	  public void onCreate(SQLiteDatabase database) {
		  DHTable.onCreate(database);
	  }

	  // Method is called during an upgrade of the database,
	  // e.g. if you increase the database version
	  // followed as per the android tutorial, might be useful later !!! ... or never :)
	  @Override
	  public void onUpgrade(SQLiteDatabase database, int oldVersion,
	      int newVersion) {
		  DHTable.onUpgrade(database, oldVersion, newVersion);
	  }	
}