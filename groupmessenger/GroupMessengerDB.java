package edu.buffalo.cse.cse486586.groupmessenger;

import android.content.Context;
import android.database.sqlite.*;

public class GroupMessengerDB extends SQLiteOpenHelper {

	  private static final String DATABASE_NAME = "groupmessenger.db";
	  private static final int DATABASE_VERSION = 1;

	  public GroupMessengerDB(Context context) {
	    super(context, DATABASE_NAME, null, DATABASE_VERSION);
	  }

	  // Method is called during creation of the database
	  @Override
	  public void onCreate(SQLiteDatabase database) {
	    GroupMessengerTable.onCreate(database);
	  }

	  // Method is called during an upgrade of the database,
	  // e.g. if you increase the database version
	  // followed as per the android tutorial, might be useful later !!! ... or never :)
	  @Override
	  public void onUpgrade(SQLiteDatabase database, int oldVersion,
	      int newVersion) {
	    GroupMessengerTable.onUpgrade(database, oldVersion, newVersion);
	  }	
}
