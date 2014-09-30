package edu.buffalo.cse.cse486586.groupmessenger;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;
/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

	//private static final Uri PROJECT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger.provider");
	private GroupMessengerDB db;
	
	SQLiteDatabase gmdb;
	
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
  
    	//load a writable instance of db into SQlite DB!
    	gmdb = db.getWritableDatabase();
    	//replaces the value if there is a key conflict so avoid error and overwrites!!
    	gmdb.insertWithOnConflict(GroupMessengerTable.TAB_GRPMSG, null, values, SQLiteDatabase.CONFLICT_REPLACE);
    	
    	Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
       db = new GroupMessengerDB(getContext());
    	return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
      
    	
    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(GroupMessengerTable.TAB_GRPMSG);
		
    	SQLiteDatabase qmdb = db.getReadableDatabase();
		
    	
    	Cursor cursor = queryBuilder.query(qmdb, null, GroupMessengerTable.TAB_GRPMSG +"."+GroupMessengerTable.COL_KEY+"='"+selection+"'", null, null, null, null);
	    cursor.setNotificationUri(getContext().getContentResolver(), uri);

    	Log.v("query", selection);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }
}
