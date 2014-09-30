package edu.buffalo.cse.cse486586.simpledht;

import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.content.ContentResolver;
import android.database.Cursor;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

	OnTestClickListener tl = new OnTestClickListener(null, null);
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }
/*
	public void LDump(View v) {
			ContentResolver conRes = getContentResolver();
			Uri mUri =tl.mUri;
			String[] selArgs = { "ldump" };
			Cursor resultCursor = conRes.query(mUri, null, "@", selArgs, null);
			int keyIndex = resultCursor.getColumnIndex("key");
			int valueIndex = resultCursor.getColumnIndex("value");
	
			resultCursor.moveToFirst();
			while (!resultCursor.isAfterLast()) {
				final String key = resultCursor.getString(keyIndex);
				final String value = resultCursor.getString(valueIndex);
				Log.v("Cr : ", "L C" + key +value);
	
				resultCursor.moveToNext();
			}
			
			resultCursor.close();
		}
	
		// GDump
		public void GDump(View v) {
			ContentResolver conRes = getContentResolver();
			Uri mUri = tl.mUri;
			String[] selArgs = { "gdump" };
			Cursor resultCursor = conRes.query(mUri, null, "*", selArgs, null);
			int keyIndex = resultCursor.getColumnIndex("key");
			int valueIndex = resultCursor.getColumnIndex("value");
			resultCursor.moveToFirst();
			while (!resultCursor.isAfterLast()) {
				final String key = resultCursor.getString(keyIndex);
				final String value = resultCursor.getString(valueIndex);
				Log.v("Cr : ", "G C" + key +value);
				
	
				resultCursor.moveToNext();
			}
			resultCursor.close();
		}
*/
}
