package edu.buffalo.cse.cse486586.groupmessenger;

import java.util.Comparator;
import java.util.PriorityQueue;







public class Causal {

	static int expectedKey = 0;
	static String myPort;
	//to compare key while inserting into the min heap!!
	Comparator<MyMessage> keyComp = new KeyComparator();
	
	//Que having the min item to publish 
	PriorityQueue<MyMessage> holdBackQueue = new PriorityQueue<MyMessage>(20, keyComp);
	
	protected MyMessage checkCausal(MyMessage nodeMessage)
	{
		holdBackQueue.add(nodeMessage);
		if(expectedKey ==  Integer.parseInt(nodeMessage.getKey())){
			if(holdBackQueue.isEmpty()!=true){
				expectedKey = expectedKey + 1;
				return(holdBackQueue.poll());
			}
		}
		return null;
	}
		
	
	protected class KeyComparator implements Comparator<MyMessage>
	{
		@Override
	    public int compare(MyMessage m1, MyMessage m2)
	    {
	        // Assume neither int is stupid. 
	        Integer x = Integer.parseInt(m1.getKey());
	        Integer y = Integer.parseInt(m2.getKey());

			if (x < y)
	        {
	            return -1;
	        }
	        if (x > y)
	        {
	            return 1;
	        }
	        return 0;
	    }	
	
}
	
}
	
	
	
	