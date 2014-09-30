package edu.buffalo.cse.cse486586.groupmessenger;

import java.util.LinkedList;
import java.util.Queue;


public class Sequencer{ 
	
	static int key=0;
	static String myPort;
	//Comparator<MessageObject> keyComp = new KeyComparator();
	//PriorityQueue<MessageObject> recvQue = new PriorityQueue<MessageObject>(20, keyComp);
	Queue<MyMessage> deliveryQue = new LinkedList<MyMessage>();
	
	//setting my msgs to delivery queue the i am ordering it and sending it to rest
	protected void getDelivery(MyMessage nodeMessage)
	{
		nodeMessage.setKey(Integer.toString(key));
		deliveryQue.add(nodeMessage);
		key= key + 1;
	}
	// polling msgs as TCP ensures FIFO!! still i'l check causal :P	
	protected MyMessage multicastMessage() {
		if(deliveryQue.isEmpty()!=true)
		{
			return(deliveryQue.poll());
		}
		return null;
		
	}

		
	
}
	
	
	
	
	
	
	