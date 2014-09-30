package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;
import java.security.NoSuchAlgorithmException;

public class NodeMap {

	public Node nodeList = null;
	Node headNode;
	Node currentNode;
	Node newNode;
	static boolean doneFlag = false;
	
	public void returnJoin(String reqPort){
	
		try{
	           
			/*check if the node is the first to join*/
			if(nodeList == null){
		       	Log.v("NODELIST", "Handling the frst member ==>" + reqPort);
		    	headNode = new Node(reqPort,new SimpleDynamoProvider().genHash(reqPort));
		    	nodeList = headNode;
		    	nodeList.pred = headNode;
		    	nodeList.succ = headNode;
		    }
			else{
		        // if the new node is the second to join no checks needed
			    nodeList = headNode;
			    newNode = new Node(reqPort,new SimpleDynamoProvider().genHash(reqPort));
			   
			    do{
				    if(nodeList.succ == headNode && nodeList.pred == headNode && doneFlag == false){
				           			
				    	newNode.succ = nodeList;
				    	newNode.pred = nodeList;
				    	nodeList.succ = newNode;
				    	nodeList.pred = newNode;
				    	doneFlag = true;
				    	break;
				           			
			        }else if(nodeList.getNodeKey().compareTo(nodeList.pred.getNodeKey()) < 0){
				           		
			         		if(newNode.getNodeKey().compareTo(nodeList.pred.getNodeKey()) > 0 || newNode.getNodeKey().compareTo(nodeList.getNodeKey()) <= 0){
			           				
			           				newNode.pred = nodeList.pred;
			           				newNode.succ = nodeList;
			           				nodeList.pred.succ = newNode;
			           				nodeList.pred = newNode;
			           				break;
				       		}
			         }else{
			           		if(nodeList.getNodeKey().compareTo(newNode.getNodeKey()) >= 0 && nodeList.pred.getNodeKey().compareTo(newNode.getNodeKey()) < 0){
			           		   			
			           				newNode.pred = nodeList.pred;
			           				newNode.succ = nodeList;
			           				nodeList.pred.succ = newNode;
			           				nodeList.pred = newNode;
			           				break;
			           		}	
		           	  }
				    nodeList = nodeList.succ;
			    }while(nodeList!=headNode);    		
			}
			
			getList(headNode);
		}catch (NoSuchAlgorithmException e) {
		 			// TODO Auto-generated catch block
		 			e.printStackTrace();
		}	
	}
	
	
	public void getList(Node current){
		System.out.println("till now :");
		do{
			
			System.out.println(current.getAssocPort() + "= >");
			current = current.succ;
		}while(current != headNode);
	}
	
	public Node getNodeList(){
		return nodeList;
	}
	
}		


class Node{
	
	private String nodeKey;
	private String assocPort;
	Node succ = null;
	Node pred = null;
	
	Node(String assocPort,String nodeKey){
		this.setNodeKey(nodeKey);
		this.setAssocPort(assocPort);
	}
	
	public String getNodeKey() {
		return nodeKey;
	}


	public void setNodeKey(String nodeKey) {
		this.nodeKey = nodeKey;
	}
	
	public String getAssocPort() {
		return assocPort;
	}
	
	public void setAssocPort(String assocPort) {
		this.assocPort = assocPort;
	}    		
}

