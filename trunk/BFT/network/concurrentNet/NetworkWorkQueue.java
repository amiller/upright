// $Id$

/**
 * 
 */
package BFT.network.concurrentNet;

import java.util.concurrent.*;
import java.util.*;
//import BFT.membership.*;
import BFT.messages.ClientRequest;
import BFT.messages.VerifiedMessageBase;
import BFT.order.MessageFactory;
import BFT.messages.*;
import BFT.util.*;
import BFT.Parameters;

/**
 * @author riche
 *
 */
public class NetworkWorkQueue implements NetQueue {

     protected ArrayList<ArrayBlockingQueue<byte[]>> filters;
     protected ArrayList<ArrayBlockingQueue<byte[]>> orders;
     protected ArrayList<ArrayBlockingQueue<byte[]>> execs;
     protected ArrayList<ArrayBlockingQueue<byte[]>> clients;

    protected int filterIndex = 0;
    protected int orderIndex = 0;
    protected int execIndex = 0;
    protected int clientIndex = 0;
    
    protected int clientCount = 0;
    protected int orderCount = 0;
    protected int filterCount = 0;
    protected int execCount = 0;
    
    public NetworkWorkQueue() {
	filters = new ArrayList<ArrayBlockingQueue<byte[]>>(BFT.Parameters.getFilterCount());
	orders = new ArrayList<ArrayBlockingQueue<byte[]>>(BFT.Parameters.getOrderCount());
	execs = new ArrayList<ArrayBlockingQueue<byte[]>>(BFT.Parameters.getExecutionCount());
	clients = new ArrayList<ArrayBlockingQueue<byte[]>>(1);
	RoleMap.initialize(BFT.Parameters.getNumberOfClients(),
			   BFT.Parameters.getFilterCount(),
				   BFT.Parameters.getOrderCount(),
			   BFT.Parameters.getExecutionCount());
    }
	
	public final synchronized void announceNetWork(Role role) {
		switch(role) {
		case CLIENT: clientCount++; break;
		case ORDER: orderCount++; break;
		case EXEC: execCount++; break;
		case FILTER: filterCount++; break;
		default: break;
		}
		//		BFT.Debug.printQueue("ADDING "  + role.toString() + " "+ clientCount + 
		//" " + orderCount + " " + execCount);
		notifyAll();
	}
	
	public final boolean hasNetWork(Role role) {
		boolean retVal = false;
		switch(role) {
		case CLIENT: if(clientCount > 0) retVal = true; break;
		case ORDER: if(orderCount > 0) retVal = true; break;
		case EXEC: if(execCount > 0) retVal = true; break;
		case FILTER: if(filterCount > 0) retVal = true; break;
		default: break;
		}
		return retVal;
	}


    ArrayList<ArrayBlockingQueue<byte[]>> queues;
//     public final boolean hasNetWork(Role role){
// 	int count = 0;
// 	switch(role){
// 	case CLIENT: count = Parameters.getNumberOfClients(); 
// 	    queues = clients;
// 	    break;
// 	case ORDER: count = Parameters.getOrderCount(); 
// 	    queues = orders;
// 	    break;
// 	case EXEC:  count = Parameters.getExecutionCount(); 
// 	    queues = execs;
// 	    break;
// 	case FILTER: count = Parameters.getFilterCount(); 
// 	    queues = filters;
// 	    break;
// 	default: BFT.Debug.kill("Invalid role");
// 	}
// 	for (int i = 0; i < count && i < queues.size(); i++){
// 	    if (queues.get(i) != null && queues.get(i).size() >0 )
// 		return true;
// 	    else if (queues.get(i) == null)
// 		break;
// 	}
// 	return false;
//      }
//     // 	boolean retVal = false;
//     // 	switch(role) {
//     // 	case CLIENT: if(clientCount > 0) retVal = true; break;
//     // 	case ORDER: if(orderCount > 0) retVal = true; break;
//     // 	case EXEC: if(execCount > 0) retVal = true; break;
//     // 	case FILTER: if(filterCount > 0) retVal = true; break;
//     // 	default: break;
//     // 	}
//     // 	if (retVal) BFT.Debug.kill("the old work counting is broken");
//     // 	return false;
//     //     }
    
    public final synchronized void hasAnyNetWork() {
    	while(!hasNetWork(Role.ORDER) && !hasNetWork(Role.EXEC) 
	      && !hasNetWork(Role.CLIENT) && !hasNetWork(Role.FILTER)) {
	    try {
		wait(1000);
	    } catch (InterruptedException e) {
	    }
    	}
	
    }
    
    public final synchronized void tookNetWork(Role role) {
			switch(role) {
			case CLIENT: clientCount--; break;
			case ORDER: orderCount--; break;
			case EXEC: execCount--; break;
			case FILTER: filterCount--; break;
			default: break;
			}
			//		BFT.Debug.printQueue("REMOVE " + role.toString() + " "+ clientCount + " " + orderCount + " " + execCount);
// 			if(clientCount < 0 || orderCount < 0 || execCount < 0 || filterCount < 0) {
// 				BFT.Debug.kill("NEGATIVE");
// 			}
	// 		notifyAll();
	
    }
    
	/* (non-Javadoc)
	 * @see BFT.network.concurrentNet.NetQueue#addWork(BFT.util.Role, int, byte[])
	 */
    private Object lock = new String("");
    public void addWork(Role role, int index, byte[] work) {
	//	System.err.println("adding work for: "+role.toString()+index);
	//	VerifiedMessageBase vmb = MessageFactory.fromBytes(work);
	//	switch(vmb.getTag()){
	//	case MessageTags.SpeculativeNextBatch: SpeculativeNextBatch req = (SpeculativeNextBatch) vmb; System.err.println("AW(" + req.getSendingReplica() + ")"); break;
	//	default: break;
	//	}
	//    	ArrayBlockingQueue<byte[]> queue = queueTable.get(RoleMap.getRoleString(role, index));//role.toString() + index);

	ArrayBlockingQueue<byte[]> queue = selectQueue(role, index);
	
	
	if(queue == null) {
	    synchronized(lock){
        queue = selectQueue(role, index);
		if (queue == null){
		    int count = 0;
		    int queueSize = 0;
		    switch(role){
		    case FILTER :  
			queueSize = 2048;
			queues = filters;
			count = BFT.Parameters.getFilterCount(); 
			break;
		    case ORDER : 
			queueSize = 2048;
			queues = orders;
			count = BFT.Parameters.getOrderCount(); 
			break;
		    case EXEC : 
			queueSize = 512;
		    queues = execs;
		    count = BFT.Parameters.getExecutionCount(); break;
		    case CLIENT : 
			queueSize = BFT.Parameters.getNumberOfClients() *4;
			queues = clients;
			count = 1; break;//BFT.Parameters.getNumberOfClients(); break;
		    default : BFT.Debug.kill("Bad role value");
		    }
		     	    //System.out.println(role);
		     	    //System.out.println(index);
		     	    //System.out.println(queueSize);
		    for (int i = 0; i < count; i++)
			queues.add(i,new ArrayBlockingQueue<byte[]>(queueSize));
		    
		    //System.out.println("Added "+role+" queues");
		    queue = queues.get(index);
		    if (queue == null)
			BFT.Debug.kill("adding the queu should not fail");
		}
		lock.notifyAll();
	    }
	}
	if (queue.offer(work))
	    this.announceNetWork(role);
 	else{
		//this.tookNetWork(role); 
	    System.out.println("likely just had a queue reject adding a message from "+RoleMap.getRoleString(role, index)); 
	    System.out.println("\t queue size: "+queue.size());
	    System.out.println("end: "+clientCount+" "+filterCount+" "+orderCount+" "+execCount);
	}
	//	this.announceNetWork(role);
    }
	
    /* (non-Javadoc)
	 * @see BFT.network.concurrentNet.NetQueue#getWork(BFT.util.Role, int)
	 */
    public byte[] getWork(Role role, int index) {
	byte[] retBytes = null;
	ArrayBlockingQueue<byte[]> queue = selectQueue(role, index);
// 	synchronized(lock){
// 	    while (queue == null){
// 		lock.wait(10000);
// 	    }
// 	}
	if(queue == null) {
	    BFT.Debug.kill("Bad queue name"+role.toString()+index);
	}
	if(!queue.isEmpty()) {
	    retBytes = queue.poll();
	}
	if(retBytes != null) this.tookNetWork(role);

// 	if (clientCount > 100)
// 	    System.out.println("end: "+clientCount+" "+filterCount+" "+orderCount+" "+execCount);

	return retBytes;
		
    }
	
    /* (non-Javadoc)
	 * @see BFT.network.concurrentNet.NetQueue#getWorkRR(BFT.util.Role)
	 */
    public byte[] getWorkRR(Role role) {
	//	System.err.println("start: "+clientCount+" "+filterCount+" "+orderCount+" "+execCount);

	byte[] retBytes = null;
	//while(retBytes == null) {
	int index = 0;
	if(role == Role.ORDER) {
	    index = orderIndex;
	    retBytes = getWork(role, orderIndex);
	    orderIndex++; 
	    if(orderIndex >= BFT.Parameters.getOrderCount()) orderIndex = 0;
	    while (retBytes == null && orderIndex != index){
		retBytes = getWork(role, orderIndex);
		orderIndex++; 
		if(orderIndex >= BFT.Parameters.getOrderCount()) orderIndex = 0;
	    }
	}
	else if(role == Role.EXEC) {
	    index = execIndex;
	    retBytes = getWork(role, execIndex);
	    execIndex++;
	    if (execIndex >= BFT.Parameters.getExecutionCount()) execIndex = 0;
	    while (retBytes == null && execIndex != index){
		retBytes = getWork(role, execIndex);
		execIndex++;
		if(execIndex >= BFT.Parameters.getExecutionCount()) {
		    execIndex = 0;
		}
	    }
	}
	else if(role == Role.FILTER) {
	    index = filterIndex;
	    retBytes = getWork(role, filterIndex);
	    filterIndex++;
	    if (filterIndex >= BFT.Parameters.getFilterCount()) filterIndex = 0;
	    while (retBytes == null && filterIndex != index){
		retBytes = getWork(role, filterIndex);
		filterIndex++;
		if(filterIndex >= BFT.Parameters.getFilterCount()) {
		    filterIndex = 0;
		}
	    }
	}
	else if(role == Role.CLIENT) {
	    //index = clientIndex;
	    //if(clientIndex >= BFT.Parameters.getNumberOfClients()) {
	    //clientIndex = 0;
	    //}
	    index = 0;
	    retBytes = getWork(role, index);
	}
	//}

	//	System.err.println("end: "+clientCount+" "+filterCount+" "+orderCount+" "+execCount);
	return retBytes;
    }
	

    protected ArrayBlockingQueue<byte[]> selectQueue(Role role, int index){
	ArrayBlockingQueue<byte[]> queue=null;
	try{
	    switch(role){
	    case CLIENT:	    queue = clients.get(0);
		break;
	    case ORDER: 
		queue = orders.get(index);
		break;
	    case EXEC:  
		queue = execs.get(index);
		break;
	    case FILTER:
		queue = filters.get(index);
		break;
	    default: BFT.Debug.kill("Invalid role");
	    }
	}catch(Exception e){ queue = null;}
	return queue;
    }


    public static void main(String[] t){
	BFT.Parameters.toleratedOrderLiars = 1;
	BFT.Parameters.toleratedOrderCrashes = 1;
	BFT.Parameters.toleratedExecutionLiars = 1;
	BFT.Parameters.toleratedExecutionCrashes = 1;
	BFT.Parameters.toleratedFilterLiars = 1;
	BFT.Parameters.toleratedFilterCrashes = 1;
	BFT.Parameters.numberOfClients = 0;
	BFT.Parameters.filtered = true;

	NetworkWorkQueue net = new NetworkWorkQueue();
	long count = 10000000;
		
	Gate g = new Gate();
	Thread t1 = new Thread(new Producer(net, count, g));
	Thread t2 = new Thread(new Consumer(net, count, g));
	long start = System.currentTimeMillis();

	run(net, count);

	long finish = System.currentTimeMillis();
	double d = finish - start;
	System.out.println("req/time: "+ (count/d));

	start = finish;
	t1.start();
	t2.start();
	try{
	t1.join();
	t2.join();
	}catch(Exception e){}
	 finish = System.currentTimeMillis();
	  d = finish - start;
	 System.out.println("req/time: "+ (count/d));
    }

    static void run(NetworkWorkQueue tmp, long count){

	byte[] tmp2 = new byte[1024];
	long i = 0;
	int index=0;
	System.out.println("read: "+i+"/"+count);
	while (i < count){
	    for (int j=0; j< 4*BFT.Parameters.getFilterCount(); j++, i++)
		tmp.addWork(Role.FILTER, j%BFT.Parameters.getFilterCount(), tmp2);
	    for (int j=0; j< 4*BFT.Parameters.getOrderCount(); j++, i++)
		tmp.addWork(Role.ORDER, j%BFT.Parameters.getOrderCount(), tmp2);   
	    for (int j=0; j< 1*BFT.Parameters.getExecutionCount(); j++, i++)
		tmp.addWork(Role.EXEC, j%BFT.Parameters.getExecutionCount(), tmp2);

	    tmp.hasAnyNetWork();
	    while(tmp.hasNetWork(Role.CLIENT)){
		tmp.getWorkRR(Role.CLIENT);
	    }
	    while (tmp.hasNetWork(Role.FILTER)){
		tmp.getWorkRR(Role.FILTER);
	    }
	    while (tmp.hasNetWork(Role.ORDER)){
		tmp.getWorkRR(Role.ORDER);
	    }
	    while (tmp.hasNetWork(Role.EXEC)){
		tmp.getWorkRR(Role.EXEC);
	    }

	}
    }

    
}

class Producer implements Runnable{
    NetworkWorkQueue tmp;
    long count;
    Gate gate;
    public Producer(NetworkWorkQueue n, long c, Gate g){tmp = n; count = c;
	gate = g;
    }

    public void run(){
	byte[] tmp2 = new byte[1024];
	long i = 0;
	int index=0;
	while (i < count){
	    gate.setTop(i);
	    for (int j=0; j< 40*BFT.Parameters.getFilterCount(); j++, i++)
		tmp.addWork(Role.FILTER, j%BFT.Parameters.getFilterCount(), tmp2);
	    for (int j=0; j< 40*BFT.Parameters.getOrderCount(); j++, i++)
		tmp.addWork(Role.ORDER, j%BFT.Parameters.getOrderCount(), tmp2);   
	    for (int j=0; j< 10*BFT.Parameters.getExecutionCount(); j++, i++)
		tmp.addWork(Role.EXEC, j%BFT.Parameters.getExecutionCount(), tmp2);
	    
	}
    }
}

class Gate{
    
    long top = 0, bottom = 0;
    
    public synchronized void setTop(long t){
	top = t;
	while (top - bottom > 512){
	    try{
		wait(100);
	    }catch(Exception e){}
	}
    }
    public synchronized void setBottom(long b){
	bottom = b;
	notifyAll();
    }
}

class Consumer implements Runnable{

    NetworkWorkQueue tmp;
    long count;
    Gate gate;
    public Consumer(NetworkWorkQueue n, long c, Gate g){tmp = n; count = c;
	gate = g;}

    public void run(){

	byte[] tmp2 = new byte[1024];
	long i = 0;
	int index=0;
	System.out.println("read: "+i+"/"+count);
	while (i < count){
	    tmp.hasAnyNetWork();
	    while (tmp.hasNetWork(Role.CLIENT)){
		tmp.getWorkRR(Role.CLIENT); i++;
	    }
	    while (tmp.hasNetWork(Role.FILTER)){
		tmp.getWorkRR(Role.FILTER);
		i++;
	    }
	    while (tmp.hasNetWork(Role.ORDER)){
		tmp.getWorkRR(Role.ORDER);
		i++;
	    }
	    while (tmp.hasNetWork(Role.EXEC)){
		tmp.getWorkRR(Role.EXEC);
		i++;
	    }
	    gate.setBottom(i);
	}
    }
}


