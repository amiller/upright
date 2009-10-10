/**
 * $Id$
 */
package BFT.order;

import java.util.*;


import BFT.Debug;
import BFT.order.*;
import BFT.messages.*;
import BFT.order.messages.*;
import BFT.network.MessageHandler;
import BFT.network.concurrentNet.NetQueue;
import BFT.network.concurrentNet.RPChooser;
import BFT.network.concurrentNet.ReadPredicate;
import BFT.util.*;
import BFT.order.statemanagement.*;

/**
 * @author riche
 *
 */
public class Worker implements Runnable {

    NetQueue netQueue = null;
    OrderWorkQueue orderMsgQueue = null;
    OrderBaseNode protocolHandler = null;
    ReadPredicate pred = null;

    int baseRCThreshold, rcThreshold;

    public Worker(NetQueue netQueue, OrderWorkQueue orderMsgQueue, OrderBaseNode protocolHandler) {
	this.netQueue = netQueue;
	this.orderMsgQueue = orderMsgQueue;
	this.protocolHandler = protocolHandler;
	this.pred = new ReadPredicate(orderMsgQueue, protocolHandler);
	//		baseRCThreshold = BFT.Parameters.getNumberOfClients() / 4;
	baseRCThreshold = BFT.Parameters.getNumberOfClients();
	rcThreshold = baseRCThreshold;
    }


    protected int threshold(int count){
	//	return 1;
	return protocolHandler.minimumBatchSize();

// 	if (count > baseRCThreshold /2)
// 	    return count;
// 	else if (count <= 0)
// 	    return 0;
// 	else 
// 	    return baseRCThreshold/2;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
	byte[] bytesRead = null;
	PrePrepare ppRead = null;
	RequestCore[] rcsRead = null;
	CheckPointState cps = null;
	int thresholdSize = 60000;
	int count = 0;
	int loopcount=1;
	long blocking = 0;
	long gettingWork = 0;
	long start = 0;
	long finish = 0;
	long startWork = System.nanoTime();
	long finishWork = 0;
	long totalWork = 0;
	boolean spin = false;
	long spincount = 0;
	while(true) {
// 	    if (loopcount %10000 == 0){
// 		finishWork = System.nanoTime();
// 		totalWork += finishWork - startWork;
// 		System.out.println("Total work: "+totalWork+
// 				   "\n  blocking: "+blocking+ " "+((float)blocking/(float)totalWork)+
// 		    "\n   getting: "+gettingWork+ " "+ ((float)gettingWork/(float)totalWork));
// 		System.out.println("spin count: "+spincount+"/"+loopcount);
// 		totalWork = 0; blocking=0; gettingWork = 0;
// 		start = System.nanoTime();
// 		spincount = 0;
// 		loopcount = 0;
// 	    }
// 	    spin = true;
// 	    loopcount++;
//	    start = System.nanoTime();
	    orderMsgQueue.hasWorkerWork();
	    //	    finish = System.nanoTime();
	    //	    blocking += finish-start;


	    if (pred.predicate(RPChooser.CP_LOG_DONE)
		&& orderMsgQueue.hasCPWork()) {
		//		start = System.nanoTime();
		cps = orderMsgQueue.getCPWork();
		//		finish = System.nanoTime();
		//		gettingWork += finish-start;
		if(cps != null) {
		    //		    spin = false;
		    Debug.profileStart("CPLOG");
		    protocolHandler.cpStable(cps);
		    Debug.profileFinis("CPLOG");
		    continue;
		}
		//		if (spin) System.out.println("spin cpwork");
	    }
	    if (pred.predicate(RPChooser.CLEAN_PP)
		&& orderMsgQueue.hasCleanPrePrepare()) {
		//		start = System.nanoTime();
		ppRead = orderMsgQueue.getCleanPrePrepareWork();
		//		finish = System.nanoTime();
		//		gettingWork += finish-start;
		if (ppRead != null) {
		    //		    spin = false;
		    Debug.profileStart("CLEANEDPP");
		    protocolHandler.handle(ppRead);
		    Debug.profileFinis("CLEANEDPP");
		    ppRead = null;
		    //continue;
		}
		//		if (spin)System.out.println("spin clean pp");
	    }
	    if (pred.predicate(RPChooser.CLEAN_RC)
		&& orderMsgQueue.hasRequestCores()) {
		if (orderMsgQueue.getCleanRCCount() >= threshold(rcThreshold--)) {
		    
		    
		    // HASHTABLE!!!!!!
		    
		    Hashtable<Integer, RequestCore> rcsReadTemp = new Hashtable<Integer, RequestCore>();
		    //		    ArrayList<RequestCore> rcsReadTemp = new ArrayList<RequestCore>(BFT.Parameters.getNumberOfClients());
		    int size = 0;
		    Debug.profileStart("EXTRACT_CLEAN");
		    while (size < OrderWorkQueue.byteSizeThreshold) {
			//			finish = System.nanoTime();
			RequestCore temp = null;
			//			start = System.nanoTime();
			temp = orderMsgQueue.getCleanRequestCoreWork();
			//			finish = System.nanoTime();
			//			gettingWork += finish-start;
			if (temp == null) {
			    break;
			} else if (protocolHandler.isNextClientRequest(temp)) {
			    RequestCore tmprc = rcsReadTemp.get(temp
								.getSendingClient());
			    if (tmprc == null
				|| tmprc.getRequestId() < temp.getRequestId()) {
				rcsReadTemp.put(temp.getSendingClient(), temp);
				//rcsReadTemp.add(temp);
				size += temp.getTotalSize()
				    - ((tmprc != null) ? tmprc
				       .getTotalSize() : 0);
				//								if (tmprc != null){
				//									System.out.println("replacing an rc");
				//								}
			    } else {
				//System.out.println("discaring an rc because it was already there or something");
				//System.out.println(tmprc);
				//System.out.println(temp);
			    }
			} else {
			    //System.out.println("DISCARDING A RC b/c its not next");
			    //System.out.println(temp);
			}
		    }
		    //System.out.println("got an array: " + rcsReadTemp.size());
		    if (!rcsReadTemp.isEmpty()) {
			rcThreshold = baseRCThreshold;
			Debug.profileStart("RC_SET");
			rcsRead = new RequestCore[rcsReadTemp.size()];
			rcsRead = rcsReadTemp.values().toArray(rcsRead);
			Debug.profileStart("handle_RC");
			//System.out.println("about to call handle");
			protocolHandler.handle(rcsRead);
			//			spin = false;
			//System.out.println("called handle");
			Debug.profileFinis("handle_RC");
			Debug.profileFinis("RC_SET");
			rcsRead = null;
			//continue;
		    }
		    Debug.profileFinis("EXTRACT_CLEAN");
		}else if (rcThreshold < 0)
		    rcThreshold = 0;
		//		if (spin)System.out.println("spin rc "+orderMsgQueue.getCleanRCCount());
	    }

	    while (pred.predicate(RPChooser.ORDER_BYTES) 
		   && orderMsgQueue.hasNetWork(Role.ORDER)
		   && count < BFT.Parameters.getOrderCount()) {
		//		start = System.nanoTime();
		bytesRead = orderMsgQueue.getWorkRR(Role.ORDER);
		//		finish = System.nanoTime();
		//		gettingWork += finish-start;
		if (bytesRead != null) {
		    Debug.profileStart("ORDER_BYTES");
		    protocolHandler.handle(bytesRead);
		    //		    spin = false;
		    Debug.profileFinis("ORDER_BYTES");
		    bytesRead = null;
		    //continue;
		}
		//		if (spin) System.out.println("spin order");
	    }
	    if (pred.predicate(RPChooser.EXEC_BYTES) 
		&& orderMsgQueue.hasNetWork(Role.EXEC)) {
		//		start = System.nanoTime();
		bytesRead = orderMsgQueue.getWorkRR(Role.EXEC);
		//		finish = System.nanoTime();
		//		gettingWork += finish-start;
		if (bytesRead != null) {
		    Debug.profileStart("EXEC_BYTES");
		    protocolHandler.handle(bytesRead);
		    //		    spin = false;
		    Debug.profileFinis("EXEC_BYTES");
		    bytesRead = null;
		    //continue;
		}
		//		if (spin)System.out.println("spin exec bytes");
	    }
	    protocolHandler.checkHeartBeat();
	    //	    if (spin)
	    //		spincount++;
	}
    }

}