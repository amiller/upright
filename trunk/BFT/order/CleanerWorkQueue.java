package BFT.order;

import java.util.concurrent.*;

import BFT.Debug;
import BFT.Parameters;

import BFT.order.messages.*;
import BFT.order.statemanagement.CheckPointState;
import BFT.order.statemanagement.RequestQueue;
import BFT.messages.*;
import BFT.network.concurrentNet.*;
import BFT.util.*;
import BFT.order.OrderBaseNode;

//public class CleanerWorkQueue extends OrderNetworkWorkQueue implements NBLogQueue, CPQueue, RequestCoreQueue {
public class CleanerWorkQueue extends NetworkWorkQueue{

    private ArrayBlockingQueue<SignedRequestCore> requestCoreQueue = null;
    private ArrayBlockingQueue<PrePrepare> prePrepareQueue = null;
    
    private OrderBaseNode osn;
    protected RequestQueue[] filteredWorkQueue;

    private int cleanerCount = 0;
	
    public static final int byteSizeThreshold = 60000;

    public CleanerWorkQueue() {
	requestCoreQueue = new ArrayBlockingQueue<SignedRequestCore>(1024);
	prePrepareQueue = new ArrayBlockingQueue<PrePrepare>(1024);
	filteredWorkQueue = new RequestQueue[Parameters.getFilterCount()];
	for (int i = 0; i < filteredWorkQueue.length; i++)
	    filteredWorkQueue[i] = new RequestQueue();

    }

    public void setOrderBaseNode(OrderBaseNode obn){osn = obn;}


    

    public synchronized void hasCleanerWork() {
	boolean waited = false;
	long start = System.currentTimeMillis();
	while(!hasFilteredWork() &&
	      cleanerCount == 0
	       && !hasNetWork(Role.CLIENT)) {
	    waited = true;
	    try {
		//		System.out.println("waiting on cleanerwork");
		wait(10000);
		//		System.out.println("woke up with: "+hasNetWork(Role.CLIENT)+" "+hasNetWork(Role.FILTER)+" "+ hasFilteredWork() +" "+cleanerCount);
	    } catch (InterruptedException e) { 
		System.out.println("interrupted");
	    }
	}
	long end = System.currentTimeMillis();
	//if(waited && (end - start > 100)) //System.out.println("Waited in CLENER for " + (end - start));
    }

    protected synchronized void announceFilteredWork(){
	notifyAll();
    }


    public boolean hasFilteredWork(){
	for (int i = 0; i < filteredWorkQueue.length; i++)
	    if (filteredWorkQueue[i].size() >0)
		return true;
	return false;
    }

    protected synchronized void announceCleanerWork() {
	cleanerCount++;
	//		BFT.Debug.printQueue("ADD CLEAN " + cleanerCount);
	notifyAll();
    }

    protected synchronized void tookCleanerWork() {
	cleanerCount--;
	//		BFT.Debug.printQueue("REM CLEAN " + cleanerCount);
    }



    public void addWork(SignedRequestCore rc) {
	this.announceCleanerWork();
	if (!requestCoreQueue.add(rc)) this.tookCleanerWork();
    }

    public void addWork(PrePrepare pp) {
	this.announceCleanerWork();
	if (!prePrepareQueue.add(pp)) this.tookCleanerWork();
    }

    public PrePrepare getPrePrepareWork() {
	PrePrepare retPP = null;
	retPP = prePrepareQueue.poll();
	if(retPP != null) this.tookCleanerWork();
	return retPP;
    }

    public SignedRequestCore getRequestCoreWork() {
	SignedRequestCore retRC = null;
	retRC = requestCoreQueue.poll();
	if(retRC != null) this.tookCleanerWork();
	return retRC;
    }



    /* (non-Javadoc)
     * @see BFT.network.concurrentNet.NetworkWorkQueue#addWork(BFT.util.Role, int, byte[])
     */
    @Override
	public void addWork(Role role, int index, byte[] work) {
	if(role != Role.FILTER) {
	    super.addWork(role, index, work);
	}
	else {
	    //	    System.out.println("adding filter work");
	    FilteredRequest req = new FilteredRequest(work);
	    if (!OrderBaseNode.obn.validateFilterMacArrayMessage(req))
		BFT.Debug.kill("FUCK ME");
	    for (int i = 0; i < req.getCore().length; i++){
		//this.announceFilteredWork();
		if (filteredWorkQueue[index].add(req.getCore()[i]))
		    this.announceFilteredWork();

// 		if (!filteredWorkQueue[index].add(req.getCore()[i])) {
// 		    //		    this.tookFilteredWork();
// 		    //System.out.println("tried to add filter work and failed");
// 		}
// 		else this.announceFilteredWork();
	    }
	}
    }


    public FilteredRequestCore getFilteredWork2(int index){
	FilteredRequestCore req = (FilteredRequestCore) filteredWorkQueue[index].poll();
	if (req != null) req.setSendingReplica(index);
	//	if (req != null) this.tookFilteredWork();
	return req;
    }

    public FilteredRequestCore getFilteredWorkRR2(){
	int index = filterIndex;
	FilteredRequestCore retBytes = getFilteredWork2(filterIndex);
	filterIndex++;
	if (filterIndex >= BFT.Parameters.getFilterCount()) filterIndex = 0;
	while (retBytes == null && filterIndex != index){
	    retBytes = getFilteredWork2(filterIndex);
	    filterIndex++;
	    if(filterIndex >= BFT.Parameters.getFilterCount()) {
		filterIndex = 0;
	    }
	}
	return retBytes;
    }

	
}
