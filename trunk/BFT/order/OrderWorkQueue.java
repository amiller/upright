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

//public class OrderWorkQueue extends OrderNetworkWorkQueue implements NBLogQueue, CPQueue, RequestCoreQueue {
public class OrderWorkQueue extends NetworkWorkQueue implements NBLogQueue, CPQueue, RequestCoreQueue, OrderNetworkWorkQueue {

    private ArrayBlockingQueue<SignedRequestCore> requestCoreQueue = null;
    private ArrayBlockingQueue<PrePrepare> prePrepareQueue = null;
    //private ArrayBlockingQueue<RequestCore> cleanRequestCoreQueue = null;
    private BFT.order.statemanagement.RequestQueue cleanRequestCoreQueue = null;
    private ArrayBlockingQueue<PrePrepare> cleanPrePrepareQueue = null;
    private ArrayBlockingQueue<NBLogWrapper> nbQueueA = null;
    private ArrayBlockingQueue<NBLogWrapper> nbQueueB = null;
    private ArrayBlockingQueue<CheckPointState> cpQueue = null;
    private ArrayBlockingQueue<PrePrepare> cleanedPPs = null;
    private ArrayBlockingQueue<SignedRequestCore> cleanedRCs= null;
    private long lastBatchTime;
    
    private OrderBaseNode osn;
    protected RequestQueue[] filteredWorkQueue;

    private int workerCountPP = 0;
    private int workerCountRC = 0;
    private int workerCountCP = 0;
    private int cleanerCount = 0;
	
    public static final int byteSizeThreshold = 60000;

    public OrderWorkQueue() {
	requestCoreQueue = new ArrayBlockingQueue<SignedRequestCore>(1024);
	prePrepareQueue = new ArrayBlockingQueue<PrePrepare>(1024);
	cleanRequestCoreQueue = //new ArrayBlockingQueue<RequestCore>(1024);
	    new BFT.order.statemanagement.RequestQueue();
	cleanPrePrepareQueue = new ArrayBlockingQueue<PrePrepare>(1024);
	nbQueueA = new ArrayBlockingQueue<NBLogWrapper>(1024, true);
	nbQueueB = new ArrayBlockingQueue<NBLogWrapper>(1024, true);
	cpQueue = new ArrayBlockingQueue<CheckPointState>(1024);
	cleanedPPs = new ArrayBlockingQueue<PrePrepare>(1024);
	cleanedRCs = new ArrayBlockingQueue<SignedRequestCore>(1024);
	filteredWorkQueue = new RequestQueue[Parameters.getFilterCount()];
	for (int i = 0; i < filteredWorkQueue.length; i++)
	    filteredWorkQueue[i] = new RequestQueue();

    }

    public void setOrderBaseNode(OrderBaseNode obn){osn = obn;}

    public synchronized void hasWorkerWork() {
	boolean waited = false;
	long start = System.currentTimeMillis();
	while(!hasNetWork(Role.ORDER) && !hasNetWork(Role.EXEC)
	      && workerCountPP == 0 
	      //&& cleanRequestCoreQueue.size() < osn.minimumBatchSize()
	      && workerCountRC < osn.minimumBatchSize()  
	      && workerCountCP == 0) {
	    waited = true;
	    try {
		//			    System.out.println("wait on hasworkerwork");
		wait(10000);
// 		System.out.println("hem"+workerCountPP+" "+workerCountRC+" "+
// 				   workerCountCP + " "+osn.minimumBatchSize()+ " "+hasNetWork(Role.ORDER) +" "+
// 				   hasNetWork(Role.EXEC) +" : "+hasNetWork(Role.CLIENT)+"  "+hasNetWork(Role.FILTER));

				
	    } catch (InterruptedException e) {
	    }
	}
	// 		if (!waited){
	// 		System.out.println("boo "+workerCountPP+" "+workerCountRC+" "+
	// 				   workerCountCP);
	// 		}
	//	long end = System.currentTimeMillis();
	//if(waited && (end - start > 100)) //System.out.println("Waited in WORKER for " + (end - start));
    }

    public boolean hasCPWork(){
	return workerCountCP > 0;
    }
    public boolean hasCleanPrePrepare(){
	return workerCountPP > 0;
    }
    public boolean hasRequestCores(){
	//return  cleanRequestCoreQueue.size() >= osn.minimumBatchSize();
	return workerCountRC >= osn.minimumBatchSize();
    }


    protected synchronized void announceWorkerWork(int arg) {
	switch(arg) {
	case 0: workerCountPP++; break;
	case 1: workerCountRC++; break;
	case 2: workerCountCP++; break;
	}
	//		BFT.Debug.printQueue("ADD WORK " + workerCountPP + " " + workerCountRC + " " + workerCountCP);
	notifyAll();
    }

    protected synchronized void tookWorkerWork(int arg) {
	switch(arg) {
	case 0: workerCountPP--; break;
	case 1: workerCountRC--; break;
	case 2: workerCountCP--; break;
	}
	//		BFT.Debug.printQueue("REM WORK " + workerCountPP + " " + workerCountRC + " " + workerCountCP);
	//	notifyAll();
    }

    public synchronized void hasCleanerWork() {
	boolean waited = false;
	long start = System.currentTimeMillis();
	while(!hasNetWork(Role.CLIENT) 
	      && !hasNetWork(Role.FILTER) && 
	      cleanerCount == 0) {
	    waited = true;
	    try {
		//			    System.out.println("waiting on cleanerwork");
		wait(10000);
	    } catch (InterruptedException e) {}
	}
	long end = System.currentTimeMillis();
	//if(waited && (end - start > 100)) //System.out.println("Waited in CLENER for " + (end - start));
    }

    protected synchronized void announceCleanerWork() {
	cleanerCount++;
	//		BFT.Debug.printQueue("ADD CLEAN " + cleanerCount);
	notifyAll();
    }

    protected synchronized void tookCleanerWork() {
	cleanerCount--;
	//		BFT.Debug.printQueue("REM CLEAN " + cleanerCount);
	//	notifyAll();
    }

    public void addWork(SignedRequestCore rc) {
	if (requestCoreQueue.add(rc)) 
	    this.announceCleanerWork();
    }

    public void addWork(PrePrepare pp) {
	if (prePrepareQueue.add(pp)) this.announceCleanerWork();
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

    public int getCleanRCCount(){
	return workerCountRC;
    }

    public void addCleanWork(RequestCore rc) {
	//	this.announceWorkerWork(1);
	if(cleanRequestCoreQueue.add(rc))
	    this.announceWorkerWork(1);
	//System.out.println("worker count is now: "+workerCountRC);
    }

    public void addCleanWork(PrePrepare pp) {
	if(cleanPrePrepareQueue.add(pp)) this.announceWorkerWork(0);
    }

    public PrePrepare getCleanPrePrepareWork() {
	PrePrepare retPP = null;
	retPP = cleanPrePrepareQueue.poll();
	if(retPP != null) this.tookWorkerWork(0);
	return retPP;
    }

    public RequestCore getCleanRequestCoreWork() {
	RequestCore retRC = null;
	retRC = (RequestCore)cleanRequestCoreQueue.poll();
	// retRC = (RequestCore) cleanRequestCoreQueue.poll();
	if(retRC != null) this.tookWorkerWork(1);
	return retRC;
    }

    /* (non-Javadoc)
     * @see BFT.order.NBLogQueue#addWork(int, BFT.order.NBLogWrapper)
     */
    public void addWork(int num, NBLogWrapper nb) {
	switch(num) {
	case 0: 
	    //			BFT.Debug.printQueue("NBQA ADD " + nbQueueA.size()); 
	    nbQueueA.add(nb); 
	    break;
	case 1: 
	    //			BFT.Debug.printQueue("NBQB ADD " + nbQueueB.size()); 
	    nbQueueB.add(nb); 
	    break;
	default:
	    BFT.Debug.kill(new RuntimeException("Unsupported queue id"));
	}
    }

    /* (non-Javadoc)
     * @see BFT.order.NBLogQueue#getNBWork(int)
     */
    public NBLogWrapper getNBWork(int num, boolean block) {
	NBLogWrapper retNB = null;
	try {
	    switch(num) {
	    case 0: 
		retNB = nbQueueA.poll(block?20000:1, TimeUnit.MILLISECONDS); 
		//				BFT.Debug.printQueue("NBQA REM " + nbQueueA.size());
		break;
	    case 1: 
		retNB = nbQueueB.poll(block?20000:1, TimeUnit.MILLISECONDS); 
		//				BFT.Debug.printQueue("NBQB REM " + nbQueueB.size());
		break;
	    default: 
		BFT.Debug.kill(new RuntimeException("Unsupported queue id"));
	    }
	} catch (InterruptedException e) {
	}
	return retNB;
    }

    /* (non-Javadoc)
     * @see BFT.order.CPQueue#addWork(BFT.order.statemanagement.CheckPointState)
     */
    public void addWork(CheckPointState cp) {
	if (cpQueue.add(cp)) this.announceWorkerWork(2);
    }

    /* (non-Javadoc)
     * @see BFT.order.CPQueue#getCPWork()
     */
    public CheckPointState getCPWork() {
	CheckPointState retCP = null;
	retCP = cpQueue.poll();
	if(retCP != null) this.tookWorkerWork(2);
	return retCP;
    }

    public void cleanerDone(PrePrepare pp) {
	if (cleanedPPs.add(pp)) this.announceCleanerWork();
    }

    public void cleanerDone(SignedRequestCore rc) {
	if (cleanedRCs.add(rc)) this.announceCleanerWork();
    }

    public PrePrepare getDonePP() {
	PrePrepare retPP = null;
	retPP = cleanedPPs.poll();
	if(retPP != null) tookCleanerWork();
	return retPP;
    }

    public SignedRequestCore getDoneRC() {
	SignedRequestCore retRC = null;
	retRC = cleanedRCs.poll();
	if(retRC != null) tookCleanerWork();
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
	    FilteredRequest req = new FilteredRequest(work);
	    if (!OrderBaseNode.obn.validateFilterMacArrayMessage(req))
		BFT.Debug.kill("FUCK ME");
	    for (int i = 0; i < req.getCore().length; i++){
		if (filteredWorkQueue[index].add(req.getCore()[i])) 
		    this.announceNetWork(Role.FILTER);
	    }
	}
    }

    public FilteredRequestCore getFilteredWork2(int index){
	FilteredRequestCore req = (FilteredRequestCore) filteredWorkQueue[index].poll();
	if (req != null) req.setSendingReplica(index);
	if (req != null) this.tookNetWork(Role.FILTER);
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

    public int getByteSize() {return 0;}
	
    protected boolean hasRCWork() {
	boolean retVal = false;
	long current = System.currentTimeMillis();
	if(cleanRequestCoreQueue.byteSize() > byteSizeThreshold || current - lastBatchTime > 100) {
	    retVal = true;
	}
	return retVal;
    }
	
    public void resetTime() {
	lastBatchTime = System.currentTimeMillis(); 
    }
	
}
