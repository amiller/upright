package BFT.serverShim;

import BFT.BaseNode;

import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Vector;
import java.lang.Thread;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.File;
import java.io.FileInputStream;

// outgoing messages
import BFT.messages.LastExecuted;
import BFT.messages.CPLoaded;
import BFT.messages.CPTokenMessage;
import BFT.messages.Reply;
import BFT.messages.WatchReply;
import BFT.messages.ReadOnlyReply;
import BFT.messages.BatchCompleted;
import BFT.messages.Entry;
import BFT.messages.FetchCommand;
import BFT.messages.CPUpdate;

// intra-shim
import BFT.serverShim.messages.FetchCPMessage;
import BFT.serverShim.messages.CPStateMessage;
import BFT.serverShim.messages.FetchState;
import BFT.serverShim.messages.AppState;

// incoming messages
import BFT.messages.NextBatch;
import BFT.messages.ReleaseCP;
import BFT.messages.LoadCPMessage;
import BFT.messages.Retransmit;
import BFT.messages.RequestCP;
import BFT.messages.ReadOnlyRequest;
import BFT.messages.ForwardCommand;
import BFT.messages.FetchDenied;

import BFT.messages.VerifiedMessageBase;
import BFT.messages.Digest;
import BFT.messages.HistoryDigest;

// shim node state
import BFT.serverShim.statemanagement.CheckPointState;
import BFT.serverShim.statemanagement.NextBatchCertificate;
import BFT.serverShim.MessageFactory;
import BFT.serverShim.messages.MessageTags;

import BFT.messages.Quorum;
import BFT.Debug;

public class ShimBaseNode extends BaseNode 
    implements ServerShimInterface{



    // Maintain one copy of the working state
    protected CheckPointState workingState;
    // maintain a collection of previous checkpoints.  indexed by the
    // first sequence number *not* to be included in the snapshot
    protected Hashtable<Long, CheckPointState> stateSnapshots;
    // set of sequence numbers at upcoming snapshots
    protected Vector<Long> maxValues;
    // index of the most recent stable snapshot
    //    protected int baseIndex;

    // set of next batch messages being gathered in quorums
    protected NextBatchCertificate[] batches;
    // index of the lowest indexed batch
    protected int baseBatch;
    protected long baseSeqNo;
    protected long maxExecuted;
    // the last batch to be passed off to the glue

    // quorums for retransmits
    protected Quorum<Retransmit>[] retrans;
    protected long[] early;
    protected int[] retransCount;
    // quorums for load cps
    protected Quorum<LoadCPMessage> loadCPQuorum;
    // quorum for release cp
    protected Quorum<ReleaseCP> releaseQuorum;

    // quorum indicating that you're ready for requests
    protected boolean readyForRequests;

    protected GlueShimInterface glue;
    public void setGlue(GlueShimInterface g) {glue = g;}


    protected CheckPointState initialCP;


    private ExecutorService pool = null;


    boolean loadingCP = false;
    
    public ShimBaseNode(String membership, int id,
			byte[] initialCPToken){
	super(membership, BFT.util.Role.EXEC, id);
	// do whatever else is necessary
	workingState = new CheckPointState(BFT.Parameters.getNumberOfClients());
	workingState.setMaxSequenceNumber(0);
	workingState.addCheckpoint(initialCPToken, 0);
	initialCP = workingState;
	initialCP.markStable();
	maxValues = new Vector<Long>();
	stateSnapshots = new Hashtable<Long, CheckPointState>();
	stateSnapshots.put(new Long(workingState.getSequenceNumber()),
			   workingState);
	workingState = new CheckPointState(workingState);
	batches = new NextBatchCertificate[BFT.order.Parameters.checkPointInterval * 3*(BFT.order.Parameters.maxPeriods)];
	for (int i = 0;i < batches.length; i++)
	    batches[i] = new NextBatchCertificate();
	//	baseIndex = 0;
	baseSeqNo = 0;
	maxExecuted = -1;

	retrans = new Quorum[BFT.Parameters.getNumberOfClients()];
	retransCount = new int[retrans.length];
	for (int i = 0;i < retrans.length; i++){
	    retrans[i] = new Quorum<Retransmit>(BFT.Parameters.getOrderCount(),
						BFT.Parameters.largeOrderQuorumSize(),
						0);
	    retransCount[i] = 0;
	}

	early = new long[BFT.Parameters.getOrderCount()];

	loadCPQuorum = new Quorum<LoadCPMessage>(BFT.Parameters.getOrderCount(),
						 BFT.Parameters.largeOrderQuorumSize(),
						 0);

	releaseQuorum  = new Quorum<ReleaseCP>(BFT.Parameters.getOrderCount(),
					       BFT.Parameters.largeOrderQuorumSize(),
					       0);

	pool = Executors.newCachedThreadPool();

	


	readyForRequests = true;
	
	
    }

    /**
       Process the nextbatch
    **/
    protected void process(NextBatch nb){
	//	if (fetchingstatenow >0)
	//Debug.println("Processing next batch"+nb.getSeqNo()+ "/"+baseSeqNo+
//		      " up to "+(baseSeqNo+batches.length) +" from "+
//		      nb.getSendingReplica()+" "+nb.getTag());
	if (!amIReadyForRequests() 
	    && nb.getSeqNo() < baseSeqNo +batches.length){ // reject a request b/c we're not ready
	    //	    Debug.println("Reject a request because we're"+
	    //			  " not yet ready for it");
	    return;
	}

	if (nb.getSeqNo() < baseSeqNo){
	    return;
	}
	if(!validateOrderMacArrayMessage(nb)) {
	    BFT.Debug.kill(new RuntimeException("FAILED VALIDATION! SN:" + nb.getSeqNo()));
	}
// 	if (fetchingstatenow>0)
// 	    System.out.println("\tprocessing that next batch");
	//Debug.println("Process next batch "+nb.getSeqNo()+ "/"+baseSeqNo+
	//	      " up to "+(baseSeqNo+batches.length) +" from "+
	//	      nb.getSendingReplica()+" with tag "+nb.getTag());




	int earlycount = 0;
	if (nb.getSeqNo() >= baseSeqNo + batches.length){
	    early[(int) (nb.getSendingReplica())] = nb.getSeqNo();
	    earlycount = 0;
	    for (int i = 0; i < early.length; i++)
		if (early[i]>=baseSeqNo + batches.length)
		    earlycount++;
	    if (earlycount >= BFT.Parameters.smallOrderQuorumSize()){
		if (!amIReadyForRequests()){
		    lastExecuted(maxExecuted+1);
		    for (int i = 0; i < early.length; i++)
			early[i] = 0;
		    earlycount = 0;
		}
// 		else{
// 		    lastExecuted(baseSeqNo);
// 		    for (int i = 0; i < early.length; i++)
// 			early[i] = 0;
// 		}
	    }
	    //	    return;
	}


	if (!amIReadyForRequests())
	    return;

// 	int index = (int) (nb.getSeqNo() - baseSeqNo);
// 	index = baseIndex + index;
// 	index = index % batches.length;
// 	int initIndex =index;
// 	boolean notcomplete = !batches[index].isComplete();

	// AGC:  7/11 -- now placing the nextbatch into the appropriate certificte no matter what
	int index = (int) (nb.getSeqNo() % batches.length);
	int initIndex = index;
	boolean notcomplete = !batches[index].isComplete();
	if (batches[index].previewNextBatch() != null &&
	    nb.getSeqNo() > batches[index].previewNextBatch().getSeqNo()){
// 	    System.out.println("wrapped around the nextbach list.  replacing "+
// 			       batches[index].previewNextBatch().getSeqNo()+
// 			       " with "+nb.getSeqNo());
	    batches[index].clear();

	    int plusone = (index+1) % batches.length;
	    if (batches[plusone].previewNextBatch() != null)
		;//		System.out.println("\tnext cp is: "+ batches[plusone].previewNextBatch().getSeqNo());
	    else
		;//System.out.println("\tthere is no next batch");
	    if (batches[plusone].previewNextBatch() != null &&
		nb.getSeqNo() == batches[plusone].previewNextBatch().getSeqNo()+batches.length-1
		&& nb.getSeqNo() - batches.length > baseSeqNo)
		lastExecuted(baseSeqNo);
	}
	batches[index].addNextBatch(nb);



	// try to roll back completeness if appropriate
	if (notcomplete && batches[index].isComplete()){

	    fetchCommands(batches[index], false);

	    long seq = nb.getSeqNo() - 1;
	    // if seq >= baseseqno, then push commit down as far as possible
	    if (seq >= baseSeqNo){
		//		System.out.println("need to commit "+seq+" first");

		NextBatchCertificate prv = batches[index];
		NextBatchCertificate tmp = prv;
		NextBatch old= null, nw = prv.getNextBatch();
		HistoryDigest hist = null;
		while (seq >= baseSeqNo && prv.isComplete()){
		    //		    System.out.println("trying out "+seq+" above "+baseSeqNo);

// 		    index = (int) (seq - baseSeqNo);
// 		    index = baseIndex + index;
// 		    index = index % batches.length;

		    index = (int) (seq % batches.length);

		    tmp = batches[index];
		    old = tmp.previewNextBatch();
		    if (old == null || tmp.isComplete())
			break;
		    hist = 
			new HistoryDigest(old.getHistory(),
					  nw.getCommands(),
					  nw.getNonDeterminism(),
					  nw.getCPDigest());
							   
							   
		    if (nw.getHistory().equals(hist)){
			//			System.out.println("frocing completed!");
			tmp.forceCompleted();
			fetchCommands(tmp, false);
		    }else{
// 			System.out.println("mismatch");
// 			System.out.println(nw.getHistory());
// 			System.out.println(hist);
			lastExecuted(baseSeqNo);
		    }
		    seq--;
		    nw = old;
		    prv = tmp;

		}

	    }

	}

	// if its ready for execution then try to execute.
	if (batches[initIndex].isReadyForExecution()){
	    //	    System.out.println(initIndex+"  is ready for execute");
	    if (!tryToExecute()){
		;//		System.out.println("failed to execute b/c we're missing an old message! "+baseIndex);
	    }
	}
	
	//	if (baseSeqNo > maxExecuted+batches.length){
	    //	    System.out.println("$$$$$$ last executed b/c baseseqno got huge "+
	    //		       baseSeqNo+" "+maxExecuted);
	//	    lastExecuted(maxExecuted+1);
	//	}

	
	

    }


    protected boolean tryToExecute(){
  // 	if (fetchingstatenow > 0)
	    
// 	    System.out.println(baseSeqNo+
// 			       " is ready for execute: "+
// 			       batches[(int)(baseSeqNo % batches.length)].isReadyForExecution());
	boolean res = false;
	int index = (int) (baseSeqNo % batches.length);
	while (batches[index].isReadyForExecution() && batches[index].getNextBatch().getSeqNo() == baseSeqNo){
	    acton(batches[index]);
	    batches[index].clear();
	    baseSeqNo += 1; // only updated on return res
	    index = (index+1) % batches.length;
	    res = true;
	}
	return res;

    }


    protected void acton(NextBatchCertificate b){
	NextBatch nb = b.getNextBatch();
	if (nb.getSeqNo() != baseSeqNo)
	    Debug.kill("SequenceNUmbers out of whack");

	if (nb.takeCP()){
	    synchronized(maxValues){
		maxValues.add(new Long(nb.getSeqNo()+1));
	    }
	}
//     	Debug.println("Executing next batch at : "+nb.getSeqNo() +
//     		      " with commands: "+nb.getCommands());
	long tmpTime = System.currentTimeMillis();
	glue.exec(b.getCommands(), nb.getSeqNo(),
		  nb.getNonDeterminism(), nb.takeCP());
	
    }


    protected void fetchCommands(NextBatchCertificate nbc, boolean fromAll){
	// first notify everybody of the batch and the expected contents
	if (!fromAll){

	    //	    System.out.println("fetching with the batch completed");
	    BatchCompleted bc = new BatchCompleted(nbc.getNextBatch(), getMyExecutionIndex());
	    authenticateFilterMacArrayMessage(bc);
	    sendToAllFilterReplicas(bc.getBytes());
	    return;
	}
	Enumeration<Entry> entries = nbc.getMissingEntries();
	int target;
	while (entries.hasMoreElements()){
	    Entry entry = entries.nextElement();
	    FetchCommand fc = new FetchCommand(nbc.getNextBatch().getSeqNo(),
					       entry, getMyExecutionIndex());
	    target = (int)((entry.getClient()+1) % BFT.Parameters.getFilterCount());
	    authenticateFilterMacArrayMessage(fc);
	    // System.out.println("fetching "+fc.getSeqNo()+" client "+
	    //		       entry.getClient()+" from all");
	    for (int i = 0;i < BFT.Parameters.getFilterCount(); i++)
		    if (i != target)
			sendToFilterReplica(fc.getBytes(), i);
	}
    }


    protected void process(ForwardCommand fwd){
	if (fwd.getSeqNo() < baseSeqNo)
	    return;
	if (!validateFilterMacMessage(fwd))
	    BFT.Debug.kill("BAD FILTER");

	int index = (int) (fwd.getSeqNo() % batches.length);

	boolean notcomplete = !batches[index].isComplete();
	batches[index].addCommand(fwd.getEntry().getClient(), fwd.getEntry());
	tryToExecute();
    }


    protected void process(FetchDenied fd){
 	System.out.println("fetch denied! seqno: "+fd.getSeqNo()+
  			   " client: "+fd.getEntry().getClient()+
  			   " from "+fd.getSendingReplica());
	int index = (int) (fd.getSeqNo() % batches.length);
	
	if (fd.getSeqNo() < baseSeqNo || batches[index ].isReadyForExecution())
	    return;
	if (!validateFilterMacMessage(fd))
	    BFT.Debug.kill("BAD FILTER");
	//	System.out.println("refetching!");
	if (fd.getSendingReplica() == (fd.getEntry().getClient()+1) % BFT.Parameters.getFilterCount())
	    fetchCommands( batches[index], true);
    }

    /**
       Process a read only request
     **/
    protected void process(ReadOnlyRequest req){
	//Debug.println("Process ReadOnlyRequest: "+req.getSender()+
	//	      " id: "+req.getCore().getRequestId());
	if (!readyForRequests)
	    return;
	if (!validateClientMacArrayMessage(req)){
	    Debug.kill("should have valid client mac array");
	}
	
	//  need to rate limit read only requests
	
	// its valid, so execute it
	glue.execReadOnly((int)(req.getCore().getSendingClient()),
			  req.getCore().getRequestId(),
			  req.getCore().getCommand());
    }


    /**
       Request an already taken checkpoint
    **/
    protected void process(RequestCP rcp){
	// if i have the requested cp, send it back
	//Debug.println("Process Request CP from "+rcp.getSendingReplica());
    System.out.println("Process Request CP from "+rcp.getSendingReplica());
	if (!validateOrderMacArrayMessage(rcp)) {
	    BFT.Debug.kill(new RuntimeException("FAILED VALIDATION! SN:"
						+ rcp.getSequenceNumber()));
	}
	CheckPointState cps = null;
	long seq = rcp.getSequenceNumber();
	Long key = new Long(seq);
	cps = (CheckPointState) stateSnapshots.get(key);
	if (cps == null && seq !=0){
	    System.out.println("no cp to return!");
	    return;
	}
	else if (cps == null && seq == 0)
	    cps = initialCP;
	if (cps != null && cps.isStable()){
	    Digest cpBytes = cps.getStableDigest();
	    System.out.println("\t sending cp back: "+rcp.getSequenceNumber());
	    CPTokenMessage cp = 
		new CPTokenMessage(cpBytes.getBytes(), 
				   rcp.getSequenceNumber(), 
				   getMyExecutionIndex());
	    authenticateOrderMacArrayMessage(cp);
	    sendToOrderReplica(cp.getBytes(), (int)(rcp.getSendingReplica()));
	}else{
	    Debug.kill("cant deliver the requested CP:"+
		       rcp.getSequenceNumber()+
		       " did you mean to get base instead of current? reason: cps != null "+
		       (cps != null) +" is stable "+ ((cps != null)?cps.isStable():false));
	}
    }

    /**
       release the specified checkpoint
    **/
    protected void process(ReleaseCP rcp){
	//Debug.println("\tprocess releaseCP");
	if (!validateOrderMacArrayMessage(rcp)) {
	    BFT.Debug.kill(new RuntimeException("FAILED VALIDATION! SN:"
						+ rcp.getSequenceNumber()));
	}
	releaseQuorum.addEntry(rcp);
	if (releaseQuorum.isComplete()){
	    acton(rcp);
	    releaseQuorum.clear();
	}

    }
    protected void acton(ReleaseCP rcp){
	//Debug.println("\t releasing checkpoint at "+rcp.getSequenceNumber());
	// fetch the checkpoint, get the app chekcpoint
	long index = rcp.getSequenceNumber()-3*BFT.order.Parameters.checkPointInterval;
	Enumeration<Long> enume = stateSnapshots.keys();
	while (enume.hasMoreElements()){
	    Long next = enume.nextElement();
	    if (index >= next.longValue()){
		CheckPointState cps = 
		    (CheckPointState) stateSnapshots.remove(next);
		if (cps == null)
		    return;
		//		System.out.println("Instructing Release CP: "+next);
		glue.releaseCP(cps.getCheckpoint());
		File cpfile = new File(cps.getMaxSequenceNumber() + "_SHIM_CP.LOG");
		if(!cpfile.exists()) {
		    //		    System.out.println("looking for file "+cps.getMaxSequenceNumber()+
		    //"_"+getMyExecutionIndex()+"_SHIM_CP.LOG");
		    cpfile = new File(cps.getMaxSequenceNumber() + "_" + getMyExecutionIndex() + "_SHIM_CP.LOG");
		}
		if(cpfile.exists()) {
		    cpfile.delete();
		    //BFT.//Debug.println("DELETED CP LOG: " + cpfile.getName());
		}
	    }
	}
    }

    long[] retranscount;
    protected void process(Retransmit ret){
	if (retranscount == null)
	    retranscount = new long[BFT.Parameters.getOrderCount()];
 	//System.out.println("rcv retransmit "+retranscount[(int)ret.getSendingReplica()]+
 	//		   " from: "+ret.getSendingReplica()+
 	//		   " for "+ret.getClient() +
 	//		   " at seqno "+ret.getSequenceNumber()+
 	//		   " last executed is "+baseSeqNo+
 	//		   " max returned is "+maxExecuted+
 	//		   " ready is "+amIReadyForRequests());
	retranscount[(int)ret.getSendingReplica()]++;
	if (!validateOrderMacArrayMessage(ret)) {
	    BFT.Debug.kill(new RuntimeException("FAILED VALIDATION! "+
						retranscount[(int)ret.getSendingReplica()]+"th retran SN:"
						+ ret.getSequenceNumber()+ 
						" from "+
						ret.getSendingReplica()));
	}
	int i = (int) ret.getClient();


 	if (!retrans[i].addEntry(ret))
 	    ;//System.out.println("replacing retrans from "+i+" with"+ ret.getSequenceNumber());
	if (retrans[i].isComplete()){
	    acton(ret);
	    retrans[i].clear();
	    //Debug.println("************ successful retrans to client: "+ret.getClient());
	}

    }

    long cantretrans = 0;
    protected void acton(Retransmit ret){
	//System.out.println("acting on retransmit for "+ret.getClient()+" with max: "+maxExecuted+" and base: "+baseSeqNo + " and ret number "+ret.getSequenceNumber());
	Reply rep = workingState.getReply((int)(ret.getClient()), this);
	if (rep != null){
	    if (retransCount[(int)ret.getClient()] > 5){
		CPUpdate cpu = new CPUpdate(workingState.getCommandIndices(), 
					    workingState.getSequenceNumber(),
					    getMyExecutionIndex());
		authenticateFilterMacArrayMessage(cpu);
		sendToAllFilterReplicas(cpu.getBytes());
	    }
	    authenticateClientMacMessage(rep, (int)ret.getClient());
	    long tmp = System.currentTimeMillis();
	    sendToClient(rep.getBytes(),
			 (int) (ret.getClient()));
	    

	}else{// this only happens when we're in the midst of fetching state
	    System.out.println("\t\t\tfailed to retrans");
	    cantretrans++;
	    if (cantretrans > 100){
		lastExecuted(baseSeqNo);
		cantretrans = 0;
		return;
	    }
	}
	if (ret.getSequenceNumber() >= workingState.getSequenceNumber()){
	    if (!amIReadyForRequests())
		lastExecuted(maxExecuted+1);
	    else 
		lastExecuted(workingState.getSequenceNumber());
	}
    }


    protected void process(LoadCPMessage lcp){
	Debug.println("proces loadcp message "+lcp.getSequenceNumber()+" from "+
		      lcp.getSendingReplica());
	if (lcp.getSequenceNumber() < baseSeqNo)
	    return;
	if(!validateOrderMacMessage(lcp)) {
	    BFT.Debug.kill(new RuntimeException("FAILED VALIDATION! SN:"
						+ lcp.getSequenceNumber()));
	}
	loadCPQuorum.addEntry(lcp);
	if (loadCPQuorum.isComplete()){
	    acton(lcp);
	    loadCPQuorum.clear();
	}

    }

    protected Digest fetchingToken;
    protected long fetchingCP;
    protected void acton(LoadCPMessage lcp){
	System.out.println("\t\tLoading cp at "+lcp.getSequenceNumber());
	// load the checkpoint locally.  once thats completed, fetch
	// the app checkpoint and...
	Long index = new Long(lcp.getSequenceNumber());
	CheckPointState tmp = stateSnapshots.get(index);
	if (tmp == null){
	    if (lcp.getSequenceNumber() < fetchingCP)
		return;

	    // try to fetch it locally
	    String suffix = "_"+getMyExecutionIndex() + "_SHIM_CP.LOG";
	    System.out.println("trying to locate "+lcp.getSequenceNumber()+suffix+" on disk");
	    int readable = 0;
	    File maybe = null;
	    FileInputStream fis = null;
	    try{
		maybe = new File(lcp.getSequenceNumber()+
				      suffix);
		fis = new FileInputStream(maybe);
		readable = fis.available();
	    }catch(java.io.FileNotFoundException fnfe){}
	    catch(Exception e){
		System.out.println("got an exception");
		e.printStackTrace();
		System.out.println(fis+" "+maybe+" "+readable);
		fis = null;
		maybe = null;
		readable = 0;
	    }
	    if (readable > 0){
		try{
		    byte b[] = new byte[fis.available()];
		    fis.read(b);
		    tmp = new CheckPointState(b);
		    Digest d = new Digest(tmp.getBytes());
		    tmp.setStableDigest(d);
		    if (!d.equals(Digest.fromBytes(lcp.getToken()))){
			System.out.println("read: "+d);
			System.out.println("want: "+
					   Digest.fromBytes(lcp.getToken()));
			BFT.Debug.kill("uh oh.  not the same tokens");
		    }
		    // now cleanup all other cp files off of the disk
		    System.out.println(maybe);
		    File parent = maybe.getCanonicalFile().getParentFile();
		    System.out.println(parent);
		    LogFilter lf = new LogFilter(suffix);
		    String[] files = parent.list(lf);
		    for (int i = 0; i < files.length; i++)
			if (new Long(files[i].replace(suffix, "")).longValue() < lcp.getSequenceNumber()){
			    System.out.println("deleteing "+files[i]);
			    new File(files[i]).delete();
			}else
			    System.out.println("keeping "+files[i]);
		}catch(Exception e){
		    System.out.println("Some exception while reading from disk");
		    BFT.Debug.kill(e);
		}
	    }else{
		// that failed so fetch it remotely
		FetchCPMessage fcpm = new FetchCPMessage(lcp.getSequenceNumber(),
							 getMyExecutionIndex());
		authenticateExecMacArrayMessage(fcpm);
		sendToOtherExecutionReplicas(fcpm.getBytes());
		fetchingCP = lcp.getSequenceNumber();
		fetchingToken = Digest.fromBytes(lcp.getToken());
		System.out.println("\t\tfetching state from other nodes:  "+fetchingCP);
		return;
	    }
	    
	}
	Digest loading = new Digest(tmp.getBytes());
	Digest cpBytes = Digest.fromBytes(lcp.getToken());
	if (!cpBytes.equals(loading)){
	    Debug.kill("CP descriptors dont match. this is a local error");
	}

	loadCheckPointState(tmp, lcp.getSequenceNumber());
    }

    protected void process(FetchCPMessage fcp){
	Debug.println("Process fetchcpmessage");
	CheckPointState tmp = stateSnapshots.get(new Long(fcp.getSequenceNumber()));
	if (tmp == null || tmp.getCheckpoint() == null){
	    //System.out.println("\trejecting fetch state b/c i dont have it");
	    // i dont have it
	    return;
	}
	byte[] bytes = tmp.getBytes();
	CPStateMessage cpsm = new CPStateMessage(bytes, fcp.getSequenceNumber(),
						 getMyExecutionIndex());
	System.out.println("Sending state with bytes: "+new Digest(bytes));
	System.out.println("app cp: "+tmp.getCheckpoint());

	authenticateExecMacMessage(cpsm, (int)(fcp.getSendingReplica()));
	sendToExecutionReplica(cpsm.getBytes(),(int)(fcp.getSendingReplica()));
    }

    protected void process(CPStateMessage cpsm){
	//Debug.println("Process CPStateMessage "+cpsm.getSequenceNumber());
	if (fetchingCP != cpsm.getSequenceNumber() ||
	    fetchingToken == null){
	    //Debug.println("\trejecting a cpstatemessage b/c its not what we're looking for");
	    return;
	}
	if(!validateExecMacMessage(cpsm)) {
	    BFT.Debug.kill(new RuntimeException("FAILED VALIDATION! SN:"
						+ cpsm.getSequenceNumber()));
	}

	Digest check = new Digest(cpsm.getState());

	if (!fetchingToken.equals(check)){
	    System.out.println(cpsm.getSequenceNumber());
	    System.out.println(fetchingCP);
	    System.out.println(fetchingToken);
	    System.out.println(check);
	    
	    Debug.kill(new RuntimeException("got back bytes that dont match"));
	}
	

	CheckPointState tmp = new CheckPointState(cpsm.getState());
	stateSnapshots.put(new Long(cpsm.getSequenceNumber()), tmp);
	loadCheckPointState(tmp, cpsm.getSequenceNumber());
    }

    protected void loadCheckPointState(CheckPointState cps, long seqno){
	Debug.println("\t\tloading glue checkpoint at : "+seqno);
	System.out.println(cps);
	readyForRequests = true;
	glue.loadCP(cps.getCheckpoint(), seqno);
	workingState = new CheckPointState(cps);

// 	for(int i = 0; i< batches.length; i++)
// 	    batches[i].clear();

	//	baseIndex = 0;
	baseSeqNo = workingState.getSequenceNumber();
	maxExecuted = baseSeqNo -1;
	synchronized(maxValues){
	    maxValues.clear();
	}
	fetchingToken = null;
	stateSnapshots.put(new Long(seqno), cps);
	if (amIReadyForRequests()){
 	    System.out.println("\t\ttrying to execute with baseseqno at: "+baseSeqNo);
	    tryToExecute();
	    System.out.println("\t\tfinished executing and baseSeqNo is now "+baseSeqNo);
	    CPLoaded cpl = new CPLoaded(baseSeqNo-1, getMyExecutionIndex());
	    authenticateOrderMacArrayMessage(cpl);
	    sendToAllOrderReplicas(cpl.getBytes());
	    CPUpdate cpu = new CPUpdate(workingState.getCommandIndices(), 
					workingState.getSequenceNumber(),
					getMyExecutionIndex());
	    authenticateFilterMacArrayMessage(cpu);
	    sendToAllFilterReplicas(cpu.getBytes());
	}
    }

    int fetchingstatenow = 0;
    protected void process(FetchState fsm){
	//Debug.println("Process FetchState from another replica ");
	if(fsm.getSendingReplica()==getMyExecutionIndex()){
	    return;
	}
	if(!validateExecMacArrayMessage(fsm)) {
	    Debug.kill(new RuntimeException("FAILED VALIDATION!"));
	}
	System.out.println("looking for state token: " +new Digest(fsm.getToken())+" for "+fsm.getSendingReplica());
	//	BFT.util.UnsignedTypes.printBytes(fsm.getToken());
	stateReqs.put(new String(fsm.getToken()), fsm);
	glue.fetchState(fsm.getToken());
	fetchingstatenow++;
    }

    protected void process(AppState as){
	//Debug.println("process appstate");
	if (!validateExecMacMessage(as))
	    BFT.Debug.kill(new RuntimeException("failed validation"));
	Debug.println("process AppState: "+new Digest(as.getToken()));
	glue.loadState(as.getToken(), as.getState());
    }

    public void handle(byte[] vmbbytes){
	VerifiedMessageBase vmb = MessageFactory.fromBytes(vmbbytes);
    //System.out.println("Got new tag "+vmb.getTag());
	switch(vmb.getTag()){
	case MessageTags.SpeculativeNextBatch: ;
	case MessageTags.TentativeNextBatch:  ;
	case MessageTags.CommittedNextBatch: process((NextBatch)vmb); return;
	case MessageTags.ReleaseCP: process((ReleaseCP) vmb); return;
	case MessageTags.Retransmit: process((Retransmit) vmb); return;
	case MessageTags.LoadCPMessage: process((LoadCPMessage) vmb); return;
	case MessageTags.RequestCP: process((RequestCP) vmb); return;
	case MessageTags.FetchCPMessage: process((FetchCPMessage) vmb); return;
	case MessageTags.CPStateMessage: process((CPStateMessage) vmb); return;
	case MessageTags.FetchState: process((FetchState) vmb); return;
	case MessageTags.AppState: process((AppState) vmb); return;
	case MessageTags.ReadOnlyRequest: process((ReadOnlyRequest) vmb); return;
	case MessageTags.ForwardCommand: process((ForwardCommand) vmb); return;
	case MessageTags.FetchDenied: process((FetchDenied)vmb); return;
	default: Debug.kill("servershim does not handle message "+vmb.getTag());
	}
    }

    // hashtable of things im asking other shims for
    protected Hashtable<String, FetchState> stateReqs = 
	new Hashtable<String, FetchState>();
    public void requestState(byte[] b){
	//Debug.println("processing request state call from the glue");
	FetchState fs = new FetchState(b, getMyExecutionIndex());
	//System.out.println("fetching ste token:");
	//	BFT.util.UnsignedTypes.printBytes(b);
	authenticateExecMacArrayMessage(fs);
	sendToOtherExecutionReplicas(fs.getBytes());
    }

    public void returnState(byte[] b, byte[] s){
	//Debug.println("\t*calling returnstate issued by the glue");
	FetchState fs = stateReqs.get(new String(b));
	fetchingstatenow--;
		
	if (fs == null){
	    //Debug.println("sending nothing back b/c nothing in the table");
	    //	    Debug.kill("WTF");
	    return;
	}
	stateReqs.remove(new String(b));
	Debug.println("Sending state "+new Digest(b));
	//	BFT.util.UnsignedTypes.printBytes(b);
	AppState as = new AppState(b, s, getMyExecutionIndex());
	authenticateExecMacMessage(as, (int)(fs.getSendingReplica()));
	sendToExecutionReplica(as.getBytes(), (int)(fs.getSendingReplica()));
    }

    /**
       Now the functions in the servershim interface
    **/
    public void result(byte[] result, int clientId, long clientReqId,
		       long seqNo, boolean toCache){
// 	Debug.println("\t\t&&&&result at "+seqNo+" for "+clientId+
// 		      " is going to cache: "+toCache+" and is "+result.length + " bytes long");
	 
	if (toCache){ // add to the reply cache
	    retransCount[clientId] = 0;
	    Reply rep = new Reply(getMyExecutionIndex(), clientReqId, result);
	    authenticateClientMacMessage(rep, clientId);
	    //Debug.println("is there a max value: "+!maxValues.isEmpty());
	    //Debug.println("current seqno: "+seqNo);
	    //Debug.println("max seqno for this state: "+workingState.getMaxSequenceNumber());	    
	    synchronized(maxValues){
		while (!maxValues.isEmpty()
		       && seqNo >= maxValues.firstElement().longValue()){
		    long val = maxValues.firstElement().longValue();
		    maxValues.remove(0);
		    workingState.setMaxSequenceNumber(val);
		    stateSnapshots.put(new Long(val),
				       workingState);
		    workingState = new CheckPointState(workingState);
		    //Debug.println("creating a new working state "+
			//	       "based at: "+
			//	       workingState.getBaseSequenceNumber());
		}
	    }
	    workingState.addReply(rep, seqNo, clientId); 
	    sendToClient(rep.getBytes(), clientId);
	    if (seqNo > maxExecuted)
		maxExecuted = seqNo;
	}else{
	    WatchReply rep = 
		new WatchReply(getMyExecutionIndex(), clientReqId, result);
	    authenticateClientMacMessage(rep, clientId);
	    sendToClient(rep.getBytes(), clientId);
	}	
    }

    public void readOnlyResult(byte[] result, int clientId, long reqId){
	//Debug.println("\t\treadonlyresult for "+clientId+" at "+reqId);
	ReadOnlyReply reply = 
	    new ReadOnlyReply(getMyExecutionIndex(), reqId, result);
	authenticateClientMacMessage(reply, clientId);
	sendToClient(reply.getBytes(), clientId);
    }


    /**
       send a cptoken message indexing the cptoken by seqno+1
    **/
    public void returnCP(byte[] AppCPToken,  long seqNo){
	CheckPointState cps = stateSnapshots.get(new Long(seqNo+1));
	if (cps == null){
	    //Debug.println("Using the working state");
	    cps = workingState;
	    if (seqNo < workingState.getBaseSequenceNumber()){
		//Debug.println("doing nothing since we've already released this checkpoint");
		return;
	    }
	}
	cps.setMaxSequenceNumber(seqNo+1);
	cps.addCheckpoint(AppCPToken, seqNo);
	Digest cpBytes = new Digest(cps.getBytes());
	//Debug.println(cps);
	//Debug.println("\t\t&&&&taking cp at "+seqNo);
	//Debug.println("\t\tcp digest is :\n"+cpBytes);
	if (BFT.order.Parameters.doLogging){
	    CPLogger cpl = new CPLogger(this, cps, getMyExecutionIndex());
	    pool.execute(cpl);
	}else
	    makeCpStable(cps);
    }
	
    
    public void makeCpStable(CheckPointState cps){
	cps.markStable();
		CPTokenMessage cp = 
	    new CPTokenMessage(cps.getStableDigest().getBytes(), cps.getMaxSequenceNumber(),
			       getMyExecutionIndex());
	//System.out.println(cp);
	//System.out.println(cps);
	authenticateOrderMacArrayMessage(cp);
	sendToAllOrderReplicas(cp.getBytes());
    }


    public synchronized boolean amIReadyForRequests(){
	return readyForRequests;
    }

    public synchronized void noMoreRequests(){
	System.out.println("denying requests at max: "+maxExecuted+
			   "base: "+baseSeqNo);
	readyForRequests = false;
	//Debug.println("Im not ready for batches anymore");
    }

    public synchronized void readyForRequests(){
	if (readyForRequests)
	    return;
	System.out.println("ready for requests again, restart at "+baseSeqNo);
	readyForRequests = true;
	//System.out.println("sending cp loaded "+baseSeqNo);
	lastExecuted(baseSeqNo);
	//Debug.println("give me more stuff!");
    }

    /**
       Upcall indicating the  last request executed by the application.
    **/
    protected long lastSentLastExecuted = 0;
    protected void lastExecuted(long seqNo){
	//	if (seqNo != baseSeqNo)
	//  Debug.kill(new RuntimeException("Invalid seqno in last executed "+
	//				    seqNo+":"+baseSeqNo));
	long time = System.currentTimeMillis();
       	if (time - lastSentLastExecuted  < 2500){

	    return;
	}
	System.out.println("Last exectued at : "+ time);
	//		try{throw new RuntimeException("seqNo: "+seqNo);}catch(Exception e){e.printStackTrace();}
	lastSentLastExecuted = time;
	System.out.println("sending last exected with "+seqNo);
	LastExecuted le = new LastExecuted(seqNo, getMyExecutionIndex());
	authenticateOrderMacArrayMessage(le);
	sendToAllOrderReplicas(le.getBytes());
	CPUpdate cpu = new CPUpdate(workingState.getCommandIndices(), 
				    workingState.getSequenceNumber(),
				    getMyExecutionIndex());
	authenticateFilterMacArrayMessage(cpu);
	sendToAllFilterReplicas(cpu.getBytes());
    }



    public InetAddress getIP(int i){
	return getMembership().getClientNodes()[i].getIP();
    }

    public int getPort(int i){
	return getMembership().getClientNodes()[i].getPort();
    }
    
    public void start(){
	super.start();
	System.out.println("started at : "+System.currentTimeMillis());
	CheckPointState cps = null;
	Long key = new Long(0);
	cps = (CheckPointState) stateSnapshots.get(key);
	//Debug.println("start!");
	if (cps != null){
	    Digest cpBytes = new Digest(cps.getBytes());
	    CPTokenMessage cp = 
		new CPTokenMessage(cpBytes.getBytes(), 
				   0,
				   getMyExecutionIndex());
	    authenticateOrderMacArrayMessage(cp);
        System.out.println("spontaneously sending CPtoken to all order nodes");
	    sendToAllOrderReplicas(cp.getBytes());
	}
// 	LastExecuted le = new LastExecuted(0, getMyExecutionIndex());
// 	authenticateOrderMacArrayMessage(le);
// 	sendToAllOrderReplicas(le.getBytes());
    }


    //	private void authenticateHere(){
    //		System.err.println("BFT/serverShim/ShimBAseNode authenticate here");
    //	}
}

