package BFT.filter;

import BFT.BaseNode;
import BFT.MessageFactory;
import BFT.Debug;

// outgoing messages
import BFT.messages.FilteredRequest;
import BFT.messages.FilteredRequestCore;
import BFT.messages.Digest;
import BFT.messages.ForwardCommand;
import BFT.messages.FetchDenied;

// incoming messages
import BFT.messages.ClientRequest;
import BFT.messages.SimpleRequestCore;
import BFT.messages.BatchCompleted;
import BFT.messages.FetchCommand;
import BFT.messages.CPUpdate;

import BFT.messages.VerifiedMessageBase;
import BFT.messages.MessageTags;

import BFT.util.Role;

import BFT.network.concurrentNet.*;
import java.security.Security;
import java.util.PriorityQueue;



import BFT.filter.statemanagement.Batch;
import BFT.filter.statemanagement.CheckPointState;
import BFT.messages.Entry;
import BFT.messages.FilteredRequest;
import BFT.messages.Quorum;
import BFT.messages.CommandBatch;


public class FilterBaseNode extends BaseNode {

    protected long[] retransDelay, // current retransmission delay for the client
	lastreqId,  // last request id ordered for the client
	lastTime,  // last time a request was sent for the client
	retransCount; // number of retransmissions for the specified client

    protected long baseRetrans = 00;
    
    protected CheckPointState[] intervals; // requests that were ordered in the specified interval
    protected int baseIntervalIndex; // the oldest interval we are currently maintaining
    
    protected ClientRequest[] earlyReqs ;// 1 request per client with seqno > the last completed one for that client
    protected long[] early ; // sequence nubmers from the future sent
    // by exec replicas
    protected BatchCompleted[][] futureBatch; // set of futur-istic
					      // batches.  for each
					      // client, an array with
					      // one entry per filter
					      // node for batch
					      // completed messages
					      // not in a checkpoint
					      // interval that we are
					      // currently caching

    protected CPUpdate[] cpus; // 1 cpupdate message per filter
    
    protected Entry[] pendingClientEntries; // pending client entries.
					    // client request with
					    // sequence number greater
					    // than the last request
					    // to be forwarded but not
					    // yet committed

    protected FilteredRequest[] lastsent; // Cached copy of hte last message sent for each client.
    
    
    protected int currentPrimary = 0;
    protected long currentView = 0;
    
    protected int loggerIndex = 0;
    protected int loggerIndexMax = 3;
    
    protected MsgLogQueue mlq;
    
    protected int min_size = 4*BFT.messages.Digest.size();  
    // minimum size to force a digest rather than raw request

    protected Integer clientLocks[];
    
    public FilterBaseNode(String membership, int id){
	super(membership, BFT.util.Role.FILTER, id);
	clientLocks = new Integer[BFT.Parameters.getNumberOfClients()];
	retransDelay = new long[BFT.Parameters.getNumberOfClients()];
	retransCount = new long[retransDelay.length];
	lastTime = new long[BFT.Parameters.getNumberOfClients()];
	lastreqId = new long[lastTime.length];
	lastsent = new FilteredRequest[lastTime.length];
	pendingClientEntries = new Entry[lastTime.length];
	for (int i = 0;i < lastsent.length; i++){
	    retransDelay[i] = baseRetrans;
	    lastreqId[i] = -1;
	    retransCount[i] = 0;
	    clientLocks[i] = new Integer(i);
	}
	
	cpus = new CPUpdate[BFT.Parameters.getExecutionCount()];

	intervals = new CheckPointState[5*(BFT.order.Parameters.maxPeriods)+1];
	baseIntervalIndex = 0;
	for (int i = 0; i < intervals.length; i++)
	    intervals[i] = new CheckPointState(i*BFT.order.Parameters.checkPointInterval);
	
	early = new long[BFT.Parameters.getExecutionCount()];
	earlyReqs = new ClientRequest[BFT.Parameters.getNumberOfClients()];
	futureBatch = new BatchCompleted[BFT.order.Parameters.checkPointInterval][];
	for (int i = 0; i < futureBatch.length; i++)
	    futureBatch[i] = new BatchCompleted[BFT.Parameters.getExecutionCount()];
    }
    

    protected void process(ClientRequest req){
	boolean early = false;
	int sender = (int) (req.getCore().getSendingClient());
	// if there is a pending request
	// && this request is greater than the last one i forwardded, then it is early
	synchronized(clientLocks[sender]){
	    if (pendingClientEntries[sender] != null
		&& req.getCore().getRequestId() > lastreqId[(sender)]){
		//	System.out.println("***got a request early from "+
		//		   sender+
		//		   " at "+req.getCore().getRequestId()+
		//		   " want "+
		//		   lastreqId[(int)(req.getCore().getSendingClient())]);
		earlyReqs[sender] = req;
		early = true;
	    }
	    if (!validateClientMacArrayMessage(req) && !BFT.Parameters.cheapClients)
		Debug.kill("bad verification on client mac array message");
	    if (!req.checkAuthenticationDigest() && !BFT.Parameters.cheapClients)
		Debug.kill("bad digest");
	    
	    SimpleRequestCore src = (SimpleRequestCore)req.getCore();
	    // if its old, or we've already got something cached
	    // for this guy, consider retransmitting
	    int client = sender;
	    long current = System.currentTimeMillis();
	    
	    // this condition should probably check that lastsent != null
	    // if the request is early 
	    // or the sequencenumber is at most the last thing i sent
	    // or there is a pending request
	    // then attempt to retransmit
	    if (early 
		|| src.getRequestId() <= lastreqId[client]
		|| pendingClientEntries[client] != null){
		if (current - lastTime[client] < retransDelay[client] || lastsent[client] == null){
		    //System.out.println("blocking retransmission of client "+client+" for request "+src.getRequestId());
		    return;
		}
		//		System.out.println("retransmitting to all order replicas for client: "+client +" @ "+src.getRequestId());
		
		sendToAllOrderReplicas(lastsent[client].getBytes());
		retransDelay[client] *=2;
		retransCount[client]++;
		if (retransDelay[client] > 1000)
		    retransDelay[client] = 1000;
		//			System.out.println(retransCount[client]+ 
		//					   " delay on "+client+" is "+retransDelay[client]+" and its been "+(current - lastTime[client]));
		lastTime[client] = current;
		return;
		
	    } 
	    // otherwise the request is not early, the request is new, and
	    // there are no outsanding requests from the client so i can
	    // process it
	    else{
		//		System.out.println("first receipt of "+sender+" "+src.getRequestId());
		// create and authenticate the filtered requestcore
		FilteredRequestCore frc;
		boolean use_digest = false;
		if (!BFT.Parameters.filterCaching 
		    || src.getCommand().length < min_size  )
		    frc = new FilteredRequestCore(src.getEntry());
		else{
		frc = 
		    new FilteredRequestCore(new Entry(src.getSendingClient(),
						      src.getRequestId(),
						      src.getEntry().getMyDigest()));
		use_digest = true;
		}
		

		authenticateOrderMacSignatureMessage(frc);
		// if we're sending a digest, record teh
		// simplecore for posterity
		//			System.out.println("filtering "+src.getRequestId()+" for client "+src.getSendingClient());
	    if (frc.getEntry().getDigest() != null)
		pendingClientEntries[client] = src.getEntry();
	    lastreqId[client] = frc.getRequestId();
	    // log it to disk
	    if (use_digest)
		log(frc, src.getEntry());
	    else
		andSend(frc);
	    }
	    //	    System.out.println(pendingClientEntries[client]+" is pending");
	}
    }
	
    protected   void log(FilteredRequestCore frc, Entry entry){
		// and now write the frc.getEntry() to disk
		// when done, call andsend
		//System.out.println("recording frc to disk "+frc.getSendingClient()+" "+frc.getRequestId());
		if (BFT.Parameters.doLogging) {
		    MsgLogWrapper wrap = new MsgLogWrapper(frc, entry);
			logCount++;
			mlq.addWork(loggerIndex, wrap);
		}
		else {
			andSend(frc);
		}
	}

 
	protected void dump(Entry ent){
		// write DEADBEEF + ent.getBytes()
		//System.out.println("dumping to disk without a send");
	    if (ent == null)
		return;
		if (BFT.Parameters.doLogging) {
			MsgLogWrapper wrap = new MsgLogWrapper(ent);
			logCount++;
			dumpCount++;
			mlq.addWork(loggerIndex, wrap);
		}
	}
    
    int dumpCount = 0;
	protected void startDump() {
	    //	    System.out.println("dumped "+dumpCount+" things");
	    dumpCount=0;
		MsgLogWrapper wrap = new MsgLogWrapper(LogFileOps.START_DUMP);
		mlq.addWork(loggerIndex, wrap);
	}

    protected void andSend(FilteredRequestCore frc){
	//System.out.println("normal send");
	//	System.out.println("made it to send " + frc.getSendingClient() + " " + frc.getRequestId());
	int client = (int)frc.getSendingClient();
	FilteredRequestCore[] frc2=new FilteredRequestCore[1];
	frc2[0] = frc;
	FilteredRequest req2 = 
	    new FilteredRequest(getMyFilterIndex(),
					frc2);
	authenticateOrderMacArrayMessage(req2);
	
	synchronized(clientLocks[client]){
	    retransDelay[client] = baseRetrans;
	    lastsent[client] = req2;
	    //	    sendToAllOrderReplicas(req2.getBytes());
	    		sendToOrderReplica(req2.getBytes(), currentPrimary);
	    lastTime[client] = System.currentTimeMillis();
	}
    }
	
    protected void andSend(FilteredRequestCore[] frc){
	    //	    System.out.println("made it to send " );
	//	    for (int i = 0; i < frc.length; i++)
	//   	System.out.println("\t sending <"+frc[i].getSendingClient()+","+
	//   			   frc[i].getRequestId()+">");
	    FilteredRequest req2 = 
			new FilteredRequest(getMyFilterIndex(),
					    frc);
	    authenticateOrderMacArrayMessage(req2);
	    sendToAllOrderReplicas(req2.getBytes());
	    //		sendToOrderReplica(req2.getBytes(), currentPrimary);
	    
	    for (int i = 0; i < frc.length; i++ ){
		int client = (int) (frc[i].getSendingClient());
		synchronized(clientLocks){
		    retransDelay[client] = baseRetrans;
		    lastsent[client] = req2;
		    lastTime[client] = System.currentTimeMillis();
		}
	    }
	}
	
    int logCount = 0;
	protected  void nextLogger() {
	    loggerIndex = (loggerIndex+1)% loggerIndexMax;
		MsgLogWrapper wrap = new MsgLogWrapper(LogFileOps.CREATE);
		mlq.addWork(loggerIndex, wrap);
		//		System.out.println("logged "+logCount+" in that log");
		logCount = 0;
	}
	
	public void setLoggerQueue(MsgLogQueue mlq) {
		this.mlq = mlq;
		nextLogger();
	}




    protected void process(CPUpdate cpu){
	//	System.out.println("cpupdate: "+cpu.getSendingReplica() + " "+cpu.getSequenceNumber());
	int sender = cpu.getSendingReplica();
	// if i already have a cpupdate and either its new than this one
	// or this one does not dominate the old one then return immeciately
	if (cpus[sender] != null
	    && (cpu.getSequenceNumber() < cpus[sender].getSequenceNumber() 
		|| !cpu.dominates(cpus[sender])))
	    return;

	if (!validateExecMacArrayMessage(cpu))
	    BFT.Debug.kill("Bad execution replcia!");

	cpus[sender] = cpu;
	int count = 0;
	int i = 0;
	// check to see if there is a cpupdate from a server
	// that is weakly dominated by $liars$ other servers.
	while  (i < cpus.length 
		&& count < BFT.Parameters.smallExecutionQuorumSize()){
	    cpu = cpus[i];
	    count = 0;
	    for (int j = 0; cpu != null && j < cpus.length; j++)
		if (i == j)
		    count++;
		else if (cpus[j] != null && cpus[j].weaklyDominates(cpu))
		    count++;
	    i++;
	}

	// no cpupdate is dominated --> i.e. nothing moves our lastreqid forward
	if (count < BFT.Parameters.smallExecutionQuorumSize() || cpu == null){
	    //	    System.out.println("nothing was dominated");
	    return;
	}	

	
	long indices[] = cpu.getCommands();
	// remove the dominated cpu
	cpus[i-1] = null;
	// update all tables to reflect the new maximal value for
	// sequence numbers committed for each client.
	for (i = 0; i < indices.length; i++){
	    // if the updated sequence nubmer is greater than the one
	    // we record, then update
	    //	    System.out.println(i+" old: "+lastreqId[i]+" new: "+indices[i]);
	    synchronized(clientLocks[i]){
		if (indices[i] >= lastreqId[i]){
		    lastreqId[i] = indices[i];
		    pendingClientEntries[i] = null;
		    if (earlyReqs[i] != null){
			process(earlyReqs[i]);
			earlyReqs[i] = null;
		    }
		}
	    }
	}
    }
    
    
    protected void process(BatchCompleted bc){
	//	System.out.println("batch completed: "+bc.getSeqNo());
	// if the batch sequence number is smaller than the base of our
	// minimal checkpoint, then done
	if (bc.getSeqNo() < baseSequenceNumber())
	    return;
	if (!validateExecMacArrayMessage(bc))
	    BFT.Debug.kill("Bad Authentication");
	// if the sequence number is larger than any sequence numbers that we are currently caching
	if (bc.getSeqNo() >= maxSequenceNumber()){
	    // record the early sequencenumber
	    early[ bc.getSendingReplica()] = bc.getSeqNo();
	    // if its in the near enough future, then cache it
	    if (bc.getSeqNo() <= maxSequenceNumber() + BFT.order.Parameters.checkPointInterval){
		futureBatch[(int)(bc.getSeqNo()%BFT.order.Parameters.checkPointInterval)][bc.getSendingReplica()] = bc;
	    }
	    int earlycount = 0;
	    for (int i = 0; i < early.length; i++)
		earlycount += (early[i] >= maxSequenceNumber())?1:0;
	    // if small quorum of execution nodes agree that we are
	    // slow, then garbage collect the oldest quorum
	    if (earlycount >= BFT.Parameters.smallExecutionQuorumSize()) {
		garbageCollect();
	    }
	    return;
	}

	int index = (int) ((bc.getSeqNo()-baseSequenceNumber())
			   / BFT.order.Parameters.checkPointInterval);
	CheckPointState cps = intervals[(baseIntervalIndex+index)%intervals.length];
	Quorum<BatchCompleted> quorum = cps.getQuorum(bc.getSeqNo());

	boolean complete = quorum.isComplete();
	quorum.addEntry(bc);
	//if the current batch completed the quorum
	if (!complete && quorum.isComplete()){
	    CommandBatch cb = bc.getCommands();
	    Entry[] entries = cb.getEntries();
	    Digest d;
	    Entry e;

	    if (bc.getView() > currentView){
		currentView = bc.getView();
		currentPrimary = 
		    (int)(bc.getView() % BFT.Parameters.getOrderCount());
	    }
	    //	    String completing = "\t completing: ";
	    for (int i = 0; i < entries.length; i++){
		e = entries[i];
		d = e.getDigest();
		int c = (int) e.getClient();
		int forme = getMyFilterIndex() ;
		synchronized(clientLocks[c]){
		    Entry e2 = pendingClientEntries[(int)c];
		    // if the pending request matches the entry in the
		    // message, then add it to the checkpoint
		    // and clear hte pending slot
		    if (d != null && e2 != null
			&& e2.getRequestId() == e.getRequestId()
			&& e2.matches(e.getDigest())){
			//			completing += "<"+c+","+e2.getRequestId()+"> ";
			cps.addRequest(e2);
			pendingClientEntries[(int)c] = null;
			// if im the sender designate, then send
			if ((c+1) % BFT.Parameters.getFilterCount() == forme){
			    //			    			    System.out.println("\tcomplete batch fetch for "+
			    //	       "client "+c+" to server "+
			    //		       bc.getSendingReplica());
			    fetch(bc.getSeqNo(), e, bc.getSendingReplica());
			    
			}
		    }// otherwise if there is no pending entry or the
		    // pending entry precedes the completed entry,
		    // clear the pending list and update the last requestid
		    else if (e2 == null || e2.getRequestId() <= e.getRequestId()){
// 			if (e2 == null)
// 			    System.out.println("clearing null");
// 			else
// 			    System.out.println("clearing pending entry <"+
// 					       e2.getClient()+","+
// 					   e2.getRequestId()+">");
			pendingClientEntries[(int)c] = null;
			if (e.getRequestId() > lastreqId[(int)c])
			    lastreqId[(int)c] = e.getRequestId();
		    }
		    if (earlyReqs[(int)c] != null){
			//System.out.println("&&& processing that thing we got early");
			process(earlyReqs[(int)c]);
			earlyReqs[(int)c] = null;
		    }
		}
	    }
	    //	    System.out.println(completing);
	    // if im at a checkpoint interval, then garbage collect
	    if (bc.getSeqNo() == maxSequenceNumber()-1-BFT.order.Parameters.checkPointInterval) {
		garbageCollect();
	    }

	}
	else{// wasnt complete, still need to respond
	    CommandBatch cb = bc.getCommands();
	    Entry[] entries = cb.getEntries();
	    Entry e;
	    int forme = getMyFilterIndex();
	    
	    for (int i = 0; i < entries.length; i++){
		e = entries[i];
		if (e.getDigest() != null && 
		    (e.getClient()+1) % BFT.Parameters.getFilterCount() == forme){
		    synchronized(clientLocks[(int)e.getClient()]){
			//			System.out.println("\tincomplete batch fetch for client "+
			//		   e.getClient()+" for "+bc.getSendingReplica());
			fetch(bc.getSeqNo(), e, bc.getSendingReplica());
		    }
		}
	    }
	}

    }


    protected void garbageCollect(){
	if(BFT.Parameters.doLogging) {
	    startDump();
	    for (int i = 0; i < pendingClientEntries.length; i++)
		if (pendingClientEntries[i] != null)
		    dump(pendingClientEntries[i]);
	    nextLogger();
	}

	intervals[baseIntervalIndex] = new CheckPointState(maxSequenceNumber());
	baseIntervalIndex = (baseIntervalIndex +1) % intervals.length;
	int earlycount = 0;
	for (int i = 0; i < early.length; i++)
	    earlycount += (early[i] >= maxSequenceNumber())?1:0;
	if (earlycount >= BFT.Parameters.smallExecutionQuorumSize())
	    garbageCollect();
	BatchCompleted bc;
	for (int i = 0; i < futureBatch.length; i++)
	    for (int j = 0; j < BFT.Parameters.getExecutionCount(); j++){
		if (futureBatch[i][j] != null){
		    bc = futureBatch[i][j];
		    futureBatch[i][j] = null;
		    process(futureBatch[i][j]);
		}
	    }
    }


    protected void process(FetchCommand fc){
	//	System.out.println("Fetching");
	if (fc.getSeqNo() < baseSequenceNumber() 
	    || fc.getSeqNo() > maxSequenceNumber()) {
	    //	    System.out.println("trying to fetch "+fc.getSeqNo()+
	    //		       " which is not in ["+baseSequenceNumber()+
	    //		       ", "+maxSequenceNumber()+")");
	    if (fc.getSeqNo() > maxSequenceNumber())
		BFT.Debug.kill("Bad things should not happen to good people");
	    return;
	}
	if (!validateExecMacArrayMessage(fc) || !fc.checkAuthenticationDigest())
	    BFT.Debug.kill("Bad authentication");

	String fetching = fc.getSeqNo()+
	    " {"+fc.getEntry().getClient()+","+fc.getEntry().getRequestId()+" }"; 
	fetch( fc.getSeqNo(), fc.getEntry(), fc.getSendingReplica());
    }



    protected void fetch(long seqno, Entry ent,  int sender){
	fetch(seqno, (int) ent.getClient(),
	      (int)ent.getRequestId(),
	      ent, sender);
    }
    protected void fetch(long seqno, int client, long reqId,
			 Entry ent, int sender ){
	int index = 
	    (int)(baseIntervalIndex + (seqno - baseSequenceNumber()) / BFT.order.Parameters.checkPointInterval);
	index = index % intervals.length;
	Entry entry;
	synchronized(clientLocks[client]){
	    entry = intervals[index].getRequest(client,
						      reqId);
	    if(entry == null) {
		entry = pendingClientEntries[client];
		
	    }
	}
	if (entry == null || ! entry.matches(ent.getDigest()) ) {
	    if (entry == null)
		System.out.println("\t nada -- failed to fetch <"+client+","+
				   reqId+"> for "+sender+" at "+seqno);
	    else
		System.out.println("\t wrong -- failed to fetch <"+client+","+
				   reqId+"> for "+sender+" at "+seqno);
	    FetchDenied fd = new FetchDenied(seqno, ent, getMyFilterIndex());
	    authenticateExecMacMessage(fd, sender);//, fc.getSendingReplica());
	    sendToExecutionReplica(fd.getBytes(), 
				   sender);
	    
	    return;
	}
	
	// 	System.out.println("successfully fetched "+fetching+" for "+fc.getSendingReplica());
	// 	System.out.println("forwarding "+seqno+" from "+fc.getEntry().getClient()+" to "+fc.getSendingReplica());
	ForwardCommand fwd = new ForwardCommand(seqno,
						entry, getMyFilterIndex());
	authenticateExecMacMessage(fwd,sender);
	sendToExecutionReplica(fwd.getBytes(), sender);
	
    }


    public void handle(byte[] vmbbytes){
	VerifiedMessageBase vmb = MessageFactory.fromBytes(vmbbytes);
	switch(vmb.getTag()){
	case MessageTags.ClientRequest: process((ClientRequest) vmb); return;
	case MessageTags.BatchCompleted: process((BatchCompleted) vmb); return;
	case MessageTags.FetchCommand: process((FetchCommand) vmb); return;
	case MessageTags.CPUpdate: process((CPUpdate) vmb); return;
	default:Debug.kill( new RuntimeException("filter does not handle message "+vmb.getTag()));
	}
    }


    protected long baseSequenceNumber(){
	return intervals[baseIntervalIndex].getBaseSequenceNumber();
    }

    protected long maxSequenceNumber(){
	return baseSequenceNumber() + intervals.length * BFT.order.Parameters.checkPointInterval;
    }

    public void start(){
	//super.start();
    }


    public static void main(String[] args){

	if(args.length < 2) {
	    BFT.Parameters.println("Usage: java BFT.filter.FilterBaseNode <id> <config_file>");
	    System.exit(0);
	}
	//Security.addProvider(new de.flexiprovider.core.FlexiCoreProvider());

	FilterBaseNode csbn = 
	    new FilterBaseNode(args[1], Integer.parseInt(args[0]));

	NetworkWorkQueue nwq = new NetworkWorkQueue();
	NettyTCPNetwork orderNet = new NettyTCPNetwork(Role.ORDER, csbn.getMembership(), nwq);
	csbn.setNetwork(orderNet);
	//netty//net.start();
	//	Listener lo = new Listener(net, nwq);
	Thread mlat = null;
	Thread mlbt = null;
	Thread mlct = null;;
	if(BFT.Parameters.doLogging) {
	    MsgLogQueue mlq = new MsgLogQueue();
	    csbn.setLoggerQueue(mlq);
	    MsgLogger MLA = new MsgLogger(0, mlq, csbn);
	    MsgLogger MLB = new MsgLogger(1, mlq, csbn);
	    MsgLogger MLC = new MsgLogger(2, mlq, csbn);
	    mlat = new Thread(MLA);
	    mlbt = new Thread(MLB);
	    mlct = new Thread(MLC);
	}
	NettyTCPNetwork clientNet = new NettyTCPNetwork(Role.CLIENT, csbn.getMembership(), nwq);
	csbn.setNetwork(clientNet);
	//netty//net.start();
	//netty//Listener lc = new Listener(net, nwq);
	
	NettyTCPNetwork execNet = new NettyTCPNetwork(Role.EXEC, csbn.getMembership(), nwq);
	csbn.setNetwork(execNet);
	//netty//net.start();
	NetworkWorkQueue execnetwq = new NetworkWorkQueue();
	//	Listener le = new Listener(net, execnetwq);
	//netty//Listener le = new Listener(net, nwq);
	//netty//Thread ltc = new Thread(lc);
	//	Thread lto = new Thread(lo);
	//netty//Thread lte = new Thread(le);
	CombinedWorker w = new CombinedWorker(nwq, csbn);
	//Worker w = new Worker(nwq, csbn);
	//ExecWorker ew = new ExecWorker(execnetwq, csbn);
	Thread wt = new Thread(w);
	//Thread wt2 = new Thread(ew);
	csbn.start();
	wt.start();
	//wt2.start();
// 	w = new Worker(nwq, csbn);
// 	wt = new Thread(w);
// 	wt.start();
	if (BFT.Parameters.doLogging) {
	    mlat.start();
	    mlbt.start();
	    mlct.start();
	}
	//netty//ltc.start();
	//netty//lte.start();

	csbn.start();
    execNet.start();
    clientNet.start();
    orderNet.start();
    }


}

