package BFT.clientShim;

import BFT.BaseNode;

// outgoing messages
import BFT.messages.ClientRequest;
import BFT.messages.SignedRequestCore;
import BFT.messages.SimpleRequestCore;
import BFT.messages.RequestCore;
import BFT.messages.ReadOnlyRequest;

// incoming messages
import BFT.messages.Reply;
import BFT.messages.WatchReply;
import BFT.messages.ReadOnlyReply;


import BFT.messages.VerifiedMessageBase;
import BFT.messages.MessageTags;
import BFT.messages.Quorum;
import BFT.membership.Membership;
import BFT.MessageFactory;

import BFT.util.Role;

import BFT.Debug;

import java.util.Random;

public class ClientShimBaseNode extends BaseNode implements ClientShimInterface{

    protected long seqNo = 1;
    protected long readOnlySeqno = 1;
    protected Quorum<Reply> replies;
    protected Quorum<WatchReply> watchReplies;
    protected Quorum<ReadOnlyReply> readreplies;
    protected ClientGlueInterface glue;
    protected int resendBase;
    protected int readQuorumSize;
    protected int pendingRequestCount;
    protected int requestThreshold;


    public ClientShimBaseNode(String membership, int myId){
	super(membership, Role.CLIENT, myId);
	
	if (BFT.Parameters.linearizeReads)
	    readQuorumSize = BFT.Parameters.linearizedExecutionQuorumSize();
	else
	    readQuorumSize = BFT.Parameters.rightExecutionQuorumSize();

	// do whatever else is necessary
	replies = new Quorum<Reply>(BFT.Parameters.getExecutionCount(),
				    BFT.Parameters.rightExecutionQuorumSize(),
				    0);
	readreplies = new Quorum<ReadOnlyReply>(BFT.Parameters.getExecutionCount(),
						readQuorumSize,
						0);
	watchReplies = new Quorum<WatchReply>(BFT.Parameters.getExecutionCount(),
					      BFT.Parameters.rightExecutionQuorumSize(),
					      0);
	Random rand = new Random(System.currentTimeMillis());
	int resendBase = rand.nextInt(BFT.Parameters.getExecutionCount());
	pendingRequestCount = 0;
	requestThreshold = BFT.Parameters.getConcurrentRequestLimit();
    }

    public void setGlue(ClientGlueInterface g){
	glue = g;
    }

    public void enqueueRequest(byte[] operation){
	byte[] res = execute(operation);
	glue.returnReply(res);
    }

    public void enqueueReadOnlyRequest(byte[] op){
	byte[] res = executeReadOnlyRequest(op);
	glue.returnReply(res);

    }

    protected long readRetrans = 500;
    public synchronized byte[] executeReadOnlyRequest(byte[] op){
	while(pendingRequestCount >= requestThreshold){
	    try{
		wait(500);
	    }catch(Exception e){}
	}
	pendingRequestCount++;
	if (pendingRequestCount > requestThreshold){
	    BFT.Debug.kill("Too many outstanding requests: "+
			   pendingRequestCount+"/"+requestThreshold);
	}

	if (op.length > BFT.Parameters.maxRequestSize)
	    Debug.kill("operation is too big!");

	// 	System.err.println("forming Read only "+
	// 		      "request with sequence number:"+(readOnlySeqno+1));
	RequestCore origRC = null;
	if (!BFT.Parameters.filtered)
	    origRC = new SignedRequestCore(members.getMyId(),readOnlySeqno,
					   op);
	else
	    origRC = new SimpleRequestCore(members.getMyId(), readOnlySeqno,
					   op);
	// dont need to sign readonly requests
	//origRC.sign(getMyPrivateKey());
	ReadOnlyRequest req = new ReadOnlyRequest(members.getMyId(), origRC);
	readreplies.clear();
	ReadOnlyReply reply = null;
	authenticateExecMacArrayMessage(req);
	boolean resend = true;
	int count = 0;
	byte[] replybytes = null;
	long startTime = System.currentTimeMillis();
	while (replybytes == null && count < 10){
	    if (resend){
		//		System.err.println("retransmitting!");
		byte[] tmp = req.getBytes();

		for (int j = 0; j < readQuorumSize; j++){
		    int index = 
			(resendBase++) % BFT.Parameters.getExecutionCount();
		    sendToExecutionReplica(tmp, index);
		}
	    }
	    resend = true;
	    count++;
	    //	    System.err.println("Sending read only sequence number: "+readOnlySeqno);
	    try{
			//	System.err.println("waiting for readRetrans "+readRetrans+" "+System.currentTimeMillis());
		wait(readRetrans);
//				System.err.println("the retrans timer expired "+System.currentTimeMillis());
	    }catch(Exception e){
		e.printStackTrace();
		Debug.kill( new RuntimeException("interrupted!?"));
	    }//System.err.println("not waiting anymore");

	    if (readreplies.isComplete()){
		//		System.err.println(readreplies.getEntries());
		VerifiedMessageBase vmb[] = readreplies.getEntries();
		byte[][] options = new byte[vmb.length][];
		for (int i = 0; i < vmb.length; i++)
		    if (vmb[i] != null)
			options[i] = ((ReadOnlyReply)(vmb[i])).getCommand();
		reply = readreplies.getEntry();
		replybytes = glue.canonicalEntry(options);
		if (replybytes == null)
		    readreplies.clear();
	    }
	    // always consume the next client id available for this
	    // client if replies come back for a different client id
	    // than the one i sent, then i need to use a new client id
	    // in order to get a response
	    //	    System.err.println("looking for readOnlySeqno: "+readOnlySeqno);
	    if (reply != null 
		&& reply.getRequestId() > readOnlySeqno
		&& reply.getRequestId() != 0){
		// 		    System.err.println("resettin the sequence number");
		// 		    System.err.println("\t"+ readOnlySeqno+":"+reply.getRequestId());
		readOnlySeqno = reply.getRequestId()+1;
		//		    System.err.println("new readOnlySeqno: "+readOnlySeqno);
		RequestCore rc = null;
		if (!BFT.Parameters.filtered){
		    rc = new SignedRequestCore(req.getSender(),
					       readOnlySeqno,
					       op);
		}
		else
		    rc = new SimpleRequestCore(req.getSender(),
					       readOnlySeqno,
					       op);
		req = new ReadOnlyRequest(req.getSender(), rc);
		authenticateExecMacArrayMessage(req);
		readreplies.clear();
		reply = null;
	    } 
	    if (reply != null && reply.getRequestId() < readOnlySeqno){
		readreplies.clear();
		reply = null;
		//		System.err.println("not resending yet!");
		resend = false;
	    }else
		readRetrans *=2;
	    if (readRetrans > 1000){ count = 11; readRetrans = 1000;}
	}
        readOnlySeqno++;
	// if the retries failed, do the normal path
	if (replybytes == null){
	   // System.err.println("I give up, converting to regular");
	    readreplies.clear();
	    replybytes = execute(op);
	    readRetrans = 2*(System.currentTimeMillis()-startTime);
	    if (readRetrans < 200)
		readRetrans = 200;
	    pendingRequestCount--;
	    if (pendingRequestCount < 0)
		BFT.Debug.kill("pending request count should not be below 0: "+
			       pendingRequestCount);
	    return replybytes;
	}
	else{
	    readreplies.clear();
	    //	    System.err.println("returning: "+new String(reply.getCommand()));
	    readRetrans = 2*(System.currentTimeMillis() - startTime);
	    if (readRetrans < 200)
		readRetrans = 200;
	    pendingRequestCount--;
	    if (pendingRequestCount < 0)
		BFT.Debug.kill("pending request count should not be below 0: "+
			       pendingRequestCount);
	    notifyAll();
	    return replybytes;
	}
    }


    protected long retrans = 1000;
    public synchronized byte[] execute(byte[] operation){
	while(pendingRequestCount >= requestThreshold){
	    try{
		wait(500);
	    }catch(Exception e){}
	}
	pendingRequestCount++;
	if (pendingRequestCount > requestThreshold){
	    BFT.Debug.kill("Too many outstanding requests: "+
			   pendingRequestCount+"/"+requestThreshold);
	}

	if (operation.length > 60000)
	    Debug.kill("Operation is too big");

	RequestCore origRC;
	ClientRequest req;
	if (!BFT.Parameters.filtered){
	    origRC = new SignedRequestCore(members.getMyId(),seqNo,	operation);
	    ((SignedRequestCore)(origRC)).sign(getMyPrivateKey());
	    req = new ClientRequest(members.getMyId(), origRC);
	}
	else {
	    origRC = 
		new SimpleRequestCore(members.getMyId(), seqNo,
				      operation);
	    req = new ClientRequest(members.getMyId(), origRC, false);
	}
//	System.err.println("exec "+seqNo);
	//Debug.println("forming request with sequence number: "+(seqNo));

	replies.clear();
	Reply reply = null;
	if (!BFT.Parameters.filtered)
	    authenticateOrderMacArrayMessage(req);
	else
	    authenticateFilterMacArrayMessage(req);
	boolean resend = true;
	boolean firstsend = true;
	long start = System.currentTimeMillis();
	while (reply == null){
	    if (resend){
		Debug.profileStart("SND_TO_FILTER-ORDER");
		//				Debug.println("sending request: "+req+" "+req.getCore().getRequestId());
		if (!BFT.Parameters.filtered){
            //System.err.println("byte count: "+req.getBytes().length);
		    sendToAllOrderReplicas(req.getBytes());
        }		else if (!firstsend ) {
		    //		    System.err.println("Sending to everybody");
		    sendToAllFilterReplicas(req.getBytes());
		}
		else{
		    byte[] tmp = req.getBytes();
		    int index = members.getMyId();
		    int fc = BFT.Parameters.getFilterCount();
		    //		    System.err.println("sending to subset");
		    for (int i = 0; //i < BFT.Parameters.getFilterCount(); i++){
			 i < BFT.Parameters.mediumFilterQuorumSize(); i++){
			index = (index+1) %fc;
			sendToFilterReplica(tmp, index);
			//sendToFilterReplica(tmp, i);
		    }
		    firstsend = false;
		}
		//Debug.println("Sending sequence number: "+seqNo);
		Debug.profileFinis("SND_TO_FILTER-ORDER");
	    }
	    resend = true;
	    try{
		long startt = System.currentTimeMillis();
		//			    System.err.println("waiting for "+ retrans+" "+System.currentTimeMillis());
		wait(retrans);
		//		System.err.println("waited for "+ (System.currentTimeMillis() - startt));
	    }catch(Exception e){throw new RuntimeException("interrupted!?");}
	    if (replies.isComplete())
		reply = replies.getEntry();

	    // always consume the next client id available for this //
	    // client if replies come back for a different client id
	    // than the one i sent, then i need to use a new client id
	    // in order to get a response Debug.println("looking for
	    // seqno: "+seqNo);
			
	    if (reply != null 
		&& reply.getRequestId() > seqNo
		&& reply.getRequestId() != 0){
		//Debug.println("resettin the sequence number");
		//Debug.println("\t"+ seqNo+":"+reply.getRequestId());
		seqNo = reply.getRequestId()+1;
		//			    BFT.Parameters.println("new seqno: "+seqNo);
			    
		RequestCore rc;
		if (!BFT.Parameters.filtered){
		    rc = new SignedRequestCore(req.getSender(),seqNo,operation);
		    ((SignedRequestCore)rc).sign(getMyPrivateKey());
		    req = new ClientRequest(req.getSender(),
					    rc);
		}
		else {
		    rc = new SimpleRequestCore(req.getSender(), seqNo, operation);
		    req = new ClientRequest(req.getSender(),
					    rc, false);
		}
		//Debug.println("new seqno: "+seqNo);
		if (!BFT.Parameters.filtered)
		    authenticateOrderMacArrayMessage(req);
		else
		    authenticateFilterMacArrayMessage(req);
		replies.clear();
		reply = null;
	    } 
	    if (reply != null && reply.getRequestId() < seqNo){
		replies.clear();
		reply = null;
		resend = false;
	    }else
		retrans *= 2;
	    if (retrans > 4000)
		retrans = 4000;
	    //		retrans = 0;

	}
	//Debug.println("returning: "+reply.getCommand()+" bytes");
	retrans = 2*(System.currentTimeMillis() - start);
	if (retrans < 500)
	    retrans = 500;
	if (retrans > 4000){
	    retrans = 4000;
	}
	seqNo++;
	//System.err.println("NEW RETRANS: " + retrans);
	//	System.err.println("returning a response!");
	pendingRequestCount--;
	if (pendingRequestCount < 0)
	    BFT.Debug.kill("pending request count should not be below 0: "+
			   pendingRequestCount);
	return reply.getCommand();
    }


    public void handle(byte[] bytes){
	VerifiedMessageBase vmb = MessageFactory.fromBytes(bytes);
	switch(vmb.getTag()){
	case MessageTags.Reply: process( (Reply) vmb); return;
	case MessageTags.WatchReply: process( (WatchReply) vmb); return;
	case MessageTags.ReadOnlyReply: process((ReadOnlyReply) vmb); return;
	default: Debug.kill("WTF");
	}
    }


    synchronized protected void process(ReadOnlyReply rep){
	// Debug.println("=====\nprocessingReadonly reply "+rep);
	// 	    BFT.Debug.println("Got Reply from: "+rep.getSendingReplica());
	//Debug.println("req id:          "+rep.getRequestId());
	if (!validateExecMacMessage(rep))
	    throw new RuntimeException("reply mac did not authenticate");

	if (rep.getRequestId() < readOnlySeqno){
	    //System.err.println("discarding the older response");
	    return;
	}
	ReadOnlyReply rop = readreplies.getEntry();
	if(!readreplies.addEntry(rep)){
	     	    System.err.println("Didn't like the reply! Replaced");
	    // 	    //System.err.println(rop);
	    // 	    System.out.print("contents: ");
	    // 	    for (int i = 0; i < 20 && i < rop.getCommand(); i++)
	    // 		System.out.print(" "+rop.getCommand()[i]);
	    // 	    //System.err.println("\nwith");
	    // 	    //System.err.println(rep);
	    // 	    System.out.print("contents: ");
	    // 	    for (int i = 0; i < 20 && i < rep.getCommand(); i++)
	    // 		    System.out.print(" "+rep.getCommand()[i]);
	    // 	    //System.err.println("=====");
	}
	if (readreplies.isComplete())
	    notifyAll();
    }

    synchronized protected void process(Reply rep){
	//System.out.println("=====processing reply "+ rep.getRequestId() +" size " +rep.getCommand().length +" bytes from " + rep.getSendingReplica());
	//	System.err.println(rep);
	if (!validateExecMacMessage(rep))
	    throw new RuntimeException("reply mac did not authenticate");
	if (rep.getRequestId() < seqNo){
	    //	    Debug.println("\t\tDiscarding old reply");
	    return;
	}
	
	Reply olrep = replies.getEntry();
	if (!replies.addEntry(rep)){
	    Debug.println("replacing "+olrep);
	    for (int i = 0; i < 20 && i < olrep.getCommand().length; i++)
		System.out.print(" "+ olrep.getCommand()[i]);
	    Debug.println("\nwith      "+rep);
	    for (int i = 0; i < 20 && i < rep.getCommand().length; i++)
		System.out.print(" "+ rep.getCommand()[i]);
	    Debug.println("");
	    
	}
	//Debug.println("complete: "+replies.isComplete());
	if (replies.isComplete())
	    notifyAll();
    }

    protected void process(WatchReply rep){
	//Debug.println("=====processing watch reply "+new String(rep.getCommand()));
	//BFT.//Debug.println("\tGot watch Reply from: "+rep.getSendingReplica());
	//Debug.println("\treq id:          "+rep.getRequestId());
	if (!validateExecMacMessage(rep))
	    Debug.kill(new RuntimeException("reply mac did not authenticate"));
	if (!watchReplies.addEntry(rep))
	    glue.brokenConnection();
	else if (watchReplies.isComplete()){
	    glue.returnReply(watchReplies.getEntry().getCommand());
	    watchReplies.clear();
	}

    }


}
