// $id: CheckPointState.java 3073 2009-03-08 06:56:58Z aclement $

package BFT.order.statemanagement;

import BFT.messages.HistoryDigest;
import BFT.messages.Digest;
import BFT.messages.NextBatch;
import BFT.messages.CommandBatch;
import BFT.messages.Entry;
import BFT.messages.MessageTags;
import BFT.messages.CertificateEntry;

import BFT.order.OrderBaseNode;

import BFT.Debug;

public class CheckPointState{


    protected OrderedEntry[] orderedRequestCache;
    protected long[] retransDelay;
    protected byte[] execCPToken; // 
    protected long baseSeqNo; // first sequence number of this checkpoint

    protected long currentSeqNo;  // next sequence number to be
				  // executed in this checkpoint.  if
				  // execcptoken is not null, then it
				  // corresponds to executing
				  // everything prior to this sequence
				  // number
    protected HistoryDigest history; // should always reflect the
				     // history up to and including
				     // currentSeqNo - 1
    protected long currentTime;  // current time
    protected boolean committed; 

    protected Digest stableDigest;

    public CheckPointState(int clients){
	baseSeqNo = 0;
	orderedRequestCache = new OrderedEntry[clients];
	retransDelay = new long[clients];
	for (int i = 0; i < clients; i++){
	    orderedRequestCache[i] = new OrderedEntry();
	    retransDelay[i] = baseDelay;
	}
	currentSeqNo = 0;
	history = new HistoryDigest();
	execCPToken = null;
	stableDigest = null;
	committed = false;
	currentTime = 0;
    }

    public CheckPointState(byte[] bytes){
	this(bytes, 0);
    }

    public CheckPointState(byte[] bytes, int offset){
	byte[] tmpint = new byte[MessageTags.uint16Size];
	byte[] tmplong = new byte[MessageTags.uint32Size];

	// read the history
	byte tmp[] = new byte[HistoryDigest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	history = HistoryDigest.fromBytes(tmp);

	for (int i = 0; i < tmplong.length; i++, offset++)
	    tmplong[i] = bytes[offset];
	currentTime = BFT.util.UnsignedTypes.bytesToLong(tmplong);

	// and now the sequence number
	for (int i = 0; i< tmplong.length; i++, offset++)
	    tmplong[i] = bytes[offset];
	currentSeqNo = BFT.util.UnsignedTypes.bytesToLong(tmplong);

	// and the base
	for (int i = 0; i< tmplong.length; i++, offset++)
	    tmplong[i] = bytes[offset];
	baseSeqNo = BFT.util.UnsignedTypes.bytesToLong(tmplong);
	if ((baseSeqNo) % BFT.order.Parameters.checkPointInterval != 0)
	    throw new RuntimeException("invalid base for a checkpoint");

	for (int i = 0; i < tmpint.length; i++, offset++)
	    tmpint[i] = bytes[offset];
	
	// read the number of entries
	int size = BFT.util.UnsignedTypes.bytesToInt(tmpint);
	orderedRequestCache = new OrderedEntry[size];
	// read the entries
	for (int i = 0; i < orderedRequestCache.length; i++){
	    for (int j = 0; j < tmplong.length; j++, offset++)
		tmplong[j] = bytes[offset];
	    long req = BFT.util.UnsignedTypes.bytesToLong(tmplong);
	    for (int j = 0; j < tmplong.length; j++, offset++)
		tmplong[j] = bytes[offset];
	    long seqno = BFT.util.UnsignedTypes.bytesToLong(tmplong);
	    orderedRequestCache[i] = new OrderedEntry(req, seqno);
	}
	
	execCPToken = new byte[bytes.length-offset];
	for (int i = 0; offset< bytes.length; i++, offset++)
	    execCPToken[i] = bytes[offset];
	
	retransDelay = new long[orderedRequestCache.length];
	for (int i = 0; i < retransDelay.length; i++)
	    retransDelay[i] = baseDelay;
 
    }
    
    public byte[] getBytes(){
	int byteSize = MessageTags.uint16Size+
	    orderedRequestCache.length * // size of cache 
	    (MessageTags.uint32Size + MessageTags.uint32Size) + 
	    execCPToken.length + // size of exec token
	    Digest.size() + // size of the history
	    MessageTags.uint32Size+ // current time
	    MessageTags.uint32Size+ // base sequence number
	    MessageTags.uint32Size; // last sequence number
	int offset = 0;
	byte[] tmp = new byte[byteSize];
	byte[] tmplong ;
	// dump the history
	tmplong = history.getBytes();
	for (int i = 0; i <tmplong.length; i++, offset++)
	    tmp[offset] = tmplong[i];

	// dump the time
	tmplong = BFT.util.UnsignedTypes.longToBytes(currentTime);
	for (int i = 0; i < tmplong.length; i++, offset++)
	    tmp[offset] = tmplong[i];

	// dump the current sequence number
	tmplong = BFT.util.UnsignedTypes.longToBytes(currentSeqNo);
	for (int i = 0; i <tmplong.length; i++, offset++)
	    tmp[offset] = tmplong[i];

	// dump the current base sequence number
	tmplong = BFT.util.UnsignedTypes.longToBytes(baseSeqNo);
	for (int i = 0; i <tmplong.length; i++, offset++)
	    tmp[offset] = tmplong[i];
	byte[] tmpint = 
	    BFT.util.UnsignedTypes.intToBytes(orderedRequestCache.length);

	// dump the number of clients
	for (int j = 0; j< tmpint.length; j++, offset++)
	    tmp[offset] = tmpint[j];
	
	// dump each of the entries in cache
	for (int i = 0; i < orderedRequestCache.length; i++){
	    OrderedEntry to = orderedRequestCache[i];
	    tmplong = BFT.util.UnsignedTypes.longToBytes(to.getReqId());
	    for(int  j = 0; j < tmplong.length; j++, offset++){
		tmp[offset] = tmplong[j];
	    }
	    tmplong = BFT.util.UnsignedTypes.longToBytes(to.getSeqNo());
	    for(int  j = 0; j < tmplong.length; j++, offset++){
		tmp[offset] = tmplong[j];
	    }
	}

	// dump the execcptoken
	for (int i = 0; i < execCPToken.length; i++, offset++)
	    tmp[offset] = execCPToken[i];
	
	    
	
	return tmp;
    }
    
    


    public CheckPointState(CheckPointState cps){
	baseSeqNo = cps.currentSeqNo;
	if ((baseSeqNo) % BFT.order.Parameters.checkPointInterval != 0)
	    throw new RuntimeException("invalid base sequence number");
	currentSeqNo = cps.currentSeqNo;
	history = cps.history;
	currentTime = cps.currentTime;
	stableDigest = null;
	execCPToken = null;
	orderedRequestCache = 
	    new OrderedEntry[cps.orderedRequestCache.length];
	retransDelay = new long[orderedRequestCache.length];
	for (int i = 0; i < orderedRequestCache.length; i++){
	    orderedRequestCache[i] = cps.orderedRequestCache[i];
	    retransDelay[i] = cps.retransDelay[i];
	}
    }

    public int getSize(){
	if (!isStable())
	    Debug.kill("can only get size of a stable checkpoint");
	return getBytes().length;
    }

    public HistoryDigest getHistory(){
	return history;
    }

    public long getBaseSequenceNumber(){
	return baseSeqNo;
    }

    public long getCurrentTime(){
	return currentTime;
    }

    public void setCurrentTime(long t){
	currentTime = t;
    }

    public long getCurrentSequenceNumber(){
	return currentSeqNo;
    }

    public long getLastOrdered(long client){
	return orderedRequestCache[(int)(client)].getReqId();
    }
    
    public long getLastOrderedSeqNo(long client){
	return orderedRequestCache[(int)(client)].getSeqNo();
    }


    public void addExecCPToken(byte[] cp, long seqNo){
	if (execCPToken != null)
	    Debug.kill("Already have a checkpoint");
	if (seqNo % BFT.order.Parameters.checkPointInterval != 0)
	    Debug.kill("Not a checkpoint interval "+seqNo);

	//	System.out.println("adding: "+seqNo+" mybase: "+getBaseSequenceNumber()+" current: "+getCurrentSequenceNumber());
	execCPToken = cp;
    }

    public byte[] getExecCPToken(){
	return execCPToken;
    }
    

    /**
       updates the ordered cache for each client that has an operation
       in this set
     **/
    public void addNextBatch(CommandBatch b, long seq, HistoryDigest h,
			     long time){
	if (seq != currentSeqNo)
	    Debug.kill("invalid batch update");
	if (seq >= getBaseSequenceNumber() + BFT.order.Parameters.checkPointInterval)
	    Debug.kill("invalid addition of a batch");
	Entry[] entries = b.getEntries();
	// copy on write to make a in memory copy
	for (int i = 0;i < entries.length; i++){
	    if (orderedRequestCache[(int)(entries[i].getClient())].getSeqNo() <  baseSeqNo){
		orderedRequestCache[(int)(entries[i].getClient())] =
		    new OrderedEntry(entries[i].getRequestId(), seq);
	    }
	    else
		orderedRequestCache[(int)(entries[i].getClient())].set(entries[i].getRequestId(), seq);
	    
	    retransDelay[(int)(entries[i].getClient())] = baseDelay;
	    //System.out.println("retransmit delay: "+retransDelay[(int)(entries[i].getClient())]);
	}
	history = h;
	setCurrentTime(time);
	currentSeqNo++;
	    
    }
    long baseDelay = 1000;

    public long getRetransmitDelay(int client){
	return retransDelay[client];
    }

    public void updateRetransmitDelay(int client){
	retransDelay[client] *= 2;
	if (retransDelay[client] > baseDelay)
	    retransDelay[client] = baseDelay;
	//System.out.println("retransmit delay: "+retransDelay[client]);
    }

    public void addNextBatch(CertificateEntry cert, long seq){
	addNextBatch(cert.getCommandBatch(), seq, cert.getHistoryDigest(),
		     cert.getNonDeterminism().getTime());
    }

    public void addNextBatch(NextBatch nb){
	addNextBatch(nb.getCommands(), nb.getSeqNo(), nb.getHistory(), nb.getNonDeterminism().getTime());
    }


    protected boolean marking = false;
    public  void makeStable(){
	if (!marking)
	    Debug.kill("should be marking stable before making stable");
	
	if (isStable())
	    Debug.kill("should not be making stable if its already stable");

	if (getCurrentSequenceNumber() % 
	    BFT.order.Parameters.checkPointInterval != 0)
	    Debug.kill(new RuntimeException("Must be at a checkpoint "+
					    "interval to be made stable"));
	// in 'stable' systems this requires a write to disk

	if (hasExecCP()){
	    stableDigest = new Digest(getBytes());
// 	    System.out.println("**********making the cp stable! ");
// 	    System.out.println(this);


	} else
	    Debug.kill(new RuntimeException("attempting to mark a checkpoint "+
					    "stable without an app cp token"));
    }
    

    public  void commit(){
	if (!isStable())
	    Debug.kill("Can only commit a CP if its stable!");
//     	while(!isStable()) {
// 	    try {
// 		System.out.println("going to wait in commit()");
// 		try{
// 		    throw new RuntimeException("uh oh");
// 		}catch(Exception e){System.out.println(e);
// 		    e.printStackTrace();}
// 		wait();
// 		System.out.println("finished waitin gin commit()");
// 	    } catch (InterruptedException e) {
// 	    }
//     	}
    	committed = true;
    }

    public boolean hasExecCP(){
	return execCPToken != null;
    }


    public   boolean isStable(){
	return stableDigest != null;
    }

    
    public  boolean isCommitted(){
	if (committed && !isStable())
	    Debug.kill( new RuntimeException("cannot be committed "+
					     "without beign stable"));
	return committed;
    }

    public  Digest getStableDigest(){
	return stableDigest;
    }

    public Digest getDigest(){
	Digest tmp = stableDigest;
	if (tmp == null && execCPToken != null)
	    tmp =  new Digest(getBytes());
	return tmp;
    }

    public boolean isMarkingStable(){
	return marking;
    }

    public void markingStable(){
	if (marking)
	    Debug.kill("already marking stable");
	marking = true;
    }

    public String toString(){
	String ret = "base: "+getBaseSequenceNumber()+
	    " next:"+getCurrentSequenceNumber()+" @ "+currentTime+"\n";
	for (int i = 0; i < orderedRequestCache.length; i++){
	    ret += "\t"+i+":\t"+orderedRequestCache[i];
	    if ((i+1)% 4 == 0) ret += "\n";
	}
	ret += "\n\texeccp:";
	for (int i = 0; execCPToken != null && i < +execCPToken.length; i++)
	    ret+= execCPToken[i]+" ";
	ret += "\n\thistory:";
	for (int i = 0 ;i < history.getBytes().length; i++)
	    ret+= +history.getBytes()[i]+" ";
	if (stableDigest != null)
	    ret += "\n\tstableDig:"+stableDigest;
	return ret;
    }

    protected void finalize(){
	//System.out.println("Garbage collecting checkpoint state");
    }

}

class OrderedEntry{
    protected long reqId;
    protected long seqNo;

    OrderedEntry(){
	reqId = 0;
	seqNo = 0;
    };

    public OrderedEntry(long r, long s){
	reqId = r;
	seqNo = s;
    }

    public void set(long r, long s){
	reqId = r;
	seqNo = s;
    }

    public long getReqId(){
	return reqId;
    }
    public long getSeqNo(){
	return seqNo;
    }
    public String toString(){
	return ""+getReqId()+" @ "+getSeqNo();
    }
}