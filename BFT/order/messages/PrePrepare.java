// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.messages.NonDeterminism;
import BFT.messages.HistoryDigest;
import BFT.messages.SignedRequestCore;
import BFT.order.messages.MessageTags;
import BFT.messages.Digest;


import BFT.messages.VerifiedMessageBase;

import BFT.Parameters;

/**
 
 **/
public class PrePrepare extends MacArrayMessage{
    
    public  PrePrepare(long view, long seq, HistoryDigest hist,
			   RequestBatch batch, NonDeterminism non,
			   Digest cp, int sendingReplica){
	super(MessageTags.PrePrepare,
	      computeSize(view, seq, hist, batch, non, cp),
	      sendingReplica,
	      Parameters.getOrderCount());

	viewNo = view;
	seqNo = seq;
	history = hist;
	reqBatch = batch;
	nondet = non;
	cpHash = cp;
	
	cleanCount = 0;
	
	int offset = getOffset();
	byte[] bytes = getBytes();
	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(viewNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the sequence number
	tmp = UnsignedTypes.longToBytes(seqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the history
	tmp = history.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];


	// place the nondeterminism
	// size first
	tmp = UnsignedTypes.longToBytes(nondet.getSize());
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// now the nondet bytes
	tmp = non.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	tmp = cpHash.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];


	// place the size of the batch
	tmp = UnsignedTypes.intToBytes(batch.getSize());
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the number of entries in the batch
	tmp = UnsignedTypes.intToBytes(batch.getEntries().length);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	

	// place the batch bytes
	tmp = batch.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }

    public PrePrepare(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.PrePrepare)
	    throw new RuntimeException("invalid message Tag: "+getTag());
	int offset = getOffset();
	byte[] tmp;

	// read the view number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	viewNo = UnsignedTypes.bytesToLong(tmp);

	// read the sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqNo = UnsignedTypes.bytesToLong(tmp);

	// read the history
	tmp = new byte[HistoryDigest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	history = HistoryDigest.fromBytes(tmp);

	// read the non det size
	tmp = new byte[4];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int nondetSize = (int)(UnsignedTypes.bytesToLong(tmp));
	// read the nondeterminism
	tmp = new byte[nondetSize];
	for(int i = 0; i < tmp.length; i++, offset++){
	    tmp[i] = bits[offset];
	}
	nondet = new NonDeterminism(tmp);

	// read the checkpoint hash
	tmp = new byte[Digest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	cpHash = Digest.fromBytes(tmp);

	// read the batch size
	tmp = new byte[2];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int batchSize = (int) (UnsignedTypes.bytesToInt(tmp));

	// read the number of entries in the batch
	tmp = new byte[2];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int count = (int) (UnsignedTypes.bytesToInt(tmp));

	// read the batch bytes
	tmp = new byte[batchSize];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	reqBatch = new RequestBatch(tmp, count);

	if (offset != getBytes().length-getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");
	
    }

    protected long viewNo;
    public long getView(){
	return viewNo;
    }

    protected long seqNo;
    public long getSeqNo(){
	return seqNo;
    }
    
    protected HistoryDigest history;
    public HistoryDigest getHistory(){
	return history;
    }

    protected RequestBatch reqBatch;
    public RequestBatch getRequestBatch(){
	return reqBatch;
    }

    protected NonDeterminism nondet;
    public NonDeterminism getNonDeterminism(){
	return nondet;
    }

    protected Digest cpHash;
    public Digest getCPHash(){
	return cpHash;
    }

    public long getSendingReplica(){
	return getSender();
    }

    public boolean equals(PrePrepare nb){
	return super.equals(nb) && viewNo == nb.viewNo && seqNo == nb.seqNo &&
	    history.equals(nb.history) && cpHash.equals( nb.cpHash) &&
	    nondet.equals(nb.nondet) && reqBatch.equals(nb.reqBatch);
    }

    /** computes the size of the bits specific to PrePrepare **/
    private static int computeSize(long view, long seq, HistoryDigest h, 
			    RequestBatch batch, NonDeterminism non, 
			    Digest cp){
	int size =  MessageTags.uint32Size + MessageTags.uint32Size +
	    HistoryDigest.size() + MessageTags.uint16Size + MessageTags.uint16Size + 
	    non.getSize() + Digest.size() + MessageTags.uint16Size + 
	    MessageTags.uint16Size + batch.getSize();
	return size;
    }


    public String toString(){
	return "<PP, "+getView()+", "+getSeqNo()+", "+history+", "+
	    nondet+", "+reqBatch+">";
    }

    public static void main(String args[]){
	SignedRequestCore[] entries = new SignedRequestCore[1];
	byte[] tmp = new byte[2];
	tmp[0] = 1;
	tmp[1] = 23;
	entries[0] = new SignedRequestCore(2,3,tmp);
	HistoryDigest hist = new HistoryDigest(tmp);
	RequestBatch batch = new RequestBatch(entries);
	NonDeterminism non = new NonDeterminism(12341, 123456);
	
	PrePrepare vmb = 
	    new PrePrepare(43, 234, hist, batch, non, hist, 3);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	PrePrepare vmb2 = 
	    new PrePrepare(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	vmb = new PrePrepare(134,8, hist, batch, non, hist, 1);
	//System.out.println("\ninitial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new PrePrepare(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
    int cleanCount;
    
    public synchronized void rcCleaned() {
    	cleanCount++;
		//System.err.println("CV SLAVE::" + cleanCount + ":" + this.getRequestBatch().getEntries().length);
    	this.notifyAll();
    }
    
    public synchronized void rcMasterCleaned() {
    	cleanCount++;
    	try {
    		//System.err.println("CV MASTER::" + cleanCount + ":" + this.getRequestBatch().getEntries().length);
			while (cleanCount < this.getRequestBatch().getEntries().length) {
			    //System.out.println("waiting on this message");
			    System.out.println("waiting on a master cleaner");
				this.wait();
			}
		} 
    	catch (InterruptedException e) {

		}
    }
}