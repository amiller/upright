// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.Digest;
import BFT.messages.MacArrayMessage;
import BFT.messages.HistoryDigest;
import BFT.order.messages.MessageTags;
import BFT.messages.VerifiedMessageBase;

import BFT.Parameters;

/**
 
 **/
public class Prepare extends MacArrayMessage{


    public Prepare (long view, long seq, HistoryDigest pphash, int sendingReplica){
	super(MessageTags.Prepare, computeSize(pphash), sendingReplica,
	      Parameters.getOrderCount());
	viewNo = view;
	seqNo = seq;
	ppHash = pphash;

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
	tmp = ppHash.getBytes();

	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }
    
	
    public Prepare(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.Prepare)
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

	// read the Preprepare hash
	tmp = new byte[HistoryDigest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	ppHash = HistoryDigest.fromBytes(tmp);

	if (offset != getBytes().length - getAuthenticationSize())
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
    
    protected HistoryDigest ppHash;
    public HistoryDigest getPrePrepareHash(){
	return ppHash;
    }

    public long getSendingReplica(){
	return getSender();
    }

    public boolean equals(Prepare nb){
	return super.equals(nb) && viewNo == nb.viewNo && seqNo == nb.seqNo &&
	    ppHash.equals(nb.ppHash);
    }

    /** computes the size of the bits specific to Prepare **/
    private static int computeSize( HistoryDigest h){
	int size =  MessageTags.uint32Size + MessageTags.uint32Size +
	    Digest.size();
	return size;
    }

    public boolean matches(VerifiedMessageBase vmb){
	if (vmb != null && vmb.getTag() == MessageTags.Prepare){
	    Prepare p = (Prepare) vmb;
	    return p.ppHash.equals(ppHash) && seqNo == p.getSeqNo() &&
		p.viewNo == viewNo;
	}else if (vmb != null && vmb.getTag() == MessageTags.Commit){
	    Commit c = (Commit) vmb;
	    return c.ppHash.equals(ppHash) && seqNo == c.getSeqNo() &&
		c.getView() == getView();
	}else
	    return false;
    }

    public String toString(){
	return "<P, view="+viewNo+", seqNo="+seqNo+", ppHash="+ppHash+", send="+getSender()+">";
    }

    public static void main(String args[]){
	BFT.messages.SignedRequestCore[] entries = new BFT.messages.SignedRequestCore[1];
	byte[] tmp = new byte[2];
	tmp[0] = 1;
	tmp[1] = 23;
	entries[0] = new BFT.messages.SignedRequestCore(2,3,tmp);
	HistoryDigest hist = new HistoryDigest(tmp);
	RequestBatch batch = new RequestBatch(entries);
	BFT.messages.NonDeterminism non = 
	    new BFT.messages.NonDeterminism(12341, 123456);
	
	PrePrepare tmp2 = 
	    new PrePrepare(43, 234, hist, batch, non, hist, 3);
	Prepare vmb = new Prepare(43, 23, hist, 2);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	Prepare vmb2 = 
	    new Prepare(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	vmb = new Prepare(42, 123, hist, 2);
	//System.out.println("\ninitial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new Prepare(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
}