// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.messages.HistoryDigest;
import BFT.order.messages.MessageTags;

import BFT.messages.VerifiedMessageBase;

import BFT.Parameters;

/**
 
 **/
public class ConfirmView extends MacArrayMessage{


    public ConfirmView (long view, long seq, 
			HistoryDigest hist, int sendingReplica){
	super(MessageTags.ConfirmView, computeSize(hist), sendingReplica,
	      Parameters.getOrderCount());
	viewNo = view;
	seqNo = seq;
	history = hist;

	int offset = getOffset();
	//System.out.println(offset);
	byte[] bytes = getBytes();
	//System.out.println(bytes.length);
	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(viewNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	//System.out.println(tmp.length);
	// place the sequence number
	tmp = UnsignedTypes.longToBytes(seqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	//System.out.println(tmp.length);
	// place the history
	tmp = history.getBytes();
	//System.out.println(tmp.length);

	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }
    
    public ConfirmView(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.ConfirmView)
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
	history = HistoryDigest.fromBytes(tmp);
	
	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("invalid byte array");
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

    public long getSendingReplica(){
	return getSender();
    }

    public boolean matches(VerifiedMessageBase vmb){
	ConfirmView cv = (ConfirmView) vmb;
	boolean res =  getHistory().equals(cv.getHistory()) &&
	    viewNo == cv.getView() && seqNo == cv.getSeqNo();
	//System.out.println("confirm view matches: "+res);
	return res;
    }

    public boolean equals(ConfirmView nb){
	return matches(nb) && super.equals(nb);
    }

    /** computes the size of the bits specific to ConfirmView **/
    private static int computeSize( HistoryDigest h){
	int size =  MessageTags.uint32Size + MessageTags.uint32Size +
	    HistoryDigest.size();
	return size;
    }


    public String toString(){
	return "<CONF-VIEW, "+super.toString()+", view="+viewNo+
	    ", seqNo="+seqNo+", history="+history+", send="+getSender()+">";
    }

    public static void main(String args[]){
	byte[] tmp = new byte[2];
	tmp[0] = 1;
	tmp[1] = 23;
	HistoryDigest hist = new HistoryDigest(tmp);

	ConfirmView vmb = new ConfirmView(123, 534 , hist, 1);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	ConfirmView vmb2 = 
	    new ConfirmView(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	vmb = new ConfirmView(42, 123, hist, 2);
	//System.out.println("\ninitial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new ConfirmView(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
}