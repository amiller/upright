// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacMessage;
import BFT.messages.HistoryDigest;
import BFT.order.messages.MessageTags;

import BFT.messages.VerifiedMessageBase;

import BFT.Parameters;

/**
 
 **/
public class StartView extends MacMessage{


    public StartView (long view, long seq, 
			HistoryDigest hist, int sendingReplica){
	super(MessageTags.StartView, computeSize(hist), sendingReplica);

	viewNo = view;
	seqNo = seq;
	history = hist;

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
    }
    
    public StartView(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.StartView)
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

    public int getSendingReplica(){
	return (int)(getSender());
    }


    /** computes the size of the bits specific to StartView **/
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

	StartView vmb = new StartView(123, 534 , hist, 1);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	StartView vmb2 = 
	    new StartView(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	vmb = new StartView(42, 123, hist, 2);
	//System.out.println("\ninitial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new StartView(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
}