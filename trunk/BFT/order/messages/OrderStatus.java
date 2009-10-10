// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.order.messages.MessageTags;

import BFT.messages.VerifiedMessageBase;

import BFT.Parameters;

/**
 
 **/
public class OrderStatus extends MacArrayMessage{


    public OrderStatus (long view, long c, long p, long pp, int sendingReplica){
	super(MessageTags.OrderStatus, computeSize(), sendingReplica,
	      Parameters.getExecutionCount());
	viewNo = view;
	comSeqNo = c;
	prepSeqNo = p;
	ppSeqNo = pp;
	
	if (c > p || p > pp || c > pp)
	    BFT.Debug.kill(c+" "+p+" "+pp+" should be non-decreasing");

	int offset = getOffset();
	byte[] bytes = getBytes();
	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(viewNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the committed  sequence number
	tmp = UnsignedTypes.longToBytes(comSeqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the prepared sequencenumber
	tmp = UnsignedTypes.longToBytes(prepSeqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the preprepared sequence number
	tmp = UnsignedTypes.longToBytes(ppSeqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

    }
    
    public OrderStatus(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.OrderStatus)
	    throw new RuntimeException("invalid message Tag: "+getTag());

	int offset = getOffset();
	byte[] tmp;

	// read the view number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	viewNo = UnsignedTypes.bytesToLong(tmp);

	// read the c sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	comSeqNo = UnsignedTypes.bytesToLong(tmp);
	// read the psequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	prepSeqNo = UnsignedTypes.bytesToLong(tmp);
	// read the pp sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	ppSeqNo = UnsignedTypes.bytesToLong(tmp);



	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");
    }

    protected long viewNo;
    public long getView(){
	return viewNo;
    }

    protected long comSeqNo;
    public long getLastCommitted(){
	return comSeqNo;
    }

    protected long prepSeqNo;
    public long getLastPrepared(){
	return prepSeqNo;
    }

    protected long ppSeqNo;
    public long getLastPrePrepared(){
	return ppSeqNo;
    }
    

    public long getSendingReplica(){
	return getSender();
    }


    /** computes the size of the bits specific to Commit **/
    private static int computeSize(){
	int size =  MessageTags.uint32Size + MessageTags.uint32Size + MessageTags.uint32Size + MessageTags.uint32Size;
	return size;
    }


}