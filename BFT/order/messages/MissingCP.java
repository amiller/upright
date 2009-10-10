// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.messages.Digest;
import BFT.order.messages.MessageTags;

import BFT.messages.VerifiedMessageBase;

/**
 
 **/
public class MissingCP extends MacArrayMessage{


    public MissingCP (long seqno, Digest cpDig, int sendingReplica){
	super(MessageTags.MissingCP, computeSize(), sendingReplica,
	      BFT.Parameters.getOrderCount());
	seqNo = seqno;
	cpDigest = cpDig;

	int offset = getOffset();
	byte[] bytes = getBytes();
	// place the sequence number
	byte[] tmp = UnsignedTypes.longToBytes(seqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the cpDigest
	tmp = cpDig.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }
    
    public MissingCP(byte[] bits){
	super(bits);
	int offset = getOffset();
	byte[] tmp;


	// read the sequence number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqNo = UnsignedTypes.bytesToLong(tmp);

	// read the digest
	tmp = new byte[Digest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	cpDigest = Digest.fromBytes(tmp);

	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("invalid byte array");
    }

    protected long seqNo;
    public long getSeqNo(){
	return seqNo;
    }

    protected Digest cpDigest;
    public Digest getCPDigest(){
	return cpDigest;
    }
    

    public int getSendingReplica(){
	return (int) getSender();
    }


    /** computes the size of the bits specific to MissingCP **/
    private static int computeSize( ){
	int size =  MessageTags.uint32Size + Digest.size();
	return size;
    }


    
}