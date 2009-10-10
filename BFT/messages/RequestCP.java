// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;

import BFT.Parameters;

public class RequestCP extends MacArrayMessage{
    public RequestCP( long seq, long sender){
	super(MessageTags.RequestCP, 
	      computeSize(), sender,
	      Parameters.getExecutionCount());

	seqNo = seq;

	// now lets get the bytes
	byte[] bytes = getBytes();

	// copy the sequence number over
	byte[] tmp = UnsignedTypes.longToBytes(seqNo);
	int offset = getOffset();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }


    public RequestCP(byte[] bytes){
	super(bytes);
	if (getTag() != MessageTags.RequestCP)
	    throw new RuntimeException("invalid message Tag: "+getTag());
		
	// pull the sequence number
	byte[] tmp = new byte[4];
	int offset = getOffset();
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	seqNo = UnsignedTypes.bytesToLong(tmp);
	
	if (offset != bytes.length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");
    }


    protected long seqNo;

    public long getSendingReplica(){
	return getSender();
    }

    public long getSequenceNumber(){
	return seqNo;
    }

    private static int computeSize(){
	return  MessageTags.uint32Size;
    }

    
    public String toString(){
	return "<REQ-CP, "+super.toString()+", seqNo:"+seqNo+">";
    }

    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	RequestCP vmb = 
	    new RequestCP( 1, 2);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	RequestCP vmb2 = 
	    new RequestCP(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());

	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte) (tmp[i] * tmp[i]);

	vmb = new RequestCP( 134,8);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	 vmb2 = new RequestCP(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
 
	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));

   }

}