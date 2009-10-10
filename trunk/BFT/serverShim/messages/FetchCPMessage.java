// $Id$

package BFT.serverShim.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;

import BFT.Parameters;

public class FetchCPMessage extends MacArrayMessage{
    public FetchCPMessage( long seq, long sender){
	super(MessageTags.FetchCPMessage, 
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


    public FetchCPMessage(byte[] bytes){
	super(bytes);
	if (getTag() != MessageTags.FetchCPMessage)
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
	return "<LAST-EXEC, "+super.toString()+", seqNo:"+seqNo+">";
    }

    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	FetchCPMessage vmb = 
	    new FetchCPMessage( 1, 2);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	FetchCPMessage vmb2 = 
	    new FetchCPMessage(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());

	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte) (tmp[i] * tmp[i]);

	vmb = new FetchCPMessage( 134,8);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	 vmb2 = new FetchCPMessage(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
 
	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));

   }

}