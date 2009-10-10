// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.messages.MessageTags;

import BFT.Parameters;

/** message instructing the execution node to retransmit result of the
    last request executed for the specified client
**/ 
public class Retransmit extends MacArrayMessage{

    public Retransmit(long clientId, long seqno, long sender){
	super(tag(), computeSize(), sender, Parameters.getExecutionCount());
	client = clientId;
	this.seqno = seqno;
	byte[] bytes = getBytes();
	int offset = getOffset();
	// copy the client to the byte array
	byte[] tmp = UnsignedTypes.longToBytes(client);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// copy the seqno to the byte array
	tmp = UnsignedTypes.longToBytes(seqno);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	
    }

    public Retransmit(byte[] bytes){
	super(bytes);
	if (getTag() != MessageTags.Retransmit)
	    throw new RuntimeException("invalid message Tag: "+getTag());

	int offset = getOffset();
	// pull the client id off teh bytes
	byte[] tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	client = UnsignedTypes.bytesToLong(tmp);
	// pull the seqno
	tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	seqno = UnsignedTypes.bytesToLong(tmp);
	
	if (offset != bytes.length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");

    }

    protected long client;
    protected long seqno;
    
    static private int computeSize(){
	return MessageTags.uint32Size + MessageTags.uint32Size;
    }

    public long getClient(){
	return client;
    }

    public long getSequenceNumber(){
	return seqno;
    }

    public long getSendingReplica(){
	return getSender();
    }

    public static int tag(){
	return MessageTags.Retransmit;
    }

    public boolean equals(Retransmit ret){
	boolean res = super.equals(ret) && seqno == ret.seqno &&
	    client == ret.client;
	return res;
    }

    public boolean matches(VerifiedMessageBase vmb){
	Retransmit ret = (Retransmit) vmb;
	boolean res = seqno == ret.seqno && client == ret.client;
	return res;
    }

    public String toString(){
	return "< RETRANS, "+super.toString()+", client:"+client+
	    ", seqno: "+seqno+">";
    }

    public static void main(String args[]){
	byte[] tmp = new byte[8];

	Retransmit vmb = 
	    new Retransmit(1, 2,3);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	Retransmit vmb2 = 
	    new Retransmit(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());


	vmb = new Retransmit(134,8,3);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	 vmb2 = new Retransmit(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));

    
    }

}