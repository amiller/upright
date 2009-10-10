// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;

/** 
 * Message sent from the order to execution node instructing the
 * execution node to load a specific checkpoint.
 **/
public class LoadCPMessage extends MacMessage{
    public LoadCPMessage(byte[] tok, long seq, long sender){
	super(MessageTags.LoadCPMessage, 
	      computeSize(tok), sender);

	seqNo = seq;
	token = tok;

	// now lets get the bytes
	byte[] bytes = getBytes();

	// copy the sequence number over
	byte[] tmp = UnsignedTypes.longToBytes(seqNo);
	int offset = getOffset();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// copy the token size over
	tmp = UnsignedTypes.longToBytes(tok.length);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	
	// copy the token itself over
	for (int i = 0; i < tok.length; i++, offset++)
	    bytes[offset] = tok[i];
    }


    public LoadCPMessage(byte[] bytes){
	super(bytes);
	if (getTag() != MessageTags.LoadCPMessage)
	    throw new RuntimeException("invalid message Tag: "+getTag());

	
	// pull the sequence number
	byte[] tmp = new byte[4];
	int offset = getOffset();
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	seqNo = UnsignedTypes.bytesToLong(tmp);
	
	// pull the token size out
	tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	long size = UnsignedTypes.bytesToLong(tmp);

	// pull the command out
	token = new byte[(int)size];
	for (int i = 0; i < size; i++, offset++)
	    token[i] = bytes[offset];

	if (offset != bytes.length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");

    }


    protected byte[] token;
    protected long seqNo;

    public long getSendingReplica(){
	return getSender();
    }

    public byte[] getToken(){
	return token;
    }

    public long getSequenceNumber(){
	return seqNo;
    }

    public boolean matches(VerifiedMessageBase vmb){
	LoadCPMessage lcp = (LoadCPMessage) vmb;
	boolean res = seqNo == lcp.seqNo && token.length == lcp.token.length;
	for (int i = 0; res && i < token.length; i++)
	    res = res && token[i] == lcp.token[i];
	return res;
    }


    private static int computeSize(byte[] tok){
	return tok.length + MessageTags.uint32Size + MessageTags.uint32Size;
    }


    public String toString(){
	String com = "";
	for (int i = 0; i < 8 && i < token.length; i++)
	    com += token[i]+",";
	return "<LOADCP, "+super.toString()+", seqno:"+seqNo+", size:"+token.length+", bytes:"+com+">";
    }
    
    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	LoadCPMessage vmb = 
	    new LoadCPMessage(tmp, 1, 2);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	LoadCPMessage vmb2 = 
	    new LoadCPMessage(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());

	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte) (tmp[i] * tmp[i]);

	vmb = new LoadCPMessage(tmp, 134,8);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	 vmb2 = new LoadCPMessage(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());

	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));
    }

}