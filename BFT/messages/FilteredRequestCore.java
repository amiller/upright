// $Id$

package BFT.messages;

import BFT.messages.MessageTags;
import BFT.messages.MacSignatureMessage; 
import BFT.util.UnsignedTypes;

/**
   Request message sent from the client to the order node.
 **/
public class FilteredRequestCore extends MacSignatureMessage implements RequestCore{


    /**
       Construct that accepts specific message fields.  This
       constructor builds up the message byte representation starting
       from where VerifiedMessageBase leaves off.
     **/
    public FilteredRequestCore(long client, long sequence, 
		   byte[] com){
	this(new Entry(client, sequence, com));
    }
    


   int sendingRep;
   public void setSendingReplica(int i){
	   sendingRep = i;
   
   }
    
   	public int getSendingReplica(){
   		return sendingRep;
   	
   	}
   
    public FilteredRequestCore(Entry entry){
	super(tag(), computeSize(entry), BFT.Parameters.getFilterCount(),
	      BFT.Parameters.getOrderCount());
	
	// now lets get the bytes
	byte[] bytes = getBytes();
	int offset = getOffset();
	byte[] tmp = entry.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	this.entry = entry;

// 	// copy the sequence number over
// 	byte[] tmp = UnsignedTypes.longToBytes(sequence);
// 	int offset = getOffset();
// 	for (int i = 0; i < tmp.length; i++, offset++)
// 	    bytes[offset] = tmp[i];

// 	// copy the command size over
// 	tmp = UnsignedTypes.longToBytes(com.length);
// 	for (int i = 0; i < tmp.length; i++, offset++)
// 	    bytes[offset] = tmp[i];
	
// 	// copy the command itself over
// 	for (int i = 0; i < com.length; i++, offset++)
// 	    bytes[offset] = com[i];
    }

    /**
       Constructor accepting a byte representation of the message.
       Parses the byte representation to populate the class fields.
     **/
    public FilteredRequestCore(byte[] bytes){
	super(bytes);
	if (getTag() != MessageTags.FilteredRequestCore)
	    throw new RuntimeException("invalid message Tag: "+getTag());

	int offset = getOffset();

	Entry tmp = Entry.fromBytes(bytes, offset);
	entry = tmp;

	offset+=entry.getSize();
	
// 	// pull the request id out.
// 	byte[] tmp = new byte[4];
// 	for (int i = 0; i < 4; i++, offset++)
// 	    tmp[i] = bytes[offset];
// 	seqNo = UnsignedTypes.bytesToLong(tmp);
	
// 	// pull the command size out
// 	tmp = new byte[4];
// 	for (int i = 0; i < 4; i++, offset++)
// 	    tmp[i] = bytes[offset];
// 	long size = UnsignedTypes.bytesToLong(tmp);

// 	// pull the command out
// 	command = new byte[(int)size];
// 	for (int i = 0; i < size; i++, offset++)
// 	    command[i] = bytes[offset];
	
	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");
    }

    private Entry entry;

    /* (non-Javadoc)
	 * @see BFT.messages.RequestCore#getSendingClient()
	 */
    public int getSendingClient(){
	return (int) entry.getClient();
    }

    public long getSender(){
	return getSendingClient();
    }

    /* (non-Javadoc)
	 * @see BFT.messages.RequestCore#getRequestId()
	 */
    public long getRequestId(){
	return entry.getRequestId();
    }

    /* (non-Javadoc)
	 * @see BFT.messages.RequestCore#getCommand()
	 */
    public byte[] getCommand(){
	return entry.getCommand();
    }

    public Entry getEntry(){
	return entry;
    }
    
    /**
       Total size of the request message based on the definition in
       request.hh and verifiable_msg.hh
     **/
    static private int computeSize(Entry entry){
	return entry.getSize();
    }



    public static int tag(){
	return MessageTags.FilteredRequestCore;
    }


    public boolean equals(FilteredRequestCore r){
	boolean res = super.equals(r) && matches(r);
	return res;
    }

    public boolean matches(VerifiedMessageBase vmb){
	boolean res = vmb.getTag() == getTag();
	if (!res) 
	    return res;
	FilteredRequestCore rc = (FilteredRequestCore) vmb;
	res = res && entry.equals(rc.getEntry());
	return res;
    }

    public String toString(){
	String com = "";
	for (int i = 0; getCommand() != null && i < 8 && i < getCommand().length; i++)
	    com += getCommand()[i]+",";
	return "< REQ, "+super.toString()+", reqId:"+getRequestId()+", command:"+com+
	    ">";
    }

    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	FilteredRequestCore vmb = 
	    new FilteredRequestCore(1,0,tmp);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	FilteredRequestCore vmb2 = 
	    new FilteredRequestCore(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());

 	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte) (tmp[i] * tmp[i]);

	vmb = new FilteredRequestCore(134,8, tmp);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	 vmb2 = new FilteredRequestCore(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));
 
	//System.out.println("old.equals(new): "+vmb.equals(vmb2));
   
    }
}