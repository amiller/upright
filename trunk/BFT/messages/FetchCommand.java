// $Id

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.Debug;

public class FetchCommand extends MacArrayMessage{

    protected Entry entry;
    protected long seqno;

    public FetchCommand(long seq, Entry ent, int sendingReplica){
	super(MessageTags.FetchCommand,
	      computeSize(ent),
	      sendingReplica,
	      BFT.Parameters.getFilterCount());
	seqno = seq;
	entry = ent;
	// need to write to bytes


	int offset = getOffset();
	byte[] bytes = getBytes();
	
	// place the sequence number
	byte[] tmp = UnsignedTypes.longToBytes(seqno);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the batch bytes
	tmp = entry.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

    }

    public FetchCommand(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.FetchCommand)
	    BFT.Debug.kill("Bad Tag; "+getTag());
	
	int offset = getOffset();
	byte[] tmp = new byte[MessageTags.uint32Size];

	// read the sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqno = UnsignedTypes.bytesToLong(tmp);

	// read the batch bytes
	entry = Entry.fromBytes(bits, offset);
	offset += entry.getSize();
	if (offset != getBytes().length- getAuthenticationSize())
	    Debug.kill(new RuntimeException("Invalid byte input"));
    }


    public int getSendingReplica(){
	return (int) getSender();
    }

    
    public long getSeqNo(){
	return seqno;
    }

    public Entry getEntry(){
	return entry;
    }

    public boolean matches(VerifiedMessageBase vmb){
	boolean res = vmb.getTag() == getTag();
	if (!res)
	    return false;
	FetchCommand bc = (FetchCommand) vmb;
	return res 
	    && getSeqNo() == bc.getSeqNo() 
	    && getEntry().equals(bc.getEntry());
	    
    }

    static public int computeSize(Entry ent){
	return ent.getSize() + MessageTags.uint32Size;

    }


}