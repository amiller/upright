// $Id

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.Debug;

public class ForwardCommand extends MacMessage{

    protected Entry entry;
    protected long seqno;

    public ForwardCommand( long seqno, Entry ent, int sendingReplica){
	super(MessageTags.ForwardCommand,
	      computeSize(ent),
	      sendingReplica);
	entry = ent;
	this.seqno = seqno;
	// need to write to bytes


	int offset = getOffset();
	byte[] bytes = getBytes();
	byte[] tmp = BFT.util.UnsignedTypes.longToBytes(seqno);
	for (int i = 0;i < tmp.length; i ++, offset++)
	    bytes[offset] = tmp[i];

	// place the batch bytes
	tmp = entry.getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

    }

    public ForwardCommand(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.ForwardCommand)
	    BFT.Debug.kill("Bad Tag; "+getTag());
	
	int offset = getOffset();

	byte[] tmp = new byte[MessageTags.uint32Size];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqno = BFT.util.UnsignedTypes.bytesToLong(tmp);

	// read the batch bytes
	entry = Entry.fromBytes(bits, offset);
	offset += entry.getSize();
	if (offset != getBytes().length- getAuthenticationSize())
	    Debug.kill(new RuntimeException("Invalid byte input"));
    }


    public int getSendingReplica(){
	return (int) getSender();
    }


    public Entry getEntry(){
	return entry;
    }

    public long getSeqNo(){
	return seqno;
    }

    public boolean matches(VerifiedMessageBase vmb){
	boolean res = vmb.getTag() == getTag();
	if (!res)
	    return false;
	ForwardCommand bc = (ForwardCommand) vmb;
	return res 
	    && getEntry().equals(bc.getEntry());
	    
    }

    static public int computeSize(Entry ent){
	return ent.getSize() + MessageTags.uint32Size;

    }


}