// $Id

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.Debug;

public class BatchCompleted extends MacArrayMessage{

    protected CommandBatch commands;
    protected long view;
    protected long seqno;

    public BatchCompleted(NextBatch nb, int sendingReplica){
	super(MessageTags.BatchCompleted,
	      computeSize(nb),
	      sendingReplica,
	      BFT.Parameters.getFilterCount());
	commands = nb.getCommands();
	view = nb.getView();
	seqno = nb.getSeqNo();
	// need to write to bytes

	int offset = getOffset();
	byte[] bytes = getBytes();

	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(view);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the sequence number
	tmp = UnsignedTypes.longToBytes(seqno);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the size of the batch
	tmp = UnsignedTypes.longToBytes(getCommands().getSize());
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the number of entries in the batch
	tmp = UnsignedTypes.intToBytes(getCommands().getEntries().length);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the batch bytes
	tmp = getCommands().getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];


    }

    public BatchCompleted(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.BatchCompleted)
	    BFT.Debug.kill("Bad Tag; "+getTag());
	


	int offset = getOffset();
	byte[] tmp;

	// read the view number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	view = UnsignedTypes.bytesToLong(tmp);

	// read the sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqno = UnsignedTypes.bytesToLong(tmp);


	// read the batch size
	tmp = new byte[4];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int batchSize = (int) (UnsignedTypes.bytesToLong(tmp));

	// read the number of entries in the batch
	tmp = new byte[2];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int count = (int) (UnsignedTypes.bytesToInt(tmp));

	// read the batch bytes
	tmp = new byte[batchSize];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	commands = new CommandBatch(tmp, count);
	if (offset != getBytes().length- getAuthenticationSize())
	    Debug.kill(new RuntimeException("Invalid byte input"));
    }


    public int getSendingReplica(){
	return (int) getSender();
    }

    public long getView(){
	return view;
    }
    
    public long getSeqNo(){
	return seqno;
    }

    public CommandBatch getCommands(){
	return commands;
    }

    public boolean matches(VerifiedMessageBase vmb){
	boolean res = vmb.getTag() == getTag();
	if (!res)
	    return false;
	BatchCompleted bc = (BatchCompleted) vmb;
	return res && getView() == bc.getView() 
	    && getSeqNo() == bc.getSeqNo() 
	    && getCommands().equals(bc.getCommands());
	    
    }

    static public int computeSize(NextBatch nb){
	return nb.getCommands().getSize() + 3 * MessageTags.uint32Size + MessageTags.uint16Size;

    }


}