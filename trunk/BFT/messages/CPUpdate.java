// $Id

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.Debug;

public class CPUpdate extends MacArrayMessage{

    protected long[] commands;
    protected long seqno;

    public CPUpdate(long[] lastCommand, long seqNo, int sendingReplica){
	super(MessageTags.CPUpdate,
	      computeSize(lastCommand),
	      sendingReplica,
	      BFT.Parameters.getFilterCount());
	if (lastCommand.length != BFT.Parameters.getNumberOfClients())
	    BFT.Debug.kill("Bad command list length");
	commands = new long[lastCommand.length];
	for (int i = 0; i < commands.length; i++)
	    commands[i] = lastCommand[i];
	seqno = seqNo;
	// need to write to bytes

	int offset = getOffset();
	byte[] bytes = getBytes();

	// place the sequence number
	byte[] tmp = UnsignedTypes.longToBytes(seqno);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the size of the batch
	for (int j = 0; j < commands.length; j++){
	    tmp = UnsignedTypes.longToBytes(commands[j]);
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	}

    }

    public CPUpdate(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.CPUpdate)
	    BFT.Debug.kill("Bad Tag; "+getTag());
	

	int offset = getOffset();
	byte[] tmp = new byte[MessageTags.uint32Size];

	// read the sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqno = UnsignedTypes.bytesToLong(tmp);

	commands = new long[BFT.Parameters.getNumberOfClients()];
	// read the batch size
	for (int j = 0; j < commands.length; j++){
	    tmp = new byte[MessageTags.uint32Size];
	    for(int i = 0; i < tmp.length; i++, offset++)
		tmp[i] = bits[offset];
	    commands[j] = UnsignedTypes.bytesToLong(tmp);
	}
	if (offset != getBytes().length- getAuthenticationSize())
	    Debug.kill(new RuntimeException("Invalid byte input"));
    }


    public int getSendingReplica(){
	return (int) getSender();
    }

    public long getSequenceNumber(){
	return seqno;
    }

    public long[] getCommands(){
	return commands;
    }

    public boolean matches(VerifiedMessageBase vmb){
	boolean res = vmb.getTag() == getTag();
	if (!res)
	    return false;
	CPUpdate bc = (CPUpdate) vmb;
	res =  res &&  getSequenceNumber() == bc.getSequenceNumber() ;
	for (int i =0; res && i < commands.length; i++)
	    res =  commands[i] == bc.commands[i];
	return res;
    }

    public boolean dominates(CPUpdate cpu){
	boolean res = getSequenceNumber() > cpu.getSequenceNumber();
	for (int i = 0; res && i < commands.length; i++)
	    res =  commands[i] >= cpu.commands[i];
	return res;
    }

 public boolean weaklyDominates(CPUpdate cpu){
	boolean res = getSequenceNumber() >= cpu.getSequenceNumber();
	for (int i = 0; res && i < commands.length; i++)
	    res =  commands[i] >= cpu.commands[i];
	return res;
    }

    static public int computeSize(long commands[]){
	return  MessageTags.uint32Size + 
	    commands.length * MessageTags.uint32Size;

    }


}