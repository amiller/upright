// $Id$

package BFT.messages;

public class CertificateEntry{
    protected HistoryDigest hd;
    protected CommandBatch commands;
    protected NonDeterminism nondet;
    protected byte[] bytes;

    public CertificateEntry(HistoryDigest h, CommandBatch com,
			    NonDeterminism nd, Digest cp){
	hd = h;
	commands = com;
	nondet = nd;
	cphash = cp;
	bytes = null;
    }

    public CertificateEntry(byte[] bytes, int offset){
	byte[] tmp;


	// history digest
	tmp = new byte[HistoryDigest.size()];
	for (int i = 0;i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	hd = HistoryDigest.fromBytes(tmp);
	// cphash
	tmp = new byte[Digest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	cphash = Digest.fromBytes(tmp);
	// nondet size
	tmp = new byte[MessageTags.uint16Size];
	for (int i = 0; i < tmp.length; i ++, offset++)
	    tmp[i] = bytes[offset];
	int sz = BFT.util.UnsignedTypes.bytesToInt(tmp);
	// nondet
	tmp = new byte[sz];
	for (int i = 0;i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	nondet = new NonDeterminism(tmp);

	// commandbatchsize
	tmp = new byte[MessageTags.uint16Size];
	for (int i = 0;i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	sz = BFT.util.UnsignedTypes.bytesToInt(tmp);

	// entries in commandbatch
	for (int i = 0;i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	int count = BFT.util.UnsignedTypes.bytesToInt(tmp);

	// command batch itself
	tmp = new byte[sz];
	for (int i = 0;i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	commands = new CommandBatch(tmp, count);

    }

    public HistoryDigest getHistoryDigest(){
	return hd;
    }

    public CommandBatch getCommandBatch(){
	return commands;
    }

    public NonDeterminism getNonDeterminism(){
	return nondet;
    }
    
    Digest cphash;
    public Digest  getCPHash(){
	return cphash;
    }

    Digest dig = null;
    public Digest getDigest(){
	if (dig == null)
	    dig = new Digest(getBytes());
	return dig;
    }

    public boolean equals(CertificateEntry e){
	return e != null && getDigest().equals(e.getDigest());
    }

    public int getSize(){
	return getBytes().length;
    }

    public byte[] getBytes(){
	if (bytes == null){
	    bytes = new byte[HistoryDigest.size()+
			     Digest.size()+
			     nondet.getSize()+
			     commands.getSize()+
			     BFT.messages.MessageTags.uint16Size *3];
	    int offset = 0;
	    // write down the history digest
	    byte[] tmp = hd.getBytes();
	    for (int i = 0; i < HistoryDigest.size(); i++, offset++)
		bytes[offset] = tmp[i];
	    // writedown the cphash
	    tmp = cphash.getBytes();
	    for (int i = 0; i < Digest.size(); i++, offset++)
		bytes[offset] = tmp[i];
	    
	    // write down the nondet size
	    tmp = BFT.util.UnsignedTypes.intToBytes(nondet.getSize());
	    for (int i = 0;i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];

	    // write downt the nondeterminism
	    tmp = nondet.getBytes();
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	
	    // write down the command size
	    tmp = BFT.util.UnsignedTypes.intToBytes(commands.getSize());
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];

	    // write down the command entries
	    tmp = BFT.util.UnsignedTypes.intToBytes(commands.getEntryCount());
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];

	    // write down the commands
	    tmp = commands.getBytes();
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	    if (bytes.length != offset)
		throw new RuntimeException("offset construction is flawed");
	}
	return bytes;

    }

}