// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.Parameters;
import BFT.messages.HistoryDigest;

/**
   NextBatch message sent from the order node to the execution node
   indicating the batch to be executed at sequence number n in view
   view with history h using specified non determinism and possibly
   taking a checkpoint after executing all requests in the batch.
 **/
abstract public class NextBatch extends MacArrayMessage{

    public NextBatch(int tag, long view, NextBatch nb){
	this(tag, view, nb.getSeqNo(), nb.getHistory(), nb.getCommands(),
	     nb.getNonDeterminism(), nb.getCPDigest(), nb.takeCP(), nb.getSendingReplica());
    }

    public  NextBatch(int tag, long view, long seq, HistoryDigest hist,
		      CommandBatch batch, NonDeterminism non, Digest cphash,
		      boolean cp, long sendingReplica){
	this(tag, view, seq, new CertificateEntry(hist, batch, non, cphash), cp,
	     sendingReplica);
    }
    public NextBatch(int tag, long view, long seq, CertificateEntry entry,
		     boolean cp, long sendingReplica){
	super(tag,
	      computeSize(view, seq, entry, cp),
	      sendingReplica,
	      Parameters.getExecutionCount());

	
	viewNo = view;
	seqNo = seq;
	certEntry = entry;
	takeCP = cp;
	
	int offset = getOffset();
	byte[] bytes = getBytes();


	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(viewNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the sequence number
	tmp = UnsignedTypes.longToBytes(seqNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the history
	tmp = getHistory().getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the cphash
	tmp = getCPDigest().getBytes();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the checkpoint flag
	tmp = UnsignedTypes.intToBytes(takeCP?1:0);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the nondeterminism
	// size first
	tmp = UnsignedTypes.intToBytes(getNonDeterminism().getSize());
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// now the nondet bytes
	tmp = getNonDeterminism().getBytes();
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

    public NextBatch(byte[] bits){
	super(bits);
	int offset = getOffset();
	byte[] tmp;

	// read the view number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	viewNo = UnsignedTypes.bytesToLong(tmp);

	// read the sequence number
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	seqNo = UnsignedTypes.bytesToLong(tmp);

	// read the history
	tmp = new byte[Digest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	HistoryDigest history = HistoryDigest.fromBytes(tmp);

	// read the cphash
	tmp = new byte[Digest.size()];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	Digest cpHash = Digest.fromBytes(tmp);
	
	// read the checkpoint flag
	tmp = new byte[2];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	takeCP = (UnsignedTypes.bytesToInt(tmp) != 0);

	// read the non det size
	tmp = new byte[2];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int nondetSize = (UnsignedTypes.bytesToInt(tmp));
	// read the nondeterminism
	tmp = new byte[nondetSize];
	for(int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	NonDeterminism nondet = new NonDeterminism(tmp);

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
	CommandBatch comBatch = new CommandBatch(tmp, count);
	
	certEntry = new CertificateEntry(history,comBatch, nondet, cpHash);

	

	if (offset != getBytes().length- getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");

    }

    protected long viewNo;
    public long getView(){
	return viewNo;
    }

    protected long seqNo;
    public long getSeqNo(){
	return seqNo;
    }
    
    public HistoryDigest getHistory(){
	return certEntry.getHistoryDigest();
    }

    public CommandBatch getCommands(){
	return certEntry.getCommandBatch();
    }

    public NonDeterminism getNonDeterminism(){
	return certEntry.getNonDeterminism();
    }
    
    public Digest getCPDigest(){
	return certEntry.getCPHash();
    }

    protected CertificateEntry certEntry;
    public CertificateEntry getCertificateEntry(){
	return certEntry;
    }

    protected boolean takeCP;
    public boolean takeCP(){
	return takeCP;
    }

    public long getSendingReplica(){
	return getSender();
    }

    public boolean equals(NextBatch nb){
	return super.equals(nb) && matches(nb);
    }

    public boolean consistent(NextBatch nb){
	return nb != null && seqNo == nb.seqNo &&
	    getHistory().equals(nb.getHistory()) && takeCP == nb.takeCP &&
	    getNonDeterminism().equals(nb.getNonDeterminism())
	    && getCommands().equals(nb.getCommands());
    }

    public boolean matches(NextBatch nb){
	return  nb != null && viewNo == nb.viewNo && consistent(nb);
    }
    

    /** computes the size of the bits specific to NextBatch **/
    private static int computeSize(long view, long seq, CertificateEntry entry,
				   boolean cp){
	int size =  MessageTags.uint32Size + MessageTags.uint32Size +
	    MessageTags.uint16Size + MessageTags.uint16Size + 
	    MessageTags.uint16Size +
	    MessageTags.uint32Size +//+ entry.getSize();
	    entry.getCPHash().size()+
	    entry.getHistoryDigest().size() +
	    entry.getNonDeterminism().size() + entry.getCommandBatch().getSize();
	return size;
    }


//  private static int computeSize(long view, long seq, Digest h, 
// 			    CommandBatch batch, NonDeterminism non, 
// 			    boolean cp){
//      int size =  MessageTags.uint32Size + MessageTags.uint32Size +
// 	 Digest.size() + MessageTags.uint16Size + MessageTags.uint16Size + 
// 	 non.getSize() + MessageTags.uint16Size +
// 	 MessageTags.uint16Size + batch.getSize();
//      return size;
//  }



    public String toString(){
	return "<NB, v: "+viewNo+" seq: "+seqNo+" hist: "+getHistory()+">";
    }

    
}
