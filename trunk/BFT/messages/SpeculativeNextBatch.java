// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;
import BFT.Parameters;
import BFT.messages.HistoryDigest;
import BFT.messages.NextBatch;

/**
   SpeculativeNextBatch message sent from the order node to the execution node
   indicating the batch to be executed at sequence number n in view
   view with history h using specified non determinism and possibly
   taking a checkpoint after executing all requests in the batch.
 **/
public class SpeculativeNextBatch extends NextBatch{

    public SpeculativeNextBatch( long view, NextBatch nb){
	this( view, nb.getSeqNo(), nb.getHistory(), nb.getCommands(),
	      nb.getNonDeterminism(), nb.getCPDigest(), 
	      nb.takeCP(), nb.getSendingReplica());
    }
    
    public  SpeculativeNextBatch(long view, long seq, 
				 HistoryDigest hist,
				 CommandBatch batch, NonDeterminism non,
				 Digest cpDig,
				 boolean cp, long sendingReplica){
	super(BFT.messages.MessageTags.SpeculativeNextBatch,
	      view, seq, hist, batch, non, cpDig, cp, sendingReplica);
    }


     
    public SpeculativeNextBatch(long view, long seq, CertificateEntry entry,
				boolean cp, long sendingReplica){
	super(BFT.messages.MessageTags.SpeculativeNextBatch,
	      view, seq, entry, cp, sendingReplica);
    }

    public SpeculativeNextBatch(byte[] bits){
	super(bits);
	if (getTag() != MessageTags.SpeculativeNextBatch)
	    throw new RuntimeException("invalid message Tag: "+getTag());
    }


    public static void main(String args[]){
	Entry[] entries = new Entry[1];
	byte[] tmp = new byte[2];
	tmp[0] = 1;
	tmp[1] = 23;
	entries[0] = new Entry(1234, 632, tmp);
	HistoryDigest hist = new HistoryDigest(tmp);
	CommandBatch batch = new CommandBatch(entries);
	NonDeterminism non = new NonDeterminism(12341, 123456);
	
	SpeculativeNextBatch vmb = 
	    new SpeculativeNextBatch(43, 234, hist, batch, non, hist, false, 3);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	SpeculativeNextBatch vmb2 = 
	    new SpeculativeNextBatch(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	vmb = new SpeculativeNextBatch(134,8, hist, batch, non, hist, true, 1);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new SpeculativeNextBatch(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
}
