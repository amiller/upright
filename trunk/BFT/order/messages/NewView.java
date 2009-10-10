// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.messages.Digest;
import BFT.order.messages.MessageTags;

import BFT.Parameters;

import BFT.messages.VerifiedMessageBase;

/**
 
 **/
public class NewView extends MacArrayMessage{


    /**
       k is the number of non-null entires in the chosenvcs array
     **/
    public NewView (long view, Digest[] chosenvcs, 
		    long sendingReplica, int k){
	super(MessageTags.NewView, computeSize(chosenvcs, k), 
	      sendingReplica,
	      Parameters.getOrderCount());
	viewNo = view;
	chosenVCs = chosenvcs;

	int offset = getOffset();
	byte[] bytes = getBytes();
	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(viewNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];

	// place the view change messages
	for(int j = 0; j < chosenVCs.length; j++){
	    if (chosenVCs[j] != null){
		// place the order index of the replica
		tmp = UnsignedTypes.longToBytes(j);
		for (int i = 0; i < tmp.length; i++, offset++)
		    bytes[offset] = tmp[i];
		// place the VC digest
		tmp = chosenVCs[j].getBytes();
		for (int i = 0; i < tmp.length; i++, offset++)
		    bytes[offset] = tmp[i];
	    }
	}


    }


    /**
       k is the number of non-null entries in the vc array.
       n is the total size of said array
     **/
    public NewView(byte[] bits, int k, int n){
	super(bits);
	if (getTag() != MessageTags.NewView)
	    throw new RuntimeException("invalid message Tag: "+getTag());

	int offset = getOffset();
	byte[] tmp;

	// read the view number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	viewNo = UnsignedTypes.bytesToLong(tmp);


	// read the vc hashes
	chosenVCs = new Digest[n];
	byte[] tmpDigestBytes = new byte[Digest.size()];
	for (int j = 0; j < k; j++){
	    // get the replica index
	    for (int i = 0; i < 4; i++, offset++){
		tmp[i] = bits[offset];
	    }
	    
	    int loc = (int)(UnsignedTypes.bytesToLong(tmp));
	    // get the vc hash
	    for (int i = 0; i < tmpDigestBytes.length; i++, offset++)
		tmpDigestBytes[i] = bits[offset];
	    chosenVCs[loc] = Digest.fromBytes(tmpDigestBytes);
	}
	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte array");
    }


    public long getSendingReplica(){
	return getSender();
    }

    protected long viewNo;
    public long getView(){
	return viewNo;
    }

    protected Digest[] chosenVCs;
    public Digest[] getChosenVCs(){
	return chosenVCs;
    }
    

    public boolean equals(NewView nb){
	boolean res = super.equals(nb) && viewNo == nb.viewNo;
	for (int i = 0; i < getChosenVCs().length && res; i++){
	    if (chosenVCs[i] == null)
		res = res && nb.chosenVCs[i] == null;
	    else
		res = res && chosenVCs[i].equals(nb.chosenVCs[i]);
	}
	return res;

    }

    /** computes the size of the bits specific to NewView **/
    private static int computeSize( Digest[] vcs, int k){
	int size =  MessageTags.uint32Size;
	size += k * (Digest.size() + MessageTags.uint32Size);
	return size;
    }


    public String toString(){
	String res =  "<NEW-VIEW, "+super.toString()+", view="+viewNo;
	for (int i = 0; i < chosenVCs.length; i++)
	    res = res +", "+i+":"+(chosenVCs[i]!=null);
	res = res +">";
	return res;

    }

    public String dumpVCs(){
	String str = "";
	for (int i =0; i < getChosenVCs().length; i++)
	    str+= getChosenVCs()[i]+"\n";
	return str;
    }

    public static void main(String args[]){
	byte[] tmp = new byte[2];
	tmp[0] = 1;
	tmp[1] = 23;

	Digest[] cv = new Digest[4];
	for (int i = 0; i < cv.length; i++)
	    cv[i] = new Digest(tmp);

	cv[2] = null;

	NewView vmb = new NewView(123,cv, 1, 1);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	NewView vmb2 = 
	    new NewView(vmb.getBytes(), 1, 4);
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	vmb = new NewView(42, cv, 2, 1);
	//System.out.println("\ninitial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new NewView(vmb.getBytes(),1, 4);
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
}