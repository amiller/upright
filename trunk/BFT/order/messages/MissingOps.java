// $Id$

package BFT.order.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacArrayMessage;
import BFT.messages.Digest;
import BFT.order.messages.MessageTags;

import BFT.messages.VerifiedMessageBase;

/**
 
 **/
public class MissingOps extends MacArrayMessage{


    public MissingOps (long view, long[] missingops, int sendingReplica){
	super(MessageTags.MissingOps, computeSize(missingops), sendingReplica,
	      BFT.Parameters.getOrderCount());
	viewNo = view;
	missingOps = missingops;

	int offset = getOffset();
	//System.out.println(offset);
	byte[] bytes = getBytes();
	//System.out.println(bytes.length);
	// place the view number
	byte[] tmp = UnsignedTypes.longToBytes(viewNo);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the number of missing ops
	tmp = UnsignedTypes.longToBytes(missingOps.length);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// place the list of missing ops
	for (int j = 0; j < missingOps.length; j++){
	    tmp = UnsignedTypes.longToBytes(missingOps[j]);
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	}
					

    }
    
    public MissingOps(byte[] bits){
	super(bits);
	int offset = getOffset();
	byte[] tmp;


	// read the view number;
	tmp = new byte[4];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	viewNo = UnsignedTypes.bytesToLong(tmp);

	// read the number of missing ops
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bits[offset];
	int ln = (int) (UnsignedTypes.bytesToLong(tmp));
	missingOps = new long[ln];
	for (int j = 0; j < ln; j++){
	    for (int i = 0; i < tmp.length; i++, offset++)
		tmp[i] = bits[offset];
	    missingOps[j] = UnsignedTypes.bytesToLong(tmp);
	}

	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("invalid byte array");
    }

    protected long viewNo;
    public long getView(){
	return viewNo;
    }

    protected long[] missingOps;
    public long[] getMissingOps(){
	return missingOps;
    }
    

    public int getSendingReplica(){
	return (int)getSender();
    }

    public boolean equals(MissingOps nb){
	boolean res =  super.equals(nb) && nb != null && 
	    viewNo == nb.viewNo && missingOps.length == nb.missingOps.length;
	for (int i = 0; i < missingOps.length && res; i++)
	    res = res && missingOps[i] == nb.missingOps[i];
	return res;
    }

    /** computes the size of the bits specific to MissingOps **/
    private static int computeSize(long b[] ){
	int size =  MessageTags.uint32Size + MessageTags.uint32Size +
	    MessageTags.uint32Size * b.length;
	return size;
    }


    public String toString(){
	return "<MISS-VC, "+super.toString()+", view="+viewNo+
	    ", ops:"+missingOps.length+">";
    }

    public static void main(String args[]){
	long list[] = new long[5];
	for(int i = 0; i < list.length; i++)
	    list[i] =i +2342+i*i*i*23;

	MissingOps vmb = new MissingOps(123, list , 1);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	MissingOps vmb2 = 
	    new MissingOps(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	
	list[2]= 234;

	vmb = new MissingOps(42, list, 2);
	//System.out.println("\ninitial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	vmb2 = new MissingOps(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
	
	//System.out.println("\nold = new: "+vmb.equals(vmb2));
    }
    
}