// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;

abstract public class FilteredRequestBase extends MacArrayMessage{

    protected FilteredRequestCore[] core;

   

    public FilteredRequestBase(int tag, long sender, FilteredRequestCore[] pay){
    	super(tag, computeSize(pay), sender, BFT.Parameters.getOrderCount());
    	
    	int offset = getOffset();
    	byte[] bytes = getBytes();
    	
    	// write length of payload
    	byte[] tmp = BFT.util.UnsignedTypes.intToBytes(pay.length);
    	for (int i = 0 ;i < tmp.length; i++, offset++)
    		bytes[offset] = tmp[i];
    	// write payload bytes
    	for (int i = 0; i < pay.length; i++)
    		for (int j = 0; j <pay[i].getBytes().length; j++, offset++)
    			bytes[offset] = pay[i].getBytes()[j];
    	core = pay;
    }
    
    
    protected static int computeSize(FilteredRequestCore[] pay){
    	int sum = 2;
	for (int i =0; i < pay.length; i++)
	    sum += pay[i].getTotalSize();
	return sum;
    }


    public FilteredRequestBase(byte[] bytes){
	super(bytes);
	
	
	int offset = getOffset();
	byte[] tmp = new byte[BFT.messages.MessageTags.uint16Size];
	for (int i = 0; i < tmp.length; i++, offset++)
		tmp[i] = bytes[offset];
	int size = BFT.util.UnsignedTypes.bytesToInt(tmp);
	
	FilteredRequestCore frc[] = new FilteredRequestCore[size];
	int fcCount = 0;
	while (offset < getOffset() + getPayloadSize() && fcCount < frc.length){
		tmp = new byte[MessageTags.uint32Size];
		for (int i = 0; i < tmp.length; i++)
			tmp[i] = bytes[offset+i+2];
		size = (int)BFT.util.UnsignedTypes.bytesToLong(tmp);
		size = getOffset() + size + MacSignatureMessage.computeAuthenticationSize(BFT.Parameters.getFilterCount(), BFT.Parameters.getOrderCount());
		tmp = new byte[size];
		for (int i = 0; i < tmp.length; i++, offset++)
			      tmp[i] = bytes[offset];
		frc[fcCount++]=new FilteredRequestCore(tmp);
	}
	core = frc;
	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");
    }

    public FilteredRequestCore[] getCore(){
	return core;
    }

    public boolean equals(FilteredRequestBase r){
	boolean res = r != null && super.equals(r) ;
	for (int i = 0; res && i < core.length; i++)
		res = res && core[i].equals(r.core[i]);
	return res;
    }
    

    
    
}