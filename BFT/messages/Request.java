// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;

abstract public class Request extends MacArrayMessage{

    protected RequestCore core;

    /**
       The following constructor is for use only by ReadOnlyRequest as
       it is designed to skip the order node entirely
     **/
    public Request(int tag, long sender, RequestCore pay, boolean b){
	super(tag, pay.getTotalSize(), sender, 
	      b?BFT.Parameters.getExecutionCount():BFT.Parameters.getFilterCount());
	byte[] bytes = getBytes();
	
	int offset = getOffset();
	byte[] coreBytes = pay.getBytes();
	for (int i = 0; i < coreBytes.length; i++, offset++)
	    bytes[offset] = coreBytes[i];
	
	core = pay;
    }

    

    public Request(int tag, long sender, RequestCore pay){
	super(tag, pay.getTotalSize(), sender, 
	      BFT.Parameters.getOrderCount());
	byte[] bytes = getBytes();
	
	int offset = getOffset();
	byte[] coreBytes = pay.getBytes();
	for (int i = 0; i < coreBytes.length; i++, offset++)
	    bytes[offset] = coreBytes[i];
	
	core = pay;
    }

    public Request(byte[] bytes){
	super(bytes);
	
	byte[] tmp = new byte[getPayloadSize()];
	int offset = getOffset();
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	if (!BFT.Parameters.filtered)
	    core = new SignedRequestCore(tmp);
	else if (getTag() == MessageTags.ClientRequest || getTag() == MessageTags.ReadOnlyRequest)
	    core = new SimpleRequestCore(tmp);
	else
	    core = new FilteredRequestCore(tmp);
	
	if (offset != getBytes().length - getAuthenticationSize())
	    throw new RuntimeException("Invalid byte input");
    }

    public RequestCore getCore(){
	return core;
    }

    public boolean equals(Request r){
	boolean res = r != null && super.equals(r) ;
	res = core.equals(r.core);
	return res;
    }

    public String toString(){
	return core.toString();
    }
    

    
    
}