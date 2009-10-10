// $Id$

package BFT.messages;  
import BFT.util.UnsignedTypes;


public class FilteredRequest extends FilteredRequestBase{
    

	public FilteredRequest(long sender, FilteredRequestCore[] pay){
		super(MessageTags.FilteredRequest, sender, pay);
		
	}
//    public FilteredRequest( long sender, FilteredRequestCore pay){
//	super(MessageTags.FilteredRequest, sender, pay);
 //   }

    public FilteredRequest(byte[] bytes){
	super(bytes);
    }

    public int getSendingReplica(){
	return (int) getSender();
    }

 //   public int getSendingClient(){
//	return getCore().getSendingClient();
 //   }

//    public long getRequestId(){
//	return getCore().getRequestId();
 //   }
/*
    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	FilteredRequestCore vmb = 
	    new FilteredRequestCore(1,0,tmp);
	Request req = new FilteredRequest(0, vmb);
	//System.out.println("initial: "+req.toString());
	UnsignedTypes.printBytes(req.getBytes());
	
	Request req2 = new FilteredRequest(req.getBytes());
	//System.out.println("\nsecondary: "+req2.toString());
	UnsignedTypes.printBytes(req2.getBytes());
	
	//System.out.println("old.equals(new): "+req.equals(req2));
	
    }*/
}