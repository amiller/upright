// $Id$

package BFT.messages;  
import BFT.util.UnsignedTypes;


public class ReadOnlyRequest extends Request{
    
    public ReadOnlyRequest( long sender, RequestCore pay){
	super(MessageTags.ReadOnlyRequest, sender, pay, true);
    }

    public ReadOnlyRequest(byte[] bytes){
	super(bytes);
    }

    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	RequestCore vmb = 
	    new SignedRequestCore(1,0,tmp);
	Request req = new ReadOnlyRequest(0, vmb);
	//System.out.println("initial: "+req.toString());
	UnsignedTypes.printBytes(req.getBytes());
	
	Request req2 = new ReadOnlyRequest(req.getBytes());
	//System.out.println("\nsecondary: "+req2.toString());
	UnsignedTypes.printBytes(req2.getBytes());
	

	//System.out.println("old.equals(new): "+req.equals(req2));
	
    }
}