// $Id$

package BFT.messages;  
import BFT.util.UnsignedTypes;


public class ClientRequest extends Request{
    
    public ClientRequest( long sender, RequestCore pay, boolean b){
	super(MessageTags.ClientRequest, sender, pay, false);
    }

    public ClientRequest( long sender, RequestCore pay){
	super(MessageTags.ClientRequest, sender, pay);
    }

    public ClientRequest(byte[] bytes){
	super(bytes);
    }



    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	SignedRequestCore vmb = 
	    new SignedRequestCore(1,0,tmp);
	Request req = new ClientRequest(0, vmb);
	//System.out.println("initial: "+req.toString());
	UnsignedTypes.printBytes(req.getBytes());
	
	Request req2 = new ClientRequest(req.getBytes());
	//System.out.println("\nsecondary: "+req2.toString());
	UnsignedTypes.printBytes(req2.getBytes());
	
	//System.out.println("old.equals(new): "+req.equals(req2));
	
    }
}