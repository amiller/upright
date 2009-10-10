// $Id$

package BFT.order.messages;  

import BFT.messages.Request;
import BFT.messages.SignedRequestCore;

public class ForwardedRequest extends Request{
    
    public ForwardedRequest( long sender, SignedRequestCore pay){
	super(MessageTags.ForwardedRequest, sender, pay);
    }

    public ForwardedRequest(byte[] bytes){
	super(bytes);
    }

    public int getSendingReplica(){
	return (int) getSender();
    }
}