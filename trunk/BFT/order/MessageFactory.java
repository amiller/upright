// $Id$

package BFT.order;

import BFT.messages.VerifiedMessageBase;

import BFT.order.messages.MessageTags;
import BFT.order.messages.*;

/**
   Converts a byte array to an appropriate message that is either
   handled or sent by an order replica
 **/
public class MessageFactory{


    static public VerifiedMessageBase fromBytes(byte[] bytes){
	byte[] tmp = new byte[2];
	tmp[0] = bytes[0];
	tmp[1] = bytes[1];
	int tag = BFT.util.UnsignedTypes.bytesToInt(tmp);
	VerifiedMessageBase vmb;
	switch(tag){	    
	case MessageTags.ForwardedRequest: return new ForwardedRequest(bytes);
	case MessageTags.PrePrepare: return new PrePrepare(bytes);
	case MessageTags.Prepare: return new Prepare(bytes);
	case MessageTags.Commit: return new Commit(bytes);
	case MessageTags.ViewChange: return new ViewChange(bytes);
	case MessageTags.ViewChangeAck: return new ViewChangeAck(bytes);
	case MessageTags.NewView: 
	    return new NewView(bytes, BFT.Parameters.largeOrderQuorumSize(),
			       BFT.Parameters.getOrderCount());
	case MessageTags.ConfirmView: return new ConfirmView(bytes);
	case MessageTags.MissingViewChange: return new MissingViewChange(bytes);
	case MessageTags.RelayViewChange: 
	    return new RelayViewChange(bytes,
				       BFT.Parameters.getOrderCount());
	case MessageTags.MissingOps: return new MissingOps(bytes);
	case MessageTags.RelayOps: return new RelayOps(bytes);
	    //case MessageTags.OpUpdate: return new OpUpdate(bytes);
	    //	case MessageTags.CPToken: return new CPToken(bytes);
	case MessageTags.MissingCP: return new MissingCP(bytes);
	case MessageTags.RelayCP: return new RelayCP(bytes);
	case MessageTags.OrderStatus: return new OrderStatus(bytes);
	case MessageTags.StartView: return new StartView(bytes);
	default: return BFT.MessageFactory.fromBytes(bytes);
	}
    }

}

