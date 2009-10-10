// $Id$

package BFT.serverShim;

import BFT.serverShim.messages.MessageTags;
import BFT.serverShim.messages.FetchCPMessage;
import BFT.serverShim.messages.CPStateMessage;
import BFT.serverShim.messages.FetchState;
import BFT.serverShim.messages.AppState;
import BFT.messages.VerifiedMessageBase;

/**
   Generates Messages that are used to communicate between
   client/order/execution nodes from the specified byte array
 **/
public class MessageFactory extends BFT.MessageFactory{


    static public VerifiedMessageBase fromBytes(byte[] bytes){
	byte[] tmp = new byte[2];
	tmp[0] = bytes[0];
	tmp[1] = bytes[1];
	int tag = BFT.util.UnsignedTypes.bytesToInt(tmp);
	switch(tag){	    
	case (MessageTags.FetchCPMessage): return new FetchCPMessage(bytes);
	case (MessageTags.CPStateMessage): return new CPStateMessage(bytes);
	case (MessageTags.FetchState): return new FetchState(bytes);
	case (MessageTags.AppState): return new AppState(bytes);
	default: return BFT.MessageFactory.fromBytes(bytes);
	}

    }

}

