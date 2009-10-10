// $Id$

package BFT;

import java.util.Vector;
import java.io.File;
import java.io.FileInputStream;

import BFT.messages.MessageTags;
import BFT.messages.VerifiedMessageBase;
import BFT.messages.ClientRequest;
import BFT.messages.NextBatch;
import BFT.messages.SpeculativeNextBatch;
import BFT.messages.TentativeNextBatch;
import BFT.messages.CommittedNextBatch;
import BFT.messages.ReleaseCP;
import BFT.messages.Retransmit;
import BFT.messages.LoadCPMessage;
import BFT.messages.CPLoaded;
import BFT.messages.LastExecuted;
import BFT.messages.CPTokenMessage;
import BFT.messages.Reply;
import BFT.messages.WatchReply;
import BFT.messages.RequestCP;
import BFT.messages.ReadOnlyRequest;
import BFT.messages.ReadOnlyReply;
import BFT.messages.FilteredRequest;
import BFT.messages.BatchCompleted;
import BFT.messages.FetchCommand;
import BFT.messages.ForwardCommand;
import BFT.messages.FetchDenied;
import BFT.messages.CPUpdate;


/**
   Generates Messages that are used to communicate between
   client/order/execution nodes from the specified byte array
 **/
public class MessageFactory{


    static public VerifiedMessageBase fromBytes(byte[] bytes){
	byte[] tmp = new byte[2];
	tmp[0] = bytes[0];
	tmp[1] = bytes[1];
	int tag = BFT.util.UnsignedTypes.bytesToInt(tmp);
	VerifiedMessageBase vmb;
	switch(tag){	    
	case MessageTags.ClientRequest: return new ClientRequest(bytes);
	case MessageTags.FilteredRequest: return new FilteredRequest(bytes);
	case MessageTags.SpeculativeNextBatch: return new SpeculativeNextBatch(bytes);
	case MessageTags.BatchCompleted: return new BatchCompleted(bytes);

	case MessageTags.TentativeNextBatch: return new TentativeNextBatch(bytes);
	case MessageTags.CommittedNextBatch: return new CommittedNextBatch(bytes);
	case MessageTags.ReleaseCP: return new ReleaseCP(bytes);
	case MessageTags.Retransmit: return new Retransmit(bytes);
	case MessageTags.LoadCPMessage: return new LoadCPMessage(bytes);
	case MessageTags.LastExecuted: return new LastExecuted(bytes);
	case MessageTags.CPLoaded: return new CPLoaded(bytes);
	case MessageTags.CPTokenMessage: return new CPTokenMessage(bytes);
	case MessageTags.Reply: return new Reply(bytes);
	case MessageTags.RequestCP: return new RequestCP(bytes);
	case MessageTags.WatchReply: return new WatchReply(bytes);
	case MessageTags.ReadOnlyRequest: return new ReadOnlyRequest(bytes);
	case MessageTags.ReadOnlyReply: return new ReadOnlyReply(bytes);
	case MessageTags.FetchCommand: return new FetchCommand(bytes);
	case MessageTags.ForwardCommand: return new ForwardCommand(bytes);
	case MessageTags.FetchDenied: return new FetchDenied(bytes);
	case MessageTags.CPUpdate: return new CPUpdate(bytes);
	    
	default: 
	    BFT.Debug.kill(new RuntimeException("Invalid Message Tag: "+tag));
	    return null;
	}
    }
    

    /** Reads from the file and returns a vector of messages from the file **/
    public static Vector<VerifiedMessageBase> fromFile(File file){

	return null;
    }

}

