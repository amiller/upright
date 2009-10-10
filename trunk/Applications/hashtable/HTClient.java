package Applications.hashtable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import BFT.clientShim.ClientShimBaseNode;
import BFT.network.TCPNetwork;



public class HTClient{
	ClientShimBaseNode clientShim;
	public HTClient(String membership, int id){
                clientShim = new ClientShimBaseNode(membership, id);
                clientShim.setNetwork(new TCPNetwork(clientShim));
                clientShim.start();
	}

	public void write(String key, int value)
	{
		HTRequest req = new HTRequest(HTRequest.WRITE, key, value);
		byte[] replyBytes = clientShim.execute(Convert.objectToBytes(req));
		HTReply rep=(HTReply)(Convert.bytesToObject(replyBytes));
                if(rep.isError()){
                	throw new RuntimeException("Write failed");
            	} 
	}

	public DataUnit read(String key){
		HTRequest req = new HTRequest(HTRequest.READ, key, 0);
		byte [] replyBytes = clientShim.execute(Convert.objectToBytes(req));
                HTReply rep = (HTReply)(Convert.bytesToObject(replyBytes));
                if(rep.isError()){
  	              throw new RuntimeException("Read failed");
                } 
                return rep.getData();
	}
	
	public static void main(String[] args){
		String membership=args[0];
		int id=Integer.parseInt(args[1]);
		HTClient client=new HTClient(membership, id);
		client.write("1",1);
		DataUnit data=client.read("1");
		System.out.println("value="+data.getValue());
		System.out.println("random="+data.getRandom());
		System.out.println("timestamp="+data.getTimestamp());
		System.exit(0);
	}
}
