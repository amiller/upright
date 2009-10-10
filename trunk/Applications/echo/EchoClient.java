// $Id$

package Applications.echo;


import java.security.Security;

import BFT.clientShim.ClientShimBaseNode;
import BFT.clientShim.ClientGlueInterface;
import BFT.clientShim.Worker;
import BFT.network.concurrentNet.*;
import BFT.util.Role;
import BFT.Debug;

import java.util.Random;

public class EchoClient implements ClientGlueInterface{

	public void returnReply(byte[] reply){ 
		//	System.out.println("\t\t\t\t***Got a watch back: "+new String(reply));
	}


	public void brokenConnection(){
		System.out.println("\t\t\t\t***broken connection resulting from a"+
		" watch.  Echo doesnt care, but you might");
	}

	public byte[] canonicalEntry(byte[][]options){
		for (int i = 0; i < options.length; i++)
			if (options[i] != null)
				return options[i];
		return null;
	}

	public static void main(String[] args){

		if(args.length < 3) {
			System.out.println("Usage: java Applications.EchoClient <id> <config_file> <op_count> |<readratio>| |<requestsize>|");
			System.exit(0);
		}
		//Security.addProvider(new de.flexiprovider.core.FlexiCoreProvider());

		double readratio = 0.0;
		int reqsize = 0;
		if (args.length >= 4)
			readratio = Float.parseFloat(args[3]);
		if (args.length == 5)
			reqsize = Integer.parseInt(args[4]);
		
		BFT.clientShim.ClientShimBaseNode csbn = 
			new ClientShimBaseNode(args[1], Integer.parseInt(args[0]));
		int clientId = Integer.parseInt(args[0]);
		//	csbn.setNetwork(new TCPNetwork(csbn));
		csbn.setGlue(new EchoClient());


		NetworkWorkQueue nwq = new NetworkWorkQueue();
		NettyTCPNetwork orderNet = new NettyTCPNetwork(Role.ORDER, csbn.getMembership(), nwq);
		csbn.setNetwork(orderNet);
		
		//netty//Listener lo = new Listener(net, nwq);
		NettyTCPNetwork filterNet = new NettyTCPNetwork(Role.FILTER, csbn.getMembership(), nwq);
		csbn.setNetwork(filterNet);
		
		//netty//Listener lf = new Listener(net, nwq);
		NettyTCPNetwork execNet = new NettyTCPNetwork(Role.EXEC, csbn.getMembership(), nwq);
		csbn.setNetwork(execNet);
		
		//netty//Listener le = new Listener(net, nwq);
		//netty//Thread lto = new Thread(lo);
		//netty//Thread lte = new Thread(le);
		//netty//Thread ltf = new Thread(lf);
		Worker w = new Worker(nwq, csbn);
		Thread wt = new Thread(w);
		csbn.start();
		wt.start();
        orderNet.start();
        execNet.start();
        filterNet.start();
		
		//csbn.start();
		int count = Integer.parseInt(args[2]);
		System.out.println("count: "+count);
		System.err.println("#start "+System.currentTimeMillis());	


		Random r = new Random();
		int readcount = 0;


		byte[] out = new byte[reqsize+15*(new Integer(0)).toString().getBytes().length];
		for (int i = 1; i <=count; i++){
			long startTime=System.currentTimeMillis();
			byte[] tmp = null;
			byte[] intbyte = (new Integer(i)).toString().getBytes();
			for (int m = 0; m < intbyte.length; m++)
				out[m] = intbyte[m];
			if (r.nextDouble() < readratio){
				long endTime=System.currentTimeMillis();
				System.err.println("#req"+i+" "+startTime+" "+endTime+" "+clientId);		
				tmp = csbn.executeReadOnlyRequest(out);
				readcount++;
			}
			else{
				tmp = csbn.execute(out);
				long endTime=System.currentTimeMillis();
				System.err.println("#req"+i+" "+startTime+" "+endTime+" "+clientId);		
				//		System.out.println("latency:  "+(endTime-startTime));
				boolean res = true;
				for (int k = 0; k < intbyte.length; k++)
					res = res && intbyte[k] == tmp[k];
				if (!res)
					throw new RuntimeException("something is borked");
			}
		}
		System.err.println("end "+System.currentTimeMillis());
		System.out.println("reads: "+readcount+" total: "+count);
		//BFT.Parameters.println("finished the loop");
		System.exit(0);
	}

}