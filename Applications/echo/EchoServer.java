// $Id$

package Applications.echo;

import java.security.Security;

import BFT.serverShim.*;
import BFT.util.Role;

import BFT.messages.CommandBatch;
import BFT.messages.NonDeterminism;
import BFT.messages.Entry;

import BFT.network.concurrentNet.*;
import BFT.order.Parameters;

import BFT.Debug;

import BFT.fork.*;

public class EchoServer implements GlueShimInterface{


	int lossrate;
	long nextseqno;
	int count;
	public EchoServer(){
		nextseqno = 0;
		count = 0;
	}
	boolean doneonce = false;

	ServerShimInterface shim;

	public void setShim(ServerShimInterface sh){
		shim = sh;

	}

	public void exec(CommandBatch batch, long seqNo,
			NonDeterminism nd, boolean takeCP){


		//if (seqNo % 1000 == 0)
		//	System.out.println("Sequence number: "+seqNo+" / "+nextseqno);
		nextseqno++;

		Entry[] entries = batch.getEntries();
		for (int i = 0; i < entries.length; i++){
		    shim.result(entries[i].getCommand(), (int)(entries[i].getClient()),
				entries[i].getRequestId(), seqNo, true);
		    if (entries[i].getRequestId() % 100 == 0){
			String tmp = "watch every 100: "+
			    (entries[i].getRequestId() / 100);
			//			System.out.println("sending a watch");
			shim.result(tmp.getBytes(), 
				    (int) (entries[i].getClient()),
				    entries[i].getRequestId(),
				    seqNo,
				    false);
		    }
		    //		    System.err.println(entries[i].getClient() + ":" + entries[i].getRequestId());
		}
		if (takeCP) {
		    //			shim.pause();
		    //shim.isPaused();
		    shim.returnCP(new byte[8], seqNo);
		    // 			Forker f = new Forker(shim, seqNo);
		    // 			Thread t = new Thread(f);
		    // 			t.start();
		    // 			f.waitForFork();
		}
	}

    public void execReadOnly(int client, long reqid, byte[] op){
	//Debug.println("executing read only request: "+reqid+" from "+client);
	String tmp = "current seqno is "+ nextseqno;
	shim.readOnlyResult(tmp.getBytes(),
			    client,
			    reqid);
    }

	public void loadCP(byte[] appCPToken, long seqNo){
		nextseqno = seqNo;
	}

	public void releaseCP(byte[] appCPToken){
		// again, a no-op
	}

	public void fetchState(byte[] stateToken){
		// noop due to no state in the app
	}

	public void loadState(byte[] stateToken, byte[] state){
		// noop
	}

	public static void main(String[] args){

		if(args.length != 2) {
			System.out.println("Usage: java Applications.EchoServer <id> <config_file> ");
			System.exit(0);
		}
		//Security.addProvider(new de.flexiprovider.core.FlexiCoreProvider());
		ShimBaseNode sbn = new ShimBaseNode(args[1],
				Integer.parseInt(args[0]),
				new byte[0]);
		EchoServer es = new EchoServer();
		sbn.setGlue(es);
		es.setShim(sbn);
		NetworkWorkQueue nwq = new NetworkWorkQueue();
		NettyTCPNetwork orderNet = new NettyTCPNetwork(Role.ORDER, sbn.getMembership(), nwq);
		sbn.setNetwork(orderNet);
		//netty//net.start();
		//netty//Listener lo = new Listener(net, nwq);
		NettyTCPNetwork clientNet = new NettyTCPNetwork(Role.CLIENT, sbn.getMembership(), nwq);
		sbn.setNetwork(clientNet);
		//netty//net.start();
		//netty//Listener lc = new Listener(net, nwq);
		NettyTCPNetwork execNet = new NettyTCPNetwork(Role.EXEC, sbn.getMembership(), nwq);
		sbn.setNetwork(execNet);
		//netty//net.start();
		//netty//Listener le = new Listener(net, nwq);
		NettyTCPNetwork filterNet = new NettyTCPNetwork(Role.FILTER, sbn.getMembership(), nwq);
		sbn.setNetwork(filterNet);
		//netty//net.start();
		//netty//Listener lf = new Listener(net, nwq);
		

		//netty//Thread ltc = new Thread(lc);
		//netty//Thread lto = new Thread(lo);
		//netty//Thread lte = new Thread(le);
		//netty//Thread ltf = new Thread(lf);
		BFT.serverShim.Worker w = new BFT.serverShim.Worker(nwq, sbn);
		Thread wt = new Thread(w);
		sbn.start();
		wt.start();
		//netty//lte.start();
		//netty//lto.start();
		//netty//ltc.start();
		//netty//ltf.start();
		orderNet.start();
		clientNet.start();
		execNet.start();
		filterNet.start();
	}

}
