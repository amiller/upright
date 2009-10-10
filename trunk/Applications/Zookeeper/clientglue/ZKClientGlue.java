/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Applications.Zookeeper.clientglue;

import BFT.clientShim.*;
import BFT.membership.Membership;
import BFT.membership.Principal;
import BFT.Debug;
import BFT.network.concurrentNet.*;


import BFT.util.Pair;
import BFT.util.Role;
import BFT.util.UnsignedTypes;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.ByteBuffer;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.*;
/**
 *
 * @author manos
 */
public class ZKClientGlue implements ClientGlueInterface {

	protected ClientShimBaseNode csbn;
	protected ZKNetwork network;

	public ZKClientGlue(String membership, int id, int port) {
		csbn = new ClientShimBaseNode(membership, id);
		csbn.setGlue(this);
		/*NetworkWorkQueue nwq = new NetworkWorkQueue();
		TCPNetwork net = new TCPNetwork(Role.ORDER, csbn.getMembership());
		csbn.setNetwork(net);
		net.start();
		Listener lo = new Listener(net, nwq);
		net = new TCPNetwork(Role.FILTER, csbn.getMembership());
		csbn.setNetwork(net);
		net.start();
		Listener lf = new Listener(net, nwq);
		net = new TCPNetwork(Role.EXEC, csbn.getMembership());
		csbn.setNetwork(net);
		net.start();
		Listener le = new Listener(net, nwq);
		Thread lto = new Thread(lo);
		Thread lte = new Thread(le);
		Thread ltf = new Thread(lf);
		Worker w = new Worker(nwq, csbn);
		Thread wt = new Thread(w);
		csbn.start();
		wt.start();
		lte.start();
		lto.start();
		ltf.start();*/

		NetworkWorkQueue nwq = new NetworkWorkQueue();
                NettyTCPNetwork orderNet = new NettyTCPNetwork(Role.ORDER, csbn.getMembership(), nwq);
                csbn.setNetwork(orderNet);

                NettyTCPNetwork filterNet = new NettyTCPNetwork(Role.FILTER, csbn.getMembership(), nwq);
                csbn.setNetwork(filterNet);

                NettyTCPNetwork execNet = new NettyTCPNetwork(Role.EXEC, csbn.getMembership(), nwq);
                csbn.setNetwork(execNet);

                Worker w = new Worker(nwq, csbn);
                Thread wt = new Thread(w);
                csbn.start();
                wt.start();
        	orderNet.start();
        	execNet.start();
        	filterNet.start();


		/*TCPNetwork tcpNetwork = new TCPNetwork(csbn);
        csbn.setNetwork(tcpNetwork);
        csbn.start();
		 */        
		// now the network to client
		ZKWorker worker;
		BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(1024);		 
		network = new ZKNetwork(this, port );
		network.start();
		worker = new ZKWorker(this, queue);
		ZKListener listener = new ZKListener(network, queue);
		//		 new Thread(worker).start();
		//		 new Thread(new ZKListener(this)).start();
		Thread zwt = new Thread(worker);
		Thread zlt = new Thread(listener);
		zwt.start();
		zlt.start();
	}
	
	public void handleBytes(byte[] bytes) {
		byte[] result = null;
		//System.out.println("request:");
		//BFT.util.UnsignedTypes.printBytes(bytes);		
		if(!isReadonly(bytes))
			result = csbn.execute(bytes);
		else
			result = csbn.executeReadOnlyRequest(bytes);
		//BFT.Debug.println("received "+result.length+" bytes");
		network.send(result);
		
	}
	
	private boolean isReadonly(byte []request){
		switch(request[11]){
		case 3:	//exists
		case 4: //getData
		case 8: //getChildren
			if(request[request.length-1]==1) //Has a watcher
				return false;
			else
				return true;
		case 6: return true;//getACL
		}
		return false;
	}


	public byte[] canonicalEntry(byte [][]replies){
		if(replies.length<=0)
			throw new RuntimeException("Invalid replies");
		/*for(int i=0;i<replies.length;i++)
			if(replies[i]!=null)
				return replies[i];*/
		int endIndex=0;
		long[] zxid = new long[replies.length];
		for (int i = 0; i < replies.length; i++) {
			if(replies[i]==null){
				//System.out.println(i+" is null");
				continue;
			}
			zxid[i] = ByteBuffer.wrap(replies[i], 8, 8).asLongBuffer().get();
			for (int j = 8; j < 16; j++) {
				replies[i][j] = 0;
			}
		}
		for (int i = 0; i < replies.length - 1; i++) {
			if (replies[i]!=null&&replies[i+1]!=null&&!Arrays.equals(replies[i], replies[i + 1]))
				return null;
		}
		for (int i = 0; i < replies.length; i++) {
			if (replies[i]!=null)
				return replies[i];
		}
		return null;
	}

	public void returnReply(byte []reply){
		network.send(reply);
	}

	public void brokenConnection(){
		// don't do anything right now
//		try {
//			network.closeConn();
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		}
	}

	public static void main(String[] args) {
		if(args.length < 3) {
			BFT.Parameters.println("Usage: java Applications.EchoClient <id> <config_file> <listen_port>");
			System.exit(0);
		}
		ZKClientGlue cShim = new ZKClientGlue(args[1], Integer.parseInt(args[0]), Integer.parseInt(args[2]));

	}
}

class ZKNetwork {

	ZKClientGlue cShim;

	private boolean listening;
	private SocketChannel socket;
	private SelectionKey sk;
	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;
	// The selector we'll be monitoring
	private Selector selector;
	// stores the available bytes on each socket
	private Hashtable<SocketChannel, byte[]> storedBytes;
	private ByteBuffer readBuffer = ByteBuffer.allocate(8192);
	BlockingQueue<byte[]> writeQueue = null;

	public ZKNetwork(ZKClientGlue shim, int port) {
		try {
			this.cShim = shim;

			storedBytes = new Hashtable<SocketChannel, byte[]>();
			writeQueue = new ArrayBlockingQueue<byte[]>(2048);

			selector = SelectorProvider.provider().openSelector();

			this.serverChannel = ServerSocketChannel.open();
			this.serverChannel.configureBlocking(false);

			InetSocketAddress isa = new InetSocketAddress(InetAddress.getByName("localhost"), port);
			this.serverChannel.socket().bind(isa);
			this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException ex) {
			//Logger.getLogger(Network.class.getName()).log(Level.SEVERE, null, ex);
			ex.printStackTrace();
			System.exit(0);
		}

	}

	public synchronized void start() {
		listening = true;
		notifyAll();
	}

	public synchronized void waitForListening() {
		while(!listening) {
			try {
				this.wait();
			}
			catch (InterruptedException e) {
			}
		}
	}

	public void closeConn() throws IOException {
		socket.close();
	}

	public Queue<byte[]> select() {
		Queue<byte[]> retMsgSet = null;
		try {
			selector.select();
			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
			//System.err.println("\tFIRED: " + selector.selectedKeys().size());
			while(iter.hasNext()) {
				SelectionKey selectedKey = iter.next();
				//System.out.println("Select "+selectedKey);
				iter.remove();
				if(!selectedKey.isValid()) {
					continue;
				}
				// Check what event is available and deal with it
				if (selectedKey.isAcceptable()) {
					//System.out.println("Acceptable socketChannel");
					accept(selectedKey);
				} else if (selectedKey.isReadable()) {
					//System.out.println("Read");
					SocketChannel sock = (SocketChannel)selectedKey.channel();

					Queue<byte[]> tempSet = null;
					tempSet = read(selectedKey);
					if(retMsgSet == null) {
						retMsgSet = tempSet;
					}
					else if(tempSet != null){
						retMsgSet.addAll(tempSet);
					}
				} 
				else if (selectedKey.isWritable()) {
					//System.out.println("Write");
					write();
				}
				else {
					//System.out.println("Something else");
				}

			}
		}
		catch (IOException e) {
			System.err.println("Select error");
			e.printStackTrace(System.err);
		}
		return retMsgSet;
	}

	protected synchronized void write() {
		 //System.out.println("Start of write "+System.currentTimeMillis());

		while(writeQueue.size()!=0) {
			//System.out.println("write sth");
			byte[] m = writeQueue.poll();
			if(m!=null) {
				try {
					int written = 0;
					int size = m.length;
					while(written < size) {
						//System.out.println("Write sth");
						written += socket.write(ByteBuffer.wrap(m));
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		this.sk.interestOps(SelectionKey.OP_READ);
		// System.out.println("End of write "+System.currentTimeMillis());

	}

	/**
	 * General function to send bytes to a node
	 */
	public synchronized void send(byte[] m) {
		 //System.out.println("Start of send "+System.currentTimeMillis());

		Debug.profileStart("SEND");
		//System.out.println("Send sth");
		if(socket == null) {
			BFT.Debug.kill("Socket to ZK client shoudl be set up before we send");
		}
		//this.sk.selector().wakeup();
		writeQueue.offer(m);
		if (this.sk.isValid()) {
                    this.sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);
                }
		else
			System.out.println("key is invalid");
		this.sk.selector().wakeup();
		Debug.profileFinis("SEND");
		// System.out.println("End of send "+System.currentTimeMillis());

	}

	/**
	 * Handles raw bytes from the selector and calls handle(bytes) 
	 * if necessary
	 * 
	 * @param bytes
	 */
	public Queue<byte[]> handleRawBytes(SocketChannel socket, byte[] bytes) {
		//BFT.//Debug.println("I got "+bytes.length+" bytes");
		 //System.out.println("Start of rawBytes "+System.currentTimeMillis());

		Queue<byte[]> parsedMsgBytes = null;

		//byte[] lengthBuf = UnsignedTypes.longToBytes((long)bytes.length);

		if(!storedBytes.containsKey(socket)) {
			storedBytes.put(socket, new byte[0]);
		}
		int target = 0;
		byte[] existingBytes = (byte [])storedBytes.get(socket);
		/*if(existingBytes.length > 0) {
            BFT.//Debug.println("there are already "+existingBytes.length+" bytes for this socket");
        }*/
		byte[] newBytes = new byte[existingBytes.length+bytes.length];
		System.arraycopy(existingBytes, 0, newBytes, 0, existingBytes.length);
		System.arraycopy(bytes, 0, newBytes, existingBytes.length, bytes.length);

		// CAUTION: I am hardcoding the number of bytes that constitute the length header

		if(newBytes.length < 4) { // we still dont know the length
			//////System.out.println("newBytes.length < 2. Do nothing");
			storedBytes.put(socket, newBytes);
		} else {    // we know the length of the message
			byte[] lenBytes = new byte[4];
			System.arraycopy(newBytes, 0, lenBytes, 0, 4);
			int len = (int)UnsignedTypes.bytesToLong(lenBytes);
			if(newBytes.length < 4+len) {       // MARKER change
				////System.out.println("More than 2 bytes, but still not enough");
				storedBytes.put(socket, newBytes);
			} else {
				////System.out.println("Enough bytes 2+("+len+")"+" and "+(newBytes.length-(2+len))+" restBytes");
				//parsedMsgBytes = new HashSet<byte[]>();
				byte[] dataBytes = new byte[len+4];
				int rest = newBytes.length-(4+len);
				byte[] restBytes = new byte[rest];
				//byte[] marker = new byte[4];
				System.arraycopy(newBytes, 0, dataBytes, 0, len+4);       // 0        to len+4
				System.arraycopy(newBytes, 4+len, restBytes, 0, rest);  // len+4   to end
				//System.out.println("COPY " + len+4 + " useful bytes");
				//int markerInt = (int)UnsignedTypes.bytesToLong(marker);
				// TLR 2009.1.23: Changed marker to be length of message
				//if(markerInt != len) {
					//System.out.println(myRole.toString() + " " + indexTable.get(socket));
				//	BFT.Debug.kill(new RuntimeException("invalid marker"));
				//}
				parsedMsgBytes = new LinkedList<byte[]>();


				parsedMsgBytes.add(dataBytes);
				storedBytes.remove(socket);
				////System.out.println("rest = "+rest+" restBytes.length = "+restBytes.length);
				if(restBytes.length > 0) { // if there are more bytes there
					////System.out.println("Calling handleRawBytes iteratively with "+restBytes.length+" bytes");

					//System.out.println("RECUR " + restBytes.length + " more bytes");
					// System.out.println("Start of handleBytes "+System.currentTimeMillis());
					Queue<byte[]> tempSet = handleRawBytes(socket, restBytes);
					// System.out.println("End of read "+System.currentTimeMillis());

					if (tempSet != null) {
						if (!tempSet.isEmpty()) {
							parsedMsgBytes.addAll(tempSet);
						} else {
							throw new RuntimeException(
							"This doesn't make sense");
						}
					}
				}
			}
		}
	 	//System.out.println("End of rawBytes "+System.currentTimeMillis());

		return parsedMsgBytes;
	}

	public synchronized void accept(SelectionKey key) throws IOException {
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

		// Accept the connection and make it non-blocking
		this.socket = serverSocketChannel.accept();

		this.socket.configureBlocking(false);

		// Register the new SocketChannel with our Selector, indicating
		// we'd like to be notified when there's data waiting to be read
		//System.out.println("Inside accept, direct register of OP_READ");
		this.sk = this.socket.register(this.selector, SelectionKey.OP_WRITE|SelectionKey.OP_READ);
		
	}


	public synchronized Queue<byte[]> read(SelectionKey key) throws IOException {
		//System.out.println("Start of read "+System.currentTimeMillis());
		SocketChannel socketChannel = (SocketChannel) key.channel();

		//System.out.println("before clear");
		// Clear out our read buffer so it's ready for new data
		this.readBuffer.clear();

		//System.out.println("before try");
		// Attempt to read off the channel
		int numRead;
		try {
			numRead = socketChannel.read(this.readBuffer);
		} catch (IOException ioe) {
			// The remote forcibly closed the connection, cancel
			// the selection key and close the channel.
			System.out.println("Exception in socketChannel.read");
			key.cancel();
			socketChannel.close();
			return null;
		}

		if (numRead == -1) {
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			System.out.println("Clean shutdown from ZK glue");
			key.channel().close();
			key.cancel();
			return null;
		}
		//System.out.println("before processData");
		// Hand the data off to our worker thread
		byte[] arr = readBuffer.array();
		//System.out.println("Arr");
		//BFT.util.UnsignedTypes.printBytes(arr);
		//BFT.//Debug.println("position="+readBuffer.position()+" numRead="+numRead);
		byte[] dataCopy = new byte[numRead];
		System.arraycopy(arr, 0, dataCopy, 0, numRead);
		//System.out.println("READ " + numRead);

		Queue<byte[]> tempSet = handleRawBytes(socketChannel, dataCopy);
		 //System.out.println("End of read "+System.currentTimeMillis());
		return tempSet;
	}

}


class ZKListener implements Runnable {

	ZKNetwork net;
	BlockingQueue<byte[]> queue;
	public ZKListener(ZKNetwork znet, BlockingQueue<byte[]> q) {
		net = znet;
		this.queue = q;
	}

	public void run() {
		Queue<byte[]> msgBytes = null;
		net.waitForListening();
		while(true) {
			msgBytes = net.select();
			if(msgBytes != null) {
				while(!msgBytes.isEmpty()) {
					queue.add(msgBytes.poll());
				}
				msgBytes = null;
			}
		}
	}
}

class ZKWorker implements Runnable {
	ZKClientGlue glue;
	protected BlockingQueue<byte[]> queue;

	public ZKWorker(ZKClientGlue zkcg, BlockingQueue<byte[]> q) {
		this.glue = zkcg;
		this.queue = q;
	}


	public void run() {

		while(true) {
			byte[] bytes = null;
			try {
				bytes = queue.poll(10, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
			}
			if(bytes != null) {
				glue.handleBytes(bytes);
			}
		}
	}
} 
