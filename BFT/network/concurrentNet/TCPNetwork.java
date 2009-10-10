// $Id$

/**
 * 
 */
package BFT.network.concurrentNet;

import BFT.network.Network;
import BFT.order.MessageFactory;
import BFT.order.messages.MessageTags;
import BFT.util.*;
import BFT.membership.*;
import BFT.messages.ClientRequest;
import BFT.messages.VerifiedMessageBase;

import java.io.IOException;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import BFT.Debug;
import BFT.Parameters;

/**
 * @author riche
 *
 */
public class TCPNetwork implements ConcurrentNetwork {

    private boolean listening;
    private Hashtable<String, SCWrapper> socketTable;
    // The channel on which we'll accept connections
    private ServerSocketChannel[] serverChannels;
    // The selector we'll be monitoring
    private Selector selector;
    // stores the available bytes on each socket
    private Hashtable<SocketChannel, byte[]> storedBytes;
    private Role myRole;
    private Membership membership;
    private ByteBuffer readBuffer = ByteBuffer.allocate(8*1024);
    private Hashtable<SocketAddress, Integer> indexTable =  null;

    public TCPNetwork(Role role, Membership members) {
	myRole = role;
	membership = members;
	listening = false;
	socketTable = new Hashtable<String, SCWrapper>();
	indexTable = new Hashtable<SocketAddress, Integer>();
	storedBytes = new Hashtable<SocketChannel, byte[]>();
	try {
	    selector = Selector.open();
	}
	catch (IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	Principal[] myInterfaces = membership.getMyInterfaces(myRole);

	String[] IPports = new String[myInterfaces.length];
	for(int i=0;i<myInterfaces.length;i++) {
	    IPports[i] = new String(myInterfaces[i].getIP().toString().split("/", 0)[1]+":"+myInterfaces[i].getPort());
	}

	// remove duplicates
	//        Set<String> set = new HashSet<String>(Arrays.asList(IPports));
	//        String[] IPportsNoDup = (String[])(set.toArray(new String[set.size()]));

	for(int i=0;i<IPports.length;i++) {
	    //System.out.println(IPports[i]);
	}

	serverChannels = new ServerSocketChannel[IPports.length];
	for(int i=0;i<IPports.length;i++) {
	    try {
		String ipStr = IPports[i].split(":", 0)[0];
		String portStr = IPports[i].split(":", 0)[1];

		this.serverChannels[i] = ServerSocketChannel.open();
		this.serverChannels[i].configureBlocking(false);

		// Bind the server socket to the specified address and port
		InetSocketAddress isa = new InetSocketAddress(InetAddress.getByName(ipStr), Integer.parseInt(portStr));
		System.err.println("Opening to "+ipStr+" "+portStr);
		this.serverChannels[i].socket().bind(isa);
		this.serverChannels[i].register(selector, SelectionKey.OP_ACCEPT);

		// Build index table
		indexTable.put(this.serverChannels[i].socket().getLocalSocketAddress(), i);

		//listenThreads[i] = new ListenerSocket(this, InetAddress.getByName(ipStr), Integer.parseInt(portStr));
		//listenThreads[i].start();
	    } catch (UnknownHostException ex) {
		//Logger.getLogger(BaseNode.class.getName()).log(Level.SEVERE, null, ex);
		System.out.println("failed (2) on establihsin ga server channel "+i);
		ex.printStackTrace();
	    } catch (IOException ioe) {
		System.out.println("failed on establishing server channel: "+i);
		ioe.printStackTrace();
	    }
	}


    }

 

    int count = 0;
    int bigcount = 0;
	
    /* (non-Javadoc)
     * @see BFT.network.concurrentNet.concurrentNetwork#send(byte[], int)
     */
    public void send(byte[] m, int index) {
	Debug.profileStart("SEND");
	String socketName = RoleMap.getRoleString(myRole, index);//myRole.toString() + index;
	SCWrapper wrapper = socketTable.get(socketName);
	SocketChannel socket = null;
	Principal p = null;
	if(wrapper != null) {
	    socket = wrapper.getSC();
	}
	wrapper = new SCWrapper(socket);
	boolean poked = wrapper.poke();
	//		System.out.println("sending to ."+index);
				
	if(socket == null && poked) {
	    switch (myRole) {
	    case CLIENT:
		p = membership.getClientNodes()[index];
		break;
	    case ORDER:
		p = membership.getOrderNodes()[index];
		break;
	    case EXEC:
		p = membership.getExecNodes()[index];
		break;
	    case FILTER:
		p = membership.getFilterNodes()[index];
		break;
	    default:
		throw new RuntimeException("Unknown Role "+myRole);
	    }
	    try {
		socket = SocketChannel.open();
		socket.configureBlocking(Parameters.blockingSends);
		socket.socket().setTcpNoDelay(true);
		socket.connect(new InetSocketAddress(p.getIP(), p.getPort()));
		socketTable.put(socketName, new SCWrapper(socket));
	    }
	    catch (IOException e) {
  		System.out.println("Failed on connection to "+
  				   myRole+"."+index);
		wrapper.kill();
		//e.printStackTrace(System.err);
		//System.exit(1);
	    }
	}
	byte[] lenBytes = UnsignedTypes.longToBytes((long)m.length);
	byte[] allBytes = new byte[m.length+8]; // MARKER change
	// TLR 2009.1.23: Changed marker to be length of message
    
	System.arraycopy(lenBytes, 0, allBytes, 0, 4);
	System.arraycopy(m, 0, allBytes, 4, m.length);
	System.arraycopy(lenBytes, 0, allBytes, m.length+4, 4);

	boolean cleanup = false;
	try {
	    if(socket.isConnected()) {
		int numWritten = socket.write(ByteBuffer.wrap(allBytes));
	    }
	    else {
		cleanup = true;
	    }
	    //			count++;
	    //			if(numWritten > 1500) {
	    //				bigcount++;
	    //			}
	    //			if(count % 1000 == 0) {
	    //				System.out.println(myRole.toString() + " SEGM " + (100*((double)bigcount)/count));
	    //			}
	}
	catch (IOException e) {
	    System.out.println("Failed on write to  "+
			       myRole+"."+index);
	    cleanup = true;
	    //e.printStackTrace(System.err);
	    //System.exit(1);
	}
	if(cleanup) {
	    try {
		socket.close();
	    }
	    catch (IOException e) {
		System.out.println("Failed on close");
	    }
	    socketTable.remove(socketName);
	}
	Debug.profileFinis("SEND");
    }

    /* (non-Javadoc)
     * @see BFT.network.concurrentNet.ConcurrentNetwork#start()
     */
    public synchronized void start() {
	listening = true;
	this.notifyAll();
    }

    /* (non-Javadoc)
     * @see BFT.network.concurrentNet.ConcurrentNetwork#stop()
     */
    public synchronized void stop() {
	listening = false;
	this.notifyAll();
    }
	
	

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
	protected void finalize() throws Throwable {
	super.finalize();
	for(SCWrapper w : socketTable.values()) {
	    try{
		w.getSC().close();
	    }catch(Exception e){
		System.out.println("exception while cleaning up the connectison");
	    }
	}
	for (int i = 0; i < serverChannels.length; i++)
	    try{
		serverChannels[i].socket().close();
	    }catch(Exception e){
		System.out.println("exception in cleanup of tcpnet");
	    }
		
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


    long start = 0;
    long finish = 0;
    long totaltime = 0;
    long select = 0;
    long starttotal = System.nanoTime();
    long finishtotal = 0;
    long loopcount = 0;
    public Queue<Pair<Integer, byte[]>> select() {
// 	loopcount++;
// 	if (loopcount % 10000 == 0){
// 	    System.out.println("\t\ttotaltime: "+ totaltime);
// 	    System.out.println("\t\tselectime: "+ select +" "+((float)select/(float)totaltime));
// 	    loopcount = 0;
// 	    totaltime = 0;
// 	    select = 0;
// 	}
// 	starttotal = System.nanoTime();
	Queue<Pair<Integer, byte[]>> retMsgSet = null;
	try {
	    //	    start = System.nanoTime();
	    selector.select();
	    //	    finish = System.nanoTime();
	    //	    select += finish - start;
	    
	    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
	    //System.err.println("\tFIRED: " + selector.selectedKeys().size());
	    while(iter.hasNext()) {
		SelectionKey selectedKey = iter.next();
		iter.remove();
		if(!selectedKey.isValid()) {
		    continue;
		}
		// Check what event is available and deal with it
		if (selectedKey.isAcceptable()) {
		    //////System.out.println("Acceptable socketChannel");
		    accept(selectedKey);
		} else if (selectedKey.isReadable()) {
		    //////System.out.println("Readable socketChannel");
		    SocketChannel sock = (SocketChannel)selectedKey.channel();
		    //System.err.println("\t\tRCVD FROM:" + sock.socket().getInetAddress().toString() + ":" + sock.socket().getPort());
		    Queue<Pair<Integer, byte[]>> tempSet = null;
		    tempSet = read(selectedKey);
		    if(retMsgSet == null) {
			retMsgSet = tempSet;
		    }
		    else if(tempSet != null){
			retMsgSet.addAll(tempSet);
		    }
		    //					if (retMsgSet != null) {
		    //						for (Pair<Integer, byte[]> p : retMsgSet) {
		    //							VerifiedMessageBase vmb = MessageFactory
		    //							.fromBytes(p.getRight());
		    //							switch (vmb.getTag()) {
		    //							case MessageTags.ClientRequest:
		    //								ClientRequest req = (ClientRequest) vmb;
		    //								//		    					System.err.println("TCP3("
		    //								//		    							+ req.getCore().getSendingClient() + ":"
		    //								//		    							+ req.getCore().getRequestId() + ")");
		    //								break;
		    //							default:
		    //								break;
		    //							}
		    //						}
		    //					}
		} 
		else {
		    //System.out.println("Something else");
		}
		//System.err.println("\tLEFT: " + selector.selectedKeys().size());
	    }
	}
	catch (IOException e) {
	    System.err.println("Select error");
	    e.printStackTrace(System.err);
	}

	//		if (retMsgSet != null) {
	//			for (Pair<Integer, byte[]> p : retMsgSet) {
	//				VerifiedMessageBase vmb = MessageFactory
	//				.fromBytes(p.getRight());
	//				switch (vmb.getTag()) {
	//				case MessageTags.ClientRequest:
	//					ClientRequest req = (ClientRequest) vmb;
	//					//					System.err.println("TCP4("
	//					//							+ req.getCore().getSendingClient() + ":"
	//					//							+ req.getCore().getRequestId() + ")");
	//					break;
	//				default:
	//					break;
	//				}
	//			}
	//		}
	//	totaltime += System.nanoTime() - starttotal;
	return retMsgSet;
    }

    protected void accept(SelectionKey key) {        
	try {
	    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();


	    // Accept the connection and make it non-blocking
	    SocketChannel socketChannel = serverSocketChannel.accept();

	    socketChannel.configureBlocking(false);

	    // Register the new SocketChannel with our Selector, indicating
	    // we'd like to be notified when there's data waiting to be read
	    ////System.out.println("Inside accept, direct register of OP_READ");
	    Integer i = indexTable.get(serverSocketChannel.socket().getLocalSocketAddress());
	    indexTable.put(socketChannel.socket().getLocalSocketAddress(), i);
	    socketChannel.register(this.selector, SelectionKey.OP_READ);
	    //System.err.println("ACCEPTING AGAIN: " + socketChannel.socket().getInetAddress().toString() + ":" + socketChannel.socket().getPort());
	}
	catch(ClosedChannelException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	catch(IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

    }

    public Queue<Pair<Integer, byte[]>> read(SelectionKey key) throws IOException {
	////System.out.println("Start of read");
	SocketChannel socketChannel = (SocketChannel) key.channel();

	////System.out.println("before clear");
	// Clear out our read buffer so it's ready for new data
	this.readBuffer.clear();

	////System.out.println("before try");
	// Attempt to read off the channel
	int numRead;
	try {
	    numRead = socketChannel.read(this.readBuffer);
	} catch (IOException ioe) {
	    // The remote forcibly closed the connection, cancel
	    // the selection key and close the channel.
	    //System.out.println("Exception in socketChannel.read");
	    for (Enumeration<String> e = socketTable.keys(); e.hasMoreElements();)
		{
		    String stringKey = e.nextElement();
		    SocketChannel sc = socketTable.get(stringKey).getSC();
		    if(sc.equals(socketChannel)) {
			socketTable.remove(stringKey);
			break;
		    }
		}
	    key.cancel();
	    socketChannel.close();
	    return null;
	}

	if (numRead == -1) {
	    // Remote entity shut the socket down cleanly. Do the
	    // same from our end and cancel the channel.
	    //System.out.println("Clean shutdown from other side");
	    for (Enumeration<String> e = socketTable.keys(); e.hasMoreElements();)
		{
		    String stringKey = e.nextElement();
		    SocketChannel sc = socketTable.get(stringKey).getSC();
		    if(sc.equals(socketChannel)) {
			socketTable.remove(stringKey);
			break;
		    }
		}
	    key.channel().close();
	    key.cancel();
	    return null;
	}
	////System.out.println("before processData");
	// Hand the data off to our worker thread
	byte[] arr = readBuffer.array();
	//BFT.//Debug.println("position="+readBuffer.position()+" numRead="+numRead);
	byte[] dataCopy = new byte[numRead];
	System.arraycopy(arr, 0, dataCopy, 0, numRead);


	//taylors//Queue<Pair<Integer, byte[]>> tempSet = handleRawBytes(socketChannel, dataCopy);
    Pair<Pair<Integer, byte[]>,byte[]> tempPair = null;
    Queue<Pair<Integer, byte[]>> tempSet = null;
    byte[] restBytes = dataCopy;
    while ( (tempPair = handleRawBytes(socketChannel, restBytes)) != null) {
        if (tempSet == null) {
            tempSet = new LinkedList<Pair<Integer, byte[]>>();
        }
        Pair<Integer, byte[]> parsedMsgBytes = tempPair.getLeft();
        restBytes = tempPair.getRight();
        tempSet.add(parsedMsgBytes);
        if(restBytes == null) {
            break;
        }
    }
	//		if (tempSet != null)
	//			for(Pair<Integer, byte[]> p : tempSet) {
	//				VerifiedMessageBase vmb = MessageFactory.fromBytes(p.getRight());
	//				switch(vmb.getTag()){
	//				case MessageTags.ClientRequest: ClientRequest req = (ClientRequest) vmb; /*System.err.println("TCP2(" + req.getCore().getSendingClient() + ":" + req.getCore().getRequestId() + ")"); */break;
	//				default: break;
	//				}
	//			}
	return tempSet;
    }

    public Pair<Pair<Integer, byte[]>, byte[]> handleRawBytes(SocketChannel socket, byte[] bytes) {
	/*BFT.//Debug.println("I got "+bytes.length+" bytes");
	  for(int i=0;i<bytes.length;i++) {
	  BFT.Debug.print(bytes[i]+" ");
	  }
	  BFT.//Debug.println();*/

	//Queue<Pair<Integer, byte[]>> parsedMsgBytes = null;
    Pair<Integer, byte[]> parsedMsgBytes = null;

	byte[] lengthBuf = UnsignedTypes.longToBytes((long)bytes.length);

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
        return null;
	} else {    // we know the length of the message
	    byte[] lenBytes = new byte[4];
	    System.arraycopy(newBytes, 0, lenBytes, 0, 4);
	    int len = (int)UnsignedTypes.bytesToLong(lenBytes);
	    if(newBytes.length < 4+len+4) {       // MARKER change
            ////System.out.println("More than 2 bytes, but still not enough");
            storedBytes.put(socket, newBytes);
            return null;
	    } else {
            ////System.out.println("Enough bytes 2+("+len+")"+" and "+(newBytes.length-(2+len))+" restBytes");
            //parsedMsgBytes = new LinkedList<Pair<Integer, byte[]>>();
            byte[] dataBytes = new byte[len];
            int rest = newBytes.length-(4+len+4);
            byte[] restBytes = new byte[rest];
            byte[] marker = new byte[4];
            System.arraycopy(newBytes, 4, dataBytes, 0, len);       // 4        to len+4
            System.arraycopy(newBytes, 4+len, marker, 0, 4);        // len+4    to len+8
            System.arraycopy(newBytes, 4+len+4, restBytes, 0, rest);  // len+8    to end

            int markerInt = (int)UnsignedTypes.bytesToLong(marker);
            // TLR 2009.1.23: Changed marker to be length of message
            if(markerInt != len) {
                //System.out.println(myRole.toString() + " " + indexTable.get(socket));
                BFT.Debug.kill(new RuntimeException("invalid marker"));
            }
            //taylors//parsedMsgBytes = new LinkedList<Pair<Integer, byte[]>>();

            parsedMsgBytes = new Pair<Integer, byte[]>(indexTable.get(socket.socket().getLocalSocketAddress()), dataBytes);
            //				VerifiedMessageBase vmb = MessageFactory.fromBytes(dataBytes);
            //				switch(vmb.getTag()){
            //				case MessageTags.ClientRequest: ClientRequest req = (ClientRequest) vmb; /*System.err.println("TCP1(" + req.getCore().getSendingClient() + ":" + req.getCore().getRequestId() + ")"); */break;
            //				default: break;
            //				}
            //reset the hashtable entry. This fixes the large message bug
            storedBytes.remove(socket);
            ////System.out.println("rest = "+rest+" restBytes.length = "+restBytes.length);
            if(restBytes.length > 0) { // if there are more bytes there
                ////System.out.println("Calling handleRawBytes iteratively with "+restBytes.length+" bytes");

                return new Pair<Pair<Integer, byte[]>, byte[]>(parsedMsgBytes, restBytes);
                /*Queue<Pair<Integer, byte[]>> tempSet = handleRawBytes(socket, restBytes);
                if (tempSet != null) {
                    if (!tempSet.isEmpty()) {
                        parsedMsgBytes.addAll(tempSet);
                    } else {
                        throw new RuntimeException(
                                       "This doesn't make sense");
                    }
                }*/
            } else {
                return new Pair<Pair<Integer, byte[]>, byte[]>(parsedMsgBytes, null);
            }
	    }
	}
	//return parsedMsgBytes;
    }

    /**
     * @return the myRole
     */
    public Role getMyRole() {
	return myRole;
    }

}

class SCWrapper {
	
    private final static int BASE_POKE_COUNT = 100;
	
    private int count = 0;
    private final SocketChannel sc;
	
    /**
     * @param sc
     */
    public SCWrapper(SocketChannel sc) {
	this.sc = sc;
    }

    /**
     * @return the sc
     */
    public SocketChannel getSC() {
	return sc;
    }

    public synchronized boolean poke() {
	boolean retVal = false;
	if(count-- == 0) {
	    retVal = true;
	}

	if(count < 0) {
	    count = 0;
	}
	return retVal;
    }
	
    public synchronized void kill() {
	count = BASE_POKE_COUNT;
    }
	
}
