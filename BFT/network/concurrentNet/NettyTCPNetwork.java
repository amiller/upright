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

import java.util.concurrent.Executors;
import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
//import org.jboss.netty.channel.MessageEvent;
//import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
//import org.jboss.netty.handler.codec.frame.FrameDecoder;



/**
 * @author riche
 *
 */
public class NettyTCPNetwork implements ConcurrentNetwork {

    private boolean listening;
    private Hashtable<String, SCWrapper> socketTable;
    // The channel on which we'll accept connections
    private ServerBootstrap[] bootstrap;
    private InetSocketAddress[] isas;
    
    // The selector we'll be monitoring
    //private Selector selector;
    // stores the available bytes on each socket
    //netty//private Hashtable<SocketChannel, byte[]> storedBytes;
    private Role myRole;
    private Membership membership;
    private ByteBuffer readBuffer = ByteBuffer.allocate(8*1024);
    //netty//private Hashtable<SocketAddress, Integer> indexTable =  null;
    private NetworkWorkQueue NWQ;

    public NettyTCPNetwork(Role role, Membership members, NetworkWorkQueue nwq) {
	myRole = role;
	membership = members;
    NWQ = nwq;
	listening = false;
	socketTable = new Hashtable<String, SCWrapper>();
	//netty//indexTable = new Hashtable<SocketAddress, Integer>();
	//netty//storedBytes = new Hashtable<SocketChannel, byte[]>();
	
	/*try {
	    selector = Selector.open();
	}
	catch (IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}*/
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

	bootstrap = new ServerBootstrap[IPports.length];
	isas = new InetSocketAddress[IPports.length];
	for(int i=0;i<IPports.length;i++) {
	    try {
		String ipStr = IPports[i].split(":", 0)[0];
		String portStr = IPports[i].split(":", 0)[1];

		bootstrap[i] = new ServerBootstrap(
				new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool(),
                1));

		//this.serverChannels[i] = ServerSocketChannel.open();
		//this.serverChannels[i].configureBlocking(false);

		// Bind the server socket to the specified address and port
		InetSocketAddress isa = new InetSocketAddress(InetAddress.getByName(ipStr), Integer.parseInt(portStr));
		System.err.println("Opening to "+ipStr+" "+portStr);
        //Pair<Role,Integer> pair = getRoleFromIndex(i);
		ServerHandler handler = new ServerHandler(role, i, NWQ);
		MessageDecoder decoder = new MessageDecoder(role , i);
		bootstrap[i].getPipeline().addLast("decoder", decoder);
		bootstrap[i].getPipeline().addLast("handler", handler);
		
		bootstrap[i].setPipelineFactory(new BFTPipelineFactory(role, i, NWQ));
		
        // Bind and start to accept incoming connections.
		bootstrap[i].bind(isa);
        //don't bind yet, we ll bind on start();
        //isas[i] = isa;

		//this.serverChannels[i].socket().bind(isa);
		//this.serverChannels[i].register(selector, SelectionKey.OP_ACCEPT);

		// Build index table
		//indexTable.put(this.serverChannels[i].socket().getLocalSocketAddress(), i);

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
    ////System.err.println("\tsending "+m.length+" bytes");
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
	
	public Queue<Pair<Integer, byte[]>> select() {
        throw new RuntimeException("no select should be called for NettyTCPNetwork");
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
    /*
	for (int i = 0; i < serverChannels.length; i++)
	    try{
		serverChannels[i].socket().close();
	    }catch(Exception e){
		System.out.println("exception in cleanup of tcpnet");
	    }
	*/	
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
    
    /**
     * @return the myRole
     */
    public Role getMyRole() {
	return myRole;
    }

    private Pair<Role,Integer> getRoleFromIndex(int id) {
        if(id==0) {
            return new Pair<Role,Integer>(Role.CLIENT, 0);
        } else if(Parameters.filtered && id<=Parameters.getFilterCount()) {
            return new Pair<Role,Integer>(Role.FILTER, new Integer(id-1));
        } else if(id<=Parameters.getFilterCount()+Parameters.getOrderCount()) {
            return new Pair<Role,Integer>(Role.ORDER, new Integer(id-Parameters.getFilterCount()-1));
        } else if(id<=Parameters.getFilterCount()+Parameters.getOrderCount()+Parameters.getExecutionCount()) {
            return new Pair<Role,Integer>(Role.EXEC, new Integer(id-Parameters.getFilterCount()-Parameters.getExecutionCount()-1));
        } else {
            throw new RuntimeException("invalid id: "+id);
        }
    }

}
