package org.apache.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.BFTRequest.ProtocolType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;


public class BftClientWrapper implements ClientProtocol, DatanodeProtocol {
	public static final Log LOG = LogFactory.getLog(BftClientWrapper.class);
	private static int numRPCServerHandler = 1;

	private static int minMatchingReplies = 1;

	int timeout; // timeout for BFTRequest
	int maxRetries; // maximum number of retransmission of BFTRequest

	private ClientProtocol bftNamenodeWrapperClient; // We use java reflection
	private DatanodeProtocol bftNamenodeWrapperDatanode;  

	private Server server; // hadoop rpc server 
	BFTRequest.ProtocolType nodeType;

	DatagramSocket socket; // UDP socket
	ConcurrentHashMap<Long,BFTRequestContainer> requests; // bftrequests
	UserGroupInformation ticket;

	Configuration conf;

	Listener listener;

	public BftClientWrapper(BFTRequest.ProtocolType type,
			Configuration _conf) throws IOException{
		this.nodeType = type;
		this.conf = _conf;
		this.server = null;
		this.ticket = null;

		LOG.info("BFT Wrapper is created for " + nodeType);
	}

	public void initialize(UserGroupInformation ugi) throws IOException{

		this.ticket = ugi;

		// initialize hadoop rpc server

		String wrapperRPCServerAddr =       	
			conf.get("dfs.bft.clientWrapper.ip","localhost") + ":"
			+ conf.getInt("dfs.bft.clientwrapper.rpcport", 0);
		InetSocketAddress socAddr = 
			NetUtils.createSocketAddr(wrapperRPCServerAddr);

		// By default, we want to retry BFTRequest four times before RPC timeout occurs
		// note that BFTRequest is sent over UDP whereas RPC is over TCP
		this.timeout = conf.getInt("dfs.bft.client.timeout", 
				conf.getInt("ipc.client.timeout", 10000) / 4);

		this.maxRetries = conf.getInt("dfs.bft.client.max.retries", 4);

		this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
				numRPCServerHandler, false, conf);

		requests = new ConcurrentHashMap<Long,BFTRequestContainer>();

		this.server.start();	
		
		// initialize udp socket
		socket = new DatagramSocket();

		listener = new Listener();
		listener.start();

		//minMatchingReplies = conf.get("dfs.bft.namenode.replicaAddresses").split(",").length;
		
		// Initialize proxy for reflection
		String agreementServerAddr = 
			conf.get("dfs.bft.agreementServer.sockAddr","localhost:59770");

		InetSocketAddress agreementServer = 
			NetUtils.createSocketAddr(agreementServerAddr);

		if(nodeType == BFTRequest.ProtocolType.CLIENT){
			bftNamenodeWrapperClient =
				(ClientProtocol) Proxy.newProxyInstance(
						ClientProtocol.class.getClassLoader(),
						new Class[] { ClientProtocol.class },
						new Invoker(nodeType, agreementServer));
		}else{
			bftNamenodeWrapperDatanode =
				(DatanodeProtocol) Proxy.newProxyInstance(
						DatanodeProtocol.class.getClassLoader(), 
						new Class[] { DatanodeProtocol.class },
						new Invoker(nodeType, agreementServer));
		}


	}

	public InetSocketAddress getRPCServerAddr(){
		return this.server.getListenerAddress();
	}

	private class BFTRequestContainer{
		BFTRequest bftRequest;
		int	totalReplies;
		Hashtable<BFTReply,HashSet<String>> replies;
		public int retries;
		public boolean done;
		private Object retVal;
	  boolean error;
	  String errorClass;
	  String errorStr;

		public BFTRequestContainer(BFTRequest req){
			this.bftRequest = req;
			totalReplies = 0;
			replies = new Hashtable<BFTReply,HashSet<String>>();
	    done = false;
	    retries = 0;
	    error = false;
		}

		public int addReply(BFTReply newRep){

			HashSet<String> set;
			if(!replies.containsKey(newRep)){
				totalReplies++;
				set = new HashSet<String>();
				set.add(newRep.getReplicaID());
				replies.put(newRep, set);
			}else{
				set = replies.get(newRep);
				if(!set.contains(newRep.getReplicaID())){
					set.add(newRep.getReplicaID());
					totalReplies++;
				}
			}
			
			return set.size();
		}
		
	  public void setErr(String[] str){
	    error = true;
	    errorClass = str[0];
	    errorStr = str[1];
	  }
	  
	  public void setRetVal(Object retVal) {
	    error = false;
	    this.retVal = retVal;
	  }

	  public Object getRetVal() throws IOException {
	    if(error){
	      throw new RemoteException(errorClass, errorStr);
	    }
	    return retVal;
	  }
		
	}

	/**
	 * This thread receives replies from BFTNamenodeWrapper
	 * and notify a thread that waits on the request
	 */

	private class Listener extends Thread {

		private boolean running = false;

		private byte[] buffer;
		private DatagramPacket packet;

		public Listener() {
			running = true;

			buffer = new byte[2048];
			packet = new DatagramPacket(buffer, buffer.length);      

		}

		public void run() {
			while(running){

				// read a reply 
				try {
					socket.receive(packet);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				byte[] data = packet.getData();
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				DataInputStream dis = new DataInputStream(bais);
				Writable r = (Writable)ReflectionUtils.newInstance(BFTReply.class, conf);

				try {
					r.readFields(dis);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				BFTReply rep = (BFTReply)r;
				if (LOG.isDebugEnabled())
					LOG.debug("BFTReply received : #" + rep.getReqID());

				BFTRequestContainer reqContainer;
				// find a Request that corresponds to the reply

				reqContainer = requests.get(rep.getReqID());

				if( reqContainer != null &&
						reqContainer.addReply(rep) == minMatchingReplies ){

					synchronized(reqContainer){
						BFTRequest req = reqContainer.bftRequest;
						String[] strs = new String[2];
						if(rep.isError(strs)){
							reqContainer.setErr(strs);
						} else {
							reqContainer.setRetVal(((ObjectWritable)rep.getRetVal()).get());
						}
						reqContainer.done = true;
						reqContainer.notify();
					}
				} else {
					if(reqContainer == null){
						LOG.debug("Received a reply for requestID " + rep.getReqID() 
								+ ", but no corresponding request found.");
					} else {
						LOG.debug("Received a reply for requestID " + rep.getReqID() 
								+ ", but min matching replies has not received yet (total:"
								+ reqContainer.totalReplies + " )");
					}
				}

			}
		}
	}  


	/** A method invocation, including the method name and its parameters.*/
	public static class Invocation implements Writable, Configurable {
		private String methodName;
		private Class[] parameterClasses;
		private Object[] parameters;
		private Configuration conf;

		public Invocation() {}

		public Invocation(Method method, Object[] parameters) {
			this.methodName = method.getName();
			this.parameterClasses = method.getParameterTypes();
			this.parameters = parameters;
		}

		/** The name of the method invoked. */
		public String getMethodName() { return methodName; }

		/** The parameter classes. */
		public Class[] getParameterClasses() { return parameterClasses; }

		/** The parameter instances. */
		public Object[] getParameters() { return parameters; }

		public void readFields(DataInput in) throws IOException {
			methodName = UTF8.readString(in);
			parameters = new Object[in.readInt()];
			parameterClasses = new Class[parameters.length];
			ObjectWritable objectWritable = new ObjectWritable();

			for (int i = 0; i < parameters.length; i++) {
				parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
				parameterClasses[i] = objectWritable.getDeclaredClass();
			}

		}

		public void write(DataOutput out) throws IOException {
			UTF8.writeString(out, methodName);
			out.writeInt(parameterClasses.length);
			for (int i = 0; i < parameterClasses.length; i++) {
				ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
						conf);
			}
		}

		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append(methodName);
			buffer.append("(");
			if(parameters != null){
				for (int i = 0; i < parameters.length; i++) {
					if (i != 0)
						buffer.append(", ");
					buffer.append(parameters[i]);
				}
			}
			buffer.append(")");
			return buffer.toString();
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
		}

		public Configuration getConf() {
			return this.conf;
		}

	}


	/**
	 * Invoker that sends a message that corresponds to each hadoop rpc call
	 * to bftNamenodeWrapper
	 */
	private class Invoker implements InvocationHandler {

		InetSocketAddress destAddr;

		BFTRequest.ProtocolType nodeType;
		String myAddress;

		public Invoker(BFTRequest.ProtocolType type, InetSocketAddress nameNodeAddr) throws UnknownHostException{
			destAddr = nameNodeAddr;
			this.nodeType = type;
	    
	    String myIP = DNS.getDefaultIP(conf.get("dfs.datanode.dns.interface","default"));
	    
	    myAddress = myIP + ":" + socket.getLocalPort();
	    LOG.info("My BFT client ID is : "+myAddress);
	    
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable{

			// Construct and Send a bftRequest to bftNamenodeWrapper
			Invocation invocation = new Invocation(method, args);
			if (LOG.isDebugEnabled())
				LOG.debug("Sending : " + invocation);

			BFTRequest req = new BFTRequest(invocation, nodeType, ticket, myAddress);
			assert !requests.containsKey(req.getRequestID());
			BFTRequestContainer reqContainer = new BFTRequestContainer(req);
			requests.put(req.getRequestID(), reqContainer);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput dataOutput = new DataOutputStream(baos);

			req.write(dataOutput);

			DatagramPacket p = new DatagramPacket(baos.toByteArray(), baos.size(), destAddr);

			synchronized(reqContainer){
				do{
					socket.send(p);

					if (LOG.isDebugEnabled())
						LOG.debug("sent bft packet : #" + req.getRequestID() 
								+ ", retries:" + reqContainer.retries);

					// wait until we receive a reply that corresponds to this request
					reqContainer.wait(timeout);  
					reqContainer.retries++;
				}while(!reqContainer.done && reqContainer.retries <= maxRetries);
			}

			if(!reqContainer.done){
				//throw new SocketTimeoutException("BFT timeout");
				throw new IOException("BFT timeout");
			}
			
			requests.remove(reqContainer);
			
			return reqContainer.getRetVal();
		}

	}


	//
	// ClientProtocol - called by the rpc server
	//

	public void abandonBlock(Block b, String src, String holder)
	throws IOException {
		bftNamenodeWrapperClient.abandonBlock(b, src, holder);
	}


	public LocatedBlock addBlock(String src, String clientName)
	throws IOException {
		return bftNamenodeWrapperClient.addBlock(src, clientName);
	}

	public boolean complete(String src, String clientName) throws IOException {
		return bftNamenodeWrapperClient.complete(src, clientName);
	}

	public void create(String src, FsPermission masked, String clientName,
			boolean overwrite, short replication, long blockSize) throws IOException {
		bftNamenodeWrapperClient.create(src, masked, clientName, overwrite, replication, blockSize);
	}

	public boolean delete(String src) throws IOException {
		return bftNamenodeWrapperClient.delete(src);
	}

	public boolean delete(String src, boolean recursive) throws IOException {
		return bftNamenodeWrapperClient.delete(src, recursive);
	}

	public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
	throws IOException {
		return bftNamenodeWrapperClient.distributedUpgradeProgress(action);
	}

	public void finalizeUpgrade() throws IOException {
		bftNamenodeWrapperClient.finalizeUpgrade();
	}

	public void fsync(String src, String client) throws IOException {
		bftNamenodeWrapperClient.fsync(src, client);
	}

	public LocatedBlocks getBlockLocations(String src, long offset, long length)
	throws IOException {
		return bftNamenodeWrapperClient.getBlockLocations(src, offset, length);
	}

	public ContentSummary getContentSummary(String path) throws IOException {
		return bftNamenodeWrapperClient.getContentSummary(path);
	}

	public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
	throws IOException {
		return bftNamenodeWrapperClient.getDatanodeReport(type);
	}

	public FileStatus getFileInfo(String src) throws IOException {
		return bftNamenodeWrapperClient.getFileInfo(src);
	}

	public FileStatus[] getListing(String src) throws IOException {
		return bftNamenodeWrapperClient.getListing(src);
	}

	public long getPreferredBlockSize(String filename) throws IOException {
		return bftNamenodeWrapperClient.getPreferredBlockSize(filename);
	}

	public long[] getStats() throws IOException {
		return bftNamenodeWrapperClient.getStats();
	}

	public void metaSave(String filename) throws IOException {
		bftNamenodeWrapperClient.metaSave(filename);
	}

	public boolean mkdirs(String src, FsPermission masked) throws IOException {
		return bftNamenodeWrapperClient.mkdirs(src, masked);
	}

	public void refreshNodes() throws IOException {
		bftNamenodeWrapperClient.refreshNodes();
	}

	public boolean rename(String src, String dst) throws IOException {
		return bftNamenodeWrapperClient.rename(src, dst);
	}

	public void renewLease(String clientName) throws IOException {
		bftNamenodeWrapperClient.renewLease(clientName);
	}

	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		bftNamenodeWrapperClient.reportBadBlocks(blocks);
	}

	public void setOwner(String src, String username, String groupname)
	throws IOException { 
		bftNamenodeWrapperClient.setOwner(src, username, groupname);
	}

	public void setPermission(String src, FsPermission permission)
	throws IOException {
		bftNamenodeWrapperClient.setPermission(src, permission);
	}

	public boolean setReplication(String src, short replication)
	throws IOException {   
		return bftNamenodeWrapperClient.setReplication(src, replication);
	}

	public boolean setSafeMode(SafeModeAction action) throws IOException {
		return bftNamenodeWrapperClient.setSafeMode(action);
	}

	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {
		if(this.nodeType == ProtocolType.CLIENT){
			return bftNamenodeWrapperClient.getProtocolVersion(protocol, clientVersion);
		}else if(this.nodeType == ProtocolType.DATANODE){
			return bftNamenodeWrapperDatanode.getProtocolVersion(protocol, clientVersion);
		}else{
			throw new IOException();
		}

	}

	//
	// Datanode Protocol - called by the rpc server
	//


	public void blockReceived(DatanodeRegistration registration, Block[] blocks,
			String[] delHints) throws IOException {
		bftNamenodeWrapperDatanode.blockReceived(registration, blocks, delHints);
	}

	public DatanodeCommand blockReport(DatanodeRegistration registration,
			long[] blocks) throws IOException {
		return bftNamenodeWrapperDatanode.blockReport(registration, blocks);
	}

	public void errorReport(DatanodeRegistration registration, int errorCode,
			String msg) throws IOException {
		bftNamenodeWrapperDatanode.errorReport(registration, errorCode, msg);
	}

	public UpgradeCommand processUpgradeCommand(UpgradeCommand comm)
	throws IOException {
		return bftNamenodeWrapperDatanode.processUpgradeCommand(comm);
	}

	public DatanodeRegistration register(DatanodeRegistration registration)
	throws IOException {
		return bftNamenodeWrapperDatanode.register(registration);
	}

	public DatanodeCommand sendHeartbeat(DatanodeRegistration registration,
			long capacity, long dfsUsed, long remaining, int xmitsInProgress,
			int xceiverCount) throws IOException {
		return bftNamenodeWrapperDatanode.sendHeartbeat(registration, capacity, 
				dfsUsed, remaining, xmitsInProgress, xceiverCount);
	}

	public NamespaceInfo versionRequest() throws IOException {
		return bftNamenodeWrapperDatanode.versionRequest();
	}


	@Override
	public LocatedBlock append(String src, String clientName) throws IOException {
		return bftNamenodeWrapperClient.append(src, clientName);    
	}


	@Override
	public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
	throws IOException {
		bftNamenodeWrapperClient.setQuota(path, namespaceQuota, diskspaceQuota);
	}


	@Override
	public void setTimes(String src, long mtime, long atime) throws IOException {
		bftNamenodeWrapperClient.setTimes(src, mtime, atime);
	}


	@Override
	public void commitBlockSynchronization(Block block, long newgenerationstamp,
			long newlength, boolean closeFile, boolean deleteblock,
			DatanodeID[] newtargets) throws IOException {
		bftNamenodeWrapperDatanode.commitBlockSynchronization(block, newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
	}


	@Override
	public long nextGenerationStamp(Block block) throws IOException {
		return bftNamenodeWrapperDatanode.nextGenerationStamp(block);
	}

	@Override
	public LocatedBlock addBlock(String src, String clentName,
			byte[] hashOfPreviousBlock) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean complete(String src, String clientName, byte[] hasOfLastBlock)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DatanodeCommand blockReport(DatanodeRegistration dnRegistration,
			Block[] report) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean confirmBlockUpdate(String clientAddr, Block block)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}