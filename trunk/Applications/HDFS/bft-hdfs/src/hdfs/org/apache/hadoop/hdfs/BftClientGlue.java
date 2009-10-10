//$Id$

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.BftGlueRequest.NodeType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import BFT.clientShim.ClientShimBaseNode;
import BFT.network.TCPNetwork;

public class BftClientGlue implements ClientProtocol, DatanodeProtocol{
	public static final Log LOG = LogFactory.getLog(BftClientGlue.class);
	// we might need/want to send multiple calls in a single request
	// but for now we assume one RPC call per each request
	private static final int numRPCServerHandler = 1;

	private NodeType nodeType; // type of node to which this glue attach
	
	//	We use java reflection
	private ClientProtocol proxyClientProtocol; 
	private DatanodeProtocol proxyDataNodeProtocol;
	
	// hadoop RPC server
	private Server server;
	
	private UserGroupInformation ticket;
	private Configuration conf;
	
	private String shimConfigurationFile;
	private int	shimId;
	
	public BftClientGlue(NodeType type,
			Configuration _conf) throws IOException{
		this.nodeType = type;
		this.conf = _conf;
		this.server = null;
		this.ticket = null;
	}

	public void initialize(UserGroupInformation ugi) throws IOException{

		this.ticket = ugi;
		this.shimConfigurationFile = conf.get("bft.configurationFile",
				"config.properties");
		this.shimId = conf.getInt("bft.shimId", 0);
		/*
		// override the config file and/or node id if provided from command line
		this.shimConfigurationFile = System.getProperty("bft.configFile",
				this.shimConfigurationFile);
		this.shimId = Integer.parseInt(
				System.getProperty("bft.nodeId",String.valueOf(this.shimId)));
		*/
		
		// Initialize proxy for reflection
		try{
		if(nodeType == NodeType.CLIENT){
			proxyClientProtocol =
				(ClientProtocol) Proxy.newProxyInstance(
						ClientProtocol.class.getClassLoader(),
						new Class[] { ClientProtocol.class },
						new Invoker());
		}else{
			proxyDataNodeProtocol =
				(DatanodeProtocol) Proxy.newProxyInstance(
						DatanodeProtocol.class.getClassLoader(), 
						new Class[] { DatanodeProtocol.class },
						new Invoker());
		}}catch(Exception e){
			e.printStackTrace();
			System.out.println(e.getMessage());
			throw new IOException(e);
		}
		
		// initialize hadoop rpc server
		String wrapperRPCServerAddr =       	
			conf.get("dfs.bft.clientWrapper.ip","localhost") + ":"
			+ conf.getInt("dfs.bft.clientwrapper.rpcport", 0);
		InetSocketAddress socAddr = 
			NetUtils.createSocketAddr(wrapperRPCServerAddr);

		this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
				numRPCServerHandler, false, conf);
		this.server.start();
	}
	
	public InetSocketAddress getRPCServerAddr(){
		return this.server.getListenerAddress();
	}
	
	
	/**
	 * Invoker that sends a message that corresponds to each hadoop rpc call
	 * to bftNamenodeWrapper
	 */
	private class Invoker implements InvocationHandler {
		
		ClientShimBaseNode clientShim;
		
		public Invoker(){
			// here we instantiate cleint shim
			clientShim = new ClientShimBaseNode(shimConfigurationFile, shimId);
			clientShim.setNetwork(new TCPNetwork(clientShim));
			clientShim.start();
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable{
			BftGlueInvocation invocation = new BftGlueInvocation(method, args);
			LOG.debug("Calling : " + invocation);
			BftGlueRequest req = new BftGlueRequest(invocation, nodeType, ticket, conf);
			byte[] request = req.toBytes(); 
			byte[] result = clientShim.execute(request);			
			BftGlueReply reply = BftGlueReply.getReplyFromBytes(result, conf);			
			return reply.getReturnValue().get();
		}

	}
	
	
	//
	// ClientProtocol - called by the rpc server
	//

	public void abandonBlock(Block b, String src, String holder)
	throws IOException {
		proxyClientProtocol.abandonBlock(b, src, holder);
	}

	public LocatedBlock addBlock(String src, String clientName)
	throws IOException {
		return proxyClientProtocol.addBlock(src, clientName);
	}
	
	public LocatedBlock addBlock(String src, String clientName, byte[] hash)
	throws IOException {
		return proxyClientProtocol.addBlock(src, clientName, hash);
	}

	public boolean complete(String src, String clientName) throws IOException {
		return proxyClientProtocol.complete(src, clientName);
	}
	
	public boolean complete(String src, String clientName, byte[] hash) throws IOException {
		return proxyClientProtocol.complete(src, clientName, hash);
	}

	public void create(String src, FsPermission masked, String clientName,
			boolean overwrite, short replication, long blockSize) 
	throws IOException {
		proxyClientProtocol.create(src, masked, clientName,
				overwrite, replication, blockSize);
	}

	public boolean delete(String src) throws IOException {
		return proxyClientProtocol.delete(src);
	}

	public boolean delete(String src, boolean recursive) throws IOException {
		return proxyClientProtocol.delete(src, recursive);
	}

	public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
	throws IOException {
		return proxyClientProtocol.distributedUpgradeProgress(action);
	}

	public void finalizeUpgrade() throws IOException {
		proxyClientProtocol.finalizeUpgrade();
	}

	public void fsync(String src, String client) throws IOException {
		proxyClientProtocol.fsync(src, client);
	}

	public LocatedBlocks getBlockLocations(String src, long offset, long length)
	throws IOException {
		return proxyClientProtocol.getBlockLocations(src, offset, length);
	}

	public ContentSummary getContentSummary(String path) throws IOException {
		return proxyClientProtocol.getContentSummary(path);
	}

	public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
	throws IOException {
		return proxyClientProtocol.getDatanodeReport(type);
	}

	public FileStatus getFileInfo(String src) throws IOException {
		return proxyClientProtocol.getFileInfo(src);
	}

	public FileStatus[] getListing(String src) throws IOException {
		return proxyClientProtocol.getListing(src);
	}

	public long getPreferredBlockSize(String filename) throws IOException {
		return proxyClientProtocol.getPreferredBlockSize(filename);
	}

	public long[] getStats() throws IOException {
		return proxyClientProtocol.getStats();
	}

	public void metaSave(String filename) throws IOException {
		proxyClientProtocol.metaSave(filename);
	}

	public boolean mkdirs(String src, FsPermission masked) throws IOException {
		return proxyClientProtocol.mkdirs(src, masked);
	}

	public void refreshNodes() throws IOException {
		proxyClientProtocol.refreshNodes();
	}

	public boolean rename(String src, String dst) throws IOException {
		return proxyClientProtocol.rename(src, dst);
	}

	public void renewLease(String clientName) throws IOException {
		proxyClientProtocol.renewLease(clientName);
	}

	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		proxyClientProtocol.reportBadBlocks(blocks);
	}

	public void setOwner(String src, String username, String groupname)
	throws IOException { 
		proxyClientProtocol.setOwner(src, username, groupname);
	}

	public void setPermission(String src, FsPermission permission)
	throws IOException {
		proxyClientProtocol.setPermission(src, permission);
	}

	public boolean setReplication(String src, short replication)
	throws IOException {   
		return proxyClientProtocol.setReplication(src, replication);
	}

	public boolean setSafeMode(SafeModeAction action) throws IOException {
		return proxyClientProtocol.setSafeMode(action);
	}

	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {
		if(this.nodeType == NodeType.CLIENT){
			return proxyClientProtocol.getProtocolVersion(
					protocol, clientVersion);
		}else if(this.nodeType == NodeType.DATANODE){
			return proxyDataNodeProtocol.getProtocolVersion(
					protocol, clientVersion);
		}else{
			throw new IOException();
		}

	}

	//
	// Datanode Protocol - called by the rpc server
	//


	public void blockReceived(DatanodeRegistration registration,
			Block[] blocks,	String[] delHints) throws IOException {
		proxyDataNodeProtocol.blockReceived(registration, blocks, delHints);
	}

	public DatanodeCommand blockReport(DatanodeRegistration registration,
			long[] blocks) throws IOException {
		return proxyDataNodeProtocol.blockReport(registration, blocks);
	}
	
	public DatanodeCommand blockReport(DatanodeRegistration registration,
			Block[] blocks) throws IOException {
		return proxyDataNodeProtocol.blockReport(registration, blocks);
	}

	public void errorReport(DatanodeRegistration registration, int errorCode,
			String msg) throws IOException {
		proxyDataNodeProtocol.errorReport(registration, errorCode, msg);
	}

	public UpgradeCommand processUpgradeCommand(UpgradeCommand comm)
	throws IOException {
		return proxyDataNodeProtocol.processUpgradeCommand(comm);
	}

	public DatanodeRegistration register(DatanodeRegistration registration)
	throws IOException {
		return proxyDataNodeProtocol.register(registration);
	}

	public DatanodeCommand sendHeartbeat(DatanodeRegistration registration,
			long capacity, long dfsUsed, long remaining, int xmitsInProgress,
			int xceiverCount) throws IOException {
		return proxyDataNodeProtocol.sendHeartbeat(registration, capacity, 
				dfsUsed, remaining, xmitsInProgress, xceiverCount);
	}

	public NamespaceInfo versionRequest() throws IOException {
		return proxyDataNodeProtocol.versionRequest();
	}


	@Override
	public LocatedBlock append(String src, String clientName)
	throws IOException {
		return proxyClientProtocol.append(src, clientName);    
	}


	@Override
	public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
	throws IOException {
		proxyClientProtocol.setQuota(path, namespaceQuota, diskspaceQuota);
	}


	@Override
	public void setTimes(String src, long mtime, long atime)
	throws IOException {
		proxyClientProtocol.setTimes(src, mtime, atime);
	}


	@Override
	public void commitBlockSynchronization(Block block, 
			long newgenerationstamp, long newlength, boolean closeFile,
			boolean deleteblock, DatanodeID[] newtargets) throws IOException {
		proxyDataNodeProtocol.commitBlockSynchronization(block, 
				newgenerationstamp, newlength, closeFile,
				deleteblock, newtargets);
	}


	@Override
	public long nextGenerationStamp(Block block) throws IOException {
		return proxyDataNodeProtocol.nextGenerationStamp(block);
	}

	@Override
	public boolean confirmBlockUpdate(String clientAddr, Block block)
			throws IOException {
		return proxyDataNodeProtocol.confirmBlockUpdate(clientAddr, block);
	}
}
