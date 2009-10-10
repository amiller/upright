//$Id$

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.BftGlueInvocation;
import org.apache.hadoop.hdfs.BftGlueReply;
import org.apache.hadoop.hdfs.BftGlueRequest;
import org.apache.hadoop.hdfs.BftGlueRequest.NodeType;
import org.apache.hadoop.hdfs.protocol.BFTWrapperNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import BFT.messages.CommandBatch;
import BFT.messages.Entry;
import BFT.messages.NonDeterminism;
import BFT.network.TCPNetwork;
import BFT.serverShim.GlueShimInterface;
import BFT.serverShim.ServerShimInterface;
import BFT.serverShim.ShimBaseNode;

public class BftNamenodeGlue implements GlueShimInterface{
	
	private InetSocketAddress nameNodeAddr;
	private BFTWrapperNamenodeProtocol wrapperNamenode;
	
	// mainly used for taking, reloading and maintaining checkpoints
	private BftSecondaryNameNode bftSNameNode;
	
	private Configuration conf;
	
	private int nextSeqNum; // expected sequence number of next batch
	//private ServerShimInterface shim;
	private ShimBaseNode shim;
	
	// cache for hadoop RPC proxy
	private HashMap<Long, VersionedProtocol> proxyCache;
	
	private String shimConfigurationFile;
	private int	shimId;
	
	private String namenodeImageLocation;
		
	public BftNamenodeGlue(){
		conf = new Configuration();
	}
	
	public BftNamenodeGlue(Configuration _conf, 
			BFTWrapperNamenodeProtocol namenode){
		this.conf = _conf;
		wrapperNamenode = namenode;
	}
	
	public void initialize() throws IOException{						
		// get rpc client
		nameNodeAddr = NetUtils.createSocketAddr
		(FileSystem.getDefaultUri(conf).getAuthority());

		if(wrapperNamenode == null){
			try{
				wrapperNamenode = (BFTWrapperNamenodeProtocol)RPC.getProxy(
						BFTWrapperNamenodeProtocol.class,
						BFTWrapperNamenodeProtocol.versionID, nameNodeAddr, 
						UnixUserGroupInformation.login(conf, true), conf,
						NetUtils.getSocketFactory(conf,
								BFTWrapperNamenodeProtocol.class));
			} catch(IOException e){
				wrapperNamenode = null;
				e.printStackTrace();
			} catch (LoginException e) {
				wrapperNamenode = null;
				e.printStackTrace();
			}
		}
		
		bftSNameNode = new BftSecondaryNameNode(conf);
		nextSeqNum = 0;
		
		
		this.shimConfigurationFile = 
			conf.get("bft.configurationFile","config.properties");
		this.shimId = conf.getInt("bft.shimId", 0);
		/*
		// override the config file and/or node id if provided from command line
		this.shimConfigurationFile = System.getProperty("bft.configFile",
				this.shimConfigurationFile);
		this.shimId = Integer.parseInt(
				System.getProperty("bft.nodeId",String.valueOf(this.shimId)));
		*/
		
		
		ShimBaseNode shimBaseNode = 
			new ShimBaseNode(shimConfigurationFile, shimId, new byte[0]);
		shimBaseNode.setGlue(this);
		shim = shimBaseNode;
		shim.setNetwork(new TCPNetwork(shim));
		shim.start();
		
		proxyCache = new HashMap<Long, VersionedProtocol>();
		
		namenodeImageLocation = wrapperNamenode.getImageName().getCanonicalPath();
	}
	
	/*
	 * This is to avoid creating RPC client for each request.
	 * Instead, we cache RPC clients created previously.
	 * 
	 * TODO : add a thread that removes RPC clients not used for a long time
	 */
	private VersionedProtocol getProxy(BftGlueRequest req, long client)
	throws IOException {
		if(proxyCache.containsKey(client)){
			VersionedProtocol ret = proxyCache.get(client);
			NodeType nodetype = req.getNodeType();
			if((nodetype==NodeType.CLIENT && ret instanceof ClientProtocol)
					|| nodetype == NodeType.DATANODE && ret instanceof DatanodeProtocol){
				return ret;
			}else{
				proxyCache.remove(client);
			}
			
		}
		
		VersionedProtocol namenode = null;
		UserGroupInformation ticket = req.getTicket();
		
		if(req.getNodeType() == NodeType.CLIENT){
			namenode = (ClientProtocol)RPC.getProxy(ClientProtocol.class,
					ClientProtocol.versionID, nameNodeAddr, ticket, conf,
					NetUtils.getSocketFactory(conf, ClientProtocol.class));
		} else if(req.getNodeType() == NodeType.DATANODE){
			namenode = (DatanodeProtocol)RPC.getProxy(DatanodeProtocol.class,
					DatanodeProtocol.versionID, nameNodeAddr, ticket, conf,
					NetUtils.getSocketFactory(conf, DatanodeProtocol.class));
		} else {
			return null;
		}
		
		proxyCache.put(client, namenode);
		
		return namenode;
	}
	
	private byte[] processRequest(Entry entry){
		
		// reconstruct BftGlueRequest
		BftGlueRequest req;
		try {
			//System.out.println("Executing : " + new String(entry.getCommand()));
			req = BftGlueRequest.getRequestFromBytes(entry.getCommand(), conf);
		} catch (IOException e3) {
			e3.printStackTrace();
			return null;
		}
		
		// create hadoop RPC client for this call
		VersionedProtocol namenode = null;
		try {
			namenode = getProxy(req, entry.getClient());
		} catch (IOException e2) {
			e2.printStackTrace();
			return null;
		}
		
		if(namenode == null){
			return null;
		}
		
		Class implementation = namenode.getClass();
		BftGlueInvocation invocation = req.getInvocation();
		Method method = null;
		Object value = null;
		
		try {
			method =
				implementation.getMethod(invocation.getMethodName(),
						invocation.getParameterClasses());
			//("invoking : " + invocation.getMethodName());
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		boolean error = false;
		String errorClass = null;
		String errorStr = null;
		BftGlueReply rep;
		
		try {
						
			try{
				value = method.invoke(namenode, invocation.getParameters());
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}

		} catch (Throwable e){
			error = true;
			errorClass = e.getClass().getName();
			errorStr = StringUtils.stringifyException(e);
		}

		// create the reply
		if(!error){
			ObjectWritable retval = 
				new ObjectWritable(method.getReturnType(), value);
			rep = new BftGlueReply(retval, conf);
		}else{
			rep = new BftGlueReply(errorClass, errorStr, conf);
		}
		
		return rep.toBytes();
				
	}

	@Override
	public void exec(CommandBatch batch, long seqNo, NonDeterminism time,
			boolean takeCP) {
		/*
		if(seqNo != nextSeqNum){
			shim.lastExecuted(nextSeqNum);
			return;
		}
		*/
		wrapperNamenode.setBftTime(time.getTime());
		
		if(true){
		//if(time.getTime() - lastTimeThreadFunctionsExcuted 
		//		> minThreadTimeInverval){
			wrapperNamenode.executeThreadFunctions();
		}
		
		// Process requests in the batch
		for(Entry e : batch.getEntries()){			
			byte[] result = processRequest(e);
			shim.result(result, (int)e.getClient(),
					e.getRequestId(), seqNo, true);			
		}
		
		//if(takeCP){
		if(seqNo % 5 == 1){

			byte[] cpToken = null;			
			
			System.out.println("TAKE CP at GLUE");
			
			try {
				//cpToken = bftSNameNode.takeCheckpoint().toBytes();
				//cpToken = bftSNameNode.mydoCheckpoint(seqNo).toBytes();
				//if(seqNo % 10 == 1){
					cpToken = bftSNameNode.myBigCheckpoint(seqNo).toBytes();
					//cpToken = bftSNameNode.mydoCheckpoint(seqNo).toBytes();
				//} else {
					//cpToken = bftSNameNode.mydoCheckpoint(seqNo).toBytes();
				//}
				
				
				/*
				if(seqNo % 10000 == 9999){
				//if(false){
					cpToken = bftSNameNode.takeCheckpoint().toBytes();
				} else {
					cpToken = new byte[2];
					cpToken[0] = '0';
					cpToken[0] = '0';
				}*/
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			//System.out.println("TAKE CP at GLUE DONE!");
			
			if(cpToken != null && takeCP){
				shim.returnCP(cpToken, seqNo);
			}
		}
		
	}

	@Override
	public void fetchState(byte[] stateToken) {
		
		BftCheckpointSignature token;
		
		try {
			token = BftCheckpointSignature.getCheckpointSignatureFromBytes(
						stateToken);
		} catch (IOException e) {
			System.out.println("### *** specified token is broken!");
			e.printStackTrace();
			// corrupted checkpoint token
			return;
		}
		
		byte[] state;
		
		try {
			state = bftSNameNode.getStateInBytes(token);
		} catch (IOException e) {
			System.out.println("### *** specified token is unknown!" + e.getLocalizedMessage());
			e.printStackTrace();
			
			// failed to read the state
			return;
		}
		
		shim.returnState(stateToken, state);
		
	}

	@Override
	public void loadCP(byte[] appCPToken, long seqNo) {
		
		BftCheckpointSignature token;
		try {
			token = BftCheckpointSignature.getCheckpointSignatureFromBytes(
						appCPToken);
		} catch (IOException e) {
			// corrupted checkpoint token
			return;
		}
		
		try{
			wrapperNamenode.shutdownFS();
			bftSNameNode.copyCheckpoint(token, namenodeImageLocation);
			//bftSNameNode.uploadCheckpoint(token);
		} catch (NoSuchBftCheckpointFoundException e){
			System.out.println("Requested CP was not found!");
			shim.requestState(appCPToken);
			return;
		} catch (IOException ioe){
			ioe.printStackTrace();
			// Not sure what we should do here.
			// Maybe retry loading it or ask the shim for the state
			System.exit(-1);
		}
		
		try {
			wrapperNamenode.reloadImage();
		} catch (IOException e) {
			System.err.println("Reloading Image Failed! : "+ e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
		
		return;
		
	}

	@Override
	public void loadState(byte[] stateToken, byte[] State) {
		//System.out.println("LOADING NEW STATE!!");
		BftCheckpointSignature token;
		try {
			token = BftCheckpointSignature.getCheckpointSignatureFromBytes(
						stateToken);
		} catch (IOException e) {
			// corrupted checkpoint token
			return;
		}
		
		bftSNameNode.addCheckpoint(token, State);
		
		try {
			//System.out.println("COPY the passed state to namenode!!");
			bftSNameNode.copyCheckpoint(token, namenodeImageLocation);
		} catch (IOException e) {
			// This must not happen!
			e.printStackTrace();
			System.exit(-1);
		}
		
		try {
			//System.out.println("RELOAD IMAGE!!");
			wrapperNamenode.reloadImage();
		} catch (IOException e) {
			System.err.println("Reloading Image Failed! : "+ e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
		
		shim.readyForRequests();
		
	}

	@Override
	public void releaseCP(byte[] appCPToken) {
		BftCheckpointSignature token;
		try {
			token = BftCheckpointSignature.getCheckpointSignatureFromBytes(
						appCPToken);
		} catch (IOException e) {
			// corrupted checkpoint token
			return;
		}
		
		bftSNameNode.removeCheckpoint(token);
	}

	@Override
	public void execReadOnly(int clientId, long reqId, byte[] operation) {
		// TODO Auto-generated method stub
		
	}

}
