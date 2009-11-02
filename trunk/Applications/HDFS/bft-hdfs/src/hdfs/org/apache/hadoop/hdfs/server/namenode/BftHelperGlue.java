package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.BftGlueInvocation;
import org.apache.hadoop.hdfs.BftGlueReply;
import org.apache.hadoop.hdfs.BftGlueRequest;
import org.apache.hadoop.hdfs.BftGlueRequest.NodeType;
import org.apache.hadoop.hdfs.protocol.BFTGlueNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import BFT.generalcp.AppCPInterface;
import BFT.generalcp.CPAppInterface;
import BFT.generalcp.GeneralCP;
import BFT.generalcp.RequestInfo;

public class BftHelperGlue implements AppCPInterface {
	public static final Log LOG = LogFactory.getLog(BftHelperGlue.class.getName());
	Configuration conf;
	String generalCPDir;
	private CPAppInterface generalCP;
	private HashMap<Integer, VersionedProtocol> proxyCache;
	private InetSocketAddress nameNodeAddr;
	private BFTGlueNamenodeProtocol wrapperNamenode;
	
	public void initialize(Configuration conf, String CPDir,
			CPAppInterface genCP) throws IOException{
		this.conf = conf;
		this.generalCPDir = CPDir;
		this.generalCP = genCP;
		
		// get rpc client
		String address = FileSystem.getDefaultUri(conf).getAuthority();
		if(address.indexOf(":") >= 0){
			String ip = address.split(":")[0];
			String port = address.split(":")[1];
			address = ip + ":" + (Integer.parseInt(port)+1);			
		} else {
			System.err.println("INCOMPATIBLE ADDR");
			System.exit(-1);
		}
		nameNodeAddr = NetUtils.createSocketAddr(address);

		if(wrapperNamenode == null){
			try{
				wrapperNamenode = (BFTGlueNamenodeProtocol)RPC.getProxy(
						BFTGlueNamenodeProtocol.class,
						BFTGlueNamenodeProtocol.versionID, nameNodeAddr, 
						UnixUserGroupInformation.login(conf, true), conf,
						NetUtils.getSocketFactory(conf,
								BFTGlueNamenodeProtocol.class));
			} catch(IOException e){
				wrapperNamenode = null;
				e.printStackTrace();
			} catch (LoginException e) {
				wrapperNamenode = null;
				e.printStackTrace();
			}
		}

		proxyCache = new HashMap<Integer, VersionedProtocol>();
	}
	
	/*
	 * This is to avoid creating RPC client for each request.
	 * Instead, we cache RPC clients created previously.
	 * 
	 * TODO : add a thread that removes RPC clients not used for a long time
	 */
	private VersionedProtocol getProxy(BftGlueRequest req, long client)
	throws IOException {
		UserGroupInformation ticket = req.getTicket();
		Integer key = getHashCode(req.getNodeType(), client, ticket);
		if(proxyCache.containsKey(key)){
			return proxyCache.get(key);
		}
		
		VersionedProtocol namenode = null;
		
		if(req.getNodeType() == NodeType.CLIENT){
			namenode = (ClientProtocol)RPC.getProxy(ClientProtocol.class,
					ClientProtocol.versionID, nameNodeAddr, ticket, conf,
					NetUtils.getSocketFactory(conf, ClientProtocol.class));
		} else if(req.getNodeType() == NodeType.DATANODE){
			namenode = (DatanodeProtocol)RPC.waitForProxy(DatanodeProtocol.class,
					DatanodeProtocol.versionID, nameNodeAddr, conf);					
		} else {
			return null;
		}
		
		proxyCache.put(key, namenode);
		
		return namenode;
	}
	
	private int getHashCode(NodeType nodetype, long clientID, UserGroupInformation ugi){
		int ret = (new Long(clientID)).hashCode() ^ nodetype.hashCode();
		if (nodetype == NodeType.DATANODE || ugi == null){
			return ret;
		}
		String[] groupNames = ugi.getGroupNames();
		if(groupNames != null && groupNames.length > 0){
		 ret ^= Arrays.hashCode(ugi.getGroupNames());
		}
		ret ^= ugi.getUserName().hashCode();	
		return ret;
	}
	
	private byte[] processRequest(byte[] request, int clientId){
		
		// reconstruct BftGlueRequest
		BftGlueRequest req;
		try {
			req = BftGlueRequest.getRequestFromBytes(request, conf);
		} catch (IOException e3) {
			e3.printStackTrace();
			return null;
		}
		
		// create hadoop RPC client for this call
		VersionedProtocol namenode = null;
		try {
			namenode = getProxy(req, clientId);
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
			LOG.debug("invoking : " + invocation.getMethodName());
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
	private long lastseq = -1;
	@Override
	public synchronized void execAsync(byte[] request, RequestInfo info) {

		lastseq = info.getSeqNo();
		
		// setup a logical time
		wrapperNamenode.setBftTime(info.getTime());
		wrapperNamenode.executeThreadFunctions();
		
		byte[] result = processRequest(request, info.getClientId());
		
		// Helper MUST NOT respond
		//generalCP.execDone(result, info);
		
	}

	@Override
	public void execReadonly(byte[] request, int clientId, long requestId) {
		// NO READONLY REQUEST SUPPORT YET
	}

	@Override
	public void loadSnapshot(String fileName) {
		LOG.info("#######\n########\n####### loadSnapshot : " + fileName);
		try {
			BftPrimaryGlue.copyfile(fileName, wrapperNamenode.getImageName().getAbsolutePath());
			wrapperNamenode.reloadImage();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	private String getSnapshotFileName(long syncSeqNum){
		return generalCPDir + File.separator + "snapshots" + File.separator + "Snapshot_" + syncSeqNum;
	}
	@Override
	public void sync() {
		String filename = null;
		try {
			filename = getSnapshotFileName(lastseq);
			wrapperNamenode.getFSImage().saveFSImage(new File(filename));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		generalCP.syncDone(filename);
	}
	
}
