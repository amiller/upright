package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import BFT.generalcp.PrimaryHelperWrapper;
import BFT.generalcp.RequestInfo;

public class BftPrimaryGlue implements AppCPInterface {
	public static final Log LOG = LogFactory.getLog(BftPrimaryGlue.class.getName());
	
	Configuration conf;
	private BFTGlueNamenodeProtocol wrapperNamenode;
	private InetSocketAddress nameNodeAddr;
	private CPAppInterface generalCP;
	String generalCPDir;
	private HashMap<Integer, VersionedProtocol> proxyCache;
	
	public BftPrimaryGlue(Configuration _conf, 
			BFTGlueNamenodeProtocol namenode){
		this.conf = _conf;
		wrapperNamenode = namenode;
	}
	
	public void initialize() throws IOException {
		// get rpc client
		nameNodeAddr = NetUtils.createSocketAddr
		(FileSystem.getDefaultUri(conf).getAuthority());

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
		
		String shimConfigurationFile = System.getProperty("UpRightConfigFile");
		int shimId = Integer.parseInt(System.getProperty("UpRightShimID"));
		generalCPDir = conf.get("dfs.bft.checkpointDir", "/tmp/generalCP/") + shimId + File.separator;
		File cpDirLog = new File(generalCPDir + "logs");
		File cpDirSnapshot = new File(generalCPDir + "snapshots");
		if(!cpDirLog.exists()){
			cpDirLog.mkdirs();			
		}
		if(!cpDirSnapshot.exists()){
			cpDirSnapshot.mkdirs();
		}

		generalCP = new GeneralCP(shimId, shimConfigurationFile,
				cpDirSnapshot.getAbsolutePath(),
				cpDirLog.getAbsolutePath()
				);
		
		BftHelperGlue helper = new BftHelperGlue();
		try{
			helper.initialize(conf, generalCPDir, generalCP);
		} catch (IOException e){
			System.err.println("Failed to connect to namenode helper.");			
			throw new IOException("Failed to connect to namenode helper.");
		}
		
		PrimaryHelperWrapper wrapper = new PrimaryHelperWrapper(this, helper);
		generalCP.setupApplication(wrapper);		
		
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
			errorStr = canonicalizeExceptionStackTraceString(StringUtils.stringifyException(e));
			LOG.debug("errorClass: " + errorClass);
			LOG.debug("errorString: " + errorStr);
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
	
	// This is temporary(ad-hoc) fix
	// StackTrace for some exception may include methods whose names are
	// decided in a nondeterministic way (at least, I don't know how for now) 
	private String canonicalizeExceptionStackTraceString(String exception){
		String ret = exception;
		ret = ret.replaceAll("GeneratedMethodAccessor[0-9]+", "GeneratedMethodAccessor");
		return ret;
	}

	@Override
	public synchronized void execAsync(byte[] request, RequestInfo info) {
		LOG.debug("executing request seqNo : " + info.getSeqNo() + ", cid: " 
				+ info.getClientId() + ", reqId: " + info.getRequestId());
		
		// setup a logical time
		wrapperNamenode.setBftTime(info.getTime());
		wrapperNamenode.executeThreadFunctions();
		
		byte[] result = processRequest(request, info.getClientId());
		
		generalCP.execDone(result, info);
		
	}

	@Override
	public void execReadonly(byte[] request, int clientId, long requestId) {
		// NOT SUPPORTED YET
	}

	@Override
	public void loadSnapshot(String fileName) {
		LOG.info("#######\n########\n####### loadSnapshot : " + fileName);
		try {
			copyfile(fileName, wrapperNamenode.getImageName().getAbsolutePath());
			wrapperNamenode.reloadImage();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}
	static void copyfile(String srFile, String dtFile) throws IOException{
		LOG.debug("Copying file from : " + srFile + " to " + dtFile);
		
		BufferedInputStream in = null;
		BufferedOutputStream out = null;
		try{
			File f1 = new File(srFile);
			File f2 = new File(dtFile);
			in = new BufferedInputStream(new FileInputStream(f1));
			out = new BufferedOutputStream(new FileOutputStream(f2));

			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf, 0, buf.length)) > 0){
				out.write(buf, 0, len);
			}
		} finally {
			if(in != null){
				in.close();
			}
			if(out != null){
				out.close();
			}
		}
	}
	@Override
	public void sync() {
		assert false : "No sync on primary allowed";

	}

}
