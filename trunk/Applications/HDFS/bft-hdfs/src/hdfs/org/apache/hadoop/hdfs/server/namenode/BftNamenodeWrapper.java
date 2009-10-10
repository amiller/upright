package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BftClientWrapper.Invocation;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.BFTOrderedRequest;
import org.apache.hadoop.hdfs.BFTRequest;
import org.apache.hadoop.hdfs.BFTReply;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

public class BftNamenodeWrapper implements FSConstants {
	public static final Log LOG = LogFactory.getLog(BftNamenodeWrapper.class);
	VersionedProtocol namenode;
	BFTWrapperNamenodeProtocol wrapperNamenode;

	DatagramSocket socket; // UDP socket
	Configuration conf;
	String myID;

	InetSocketAddress agreementServer;
	//BFTRequest	initReq;
	boolean initialized;

	private Listener listener; 
	boolean running;

	// XXX if we somehow want to process bftrequests concurrently,
	// this should be replaced with something thread-safe
	// for now it is okay since we don't do that.
	HashMap<SocketAddress,BFTReply> replyCache;
	
	
	BftSecondaryNameNode bftSNameNode;

	public BftNamenodeWrapper(Configuration _conf) throws IOException, LoginException {

		this.conf = _conf;
		initialized = false;
		//initReq = null;

	}

	public void initialize() throws IOException, LoginException{
					
		// get rpc client
		namenode = null;
		InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr
		(FileSystem.getDefaultUri(conf).getAuthority());

		try{
			wrapperNamenode = (BFTWrapperNamenodeProtocol)RPC.getProxy(BFTWrapperNamenodeProtocol.class,
					BFTWrapperNamenodeProtocol.versionID, nameNodeAddr, UnixUserGroupInformation.login(conf, true), conf,
					NetUtils.getSocketFactory(conf, BFTWrapperNamenodeProtocol.class));
		} catch(IOException e){
			wrapperNamenode = null;
		}

		replyCache = new HashMap<SocketAddress, BFTReply>();

		String agreementServerAddr = 
			conf.get("dfs.bft.agreementServer.ip","localhost") 
			+ ":"	+ conf.getInt("dfs.bft.agreementServer.port", 59770);

		agreementServer = 
			NetUtils.createSocketAddr(agreementServerAddr);


		// create a UDP socket
		socket = new DatagramSocket(conf.getInt("dfs.bft.namenodewrapper.udpport", 59900));
		listener = new Listener();
		running = true;
		listener.start();

		myID = DNS.getDefaultIP("defaults") + ":" + socket.getLocalPort();
		//initReq = new BFTRequest(BFTRequest.ProtocolType.NAMENODE); 		

		//sendInitPacket();
		
		bftSNameNode = new BftSecondaryNameNode(conf);

		LOG.info("bftNamenodeWrapper initialized listening on port " + socket.getLocalPort());		
	}
	/*
	private void sendInitPacket() throws IOException{

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);

		initReq.write(dataOutput);

		DatagramPacket p = 
			new DatagramPacket(baos.toByteArray(), baos.size(), agreementServer);

		int retries = 0;

		synchronized(initReq){
			do{
				socket.send(p);
				try {
					initReq.wait(3000);
				} catch (InterruptedException e) {
					LOG.debug("Interrupted while waiting for a response to initReq");
				}
				retries++;
			}while(!initialized && retries < 5);
		}

		if(!initialized){
			throw new IOException("BFT wrapper creation timed out!");
		}

		initialized = true;
		initReq = null;
	}
	*/
	
	/**
	 * This thread receives a request from BftClinetWrapper
	 * (eventually from agreement cluster)
	 * and enqueues it
	 */

	private class Listener extends Thread {               

		private byte[] buffer;
		private DatagramPacket packet;

		private UserGroupInformation ticket;

		public Listener() {
			buffer = new byte[2048];
			packet = new DatagramPacket(buffer, buffer.length);
		}

		public void run() {

			InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr
			(FileSystem.getDefaultUri(conf).getAuthority());
			
			System.out.println("******" + nameNodeAddr);

			while(running){	
				try {
					socket.receive(packet);
				} catch (IOException e) {
					e.printStackTrace();
				}
				byte[] data = packet.getData();
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				DataInputStream dis = new DataInputStream(bais);
				Writable req = (Writable)ReflectionUtils.newInstance(BFTOrderedRequest.class, conf);
				try {
					req.readFields(dis);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					System.exit(-1);
				}

				BFTOrderedRequest or = (BFTOrderedRequest)req;
				System.out.println("Ordered Request Received\n" + or);
				
				if(or.getSequenceNumber() % 10 == 0){
					try {
						takeCP();
					} catch (IOException e) {
						e.printStackTrace();
						System.err.println("TAKE CP FAILED : " + e.getMessage());
					}
				}


				if(wrapperNamenode == null){
					try{
						wrapperNamenode = (BFTWrapperNamenodeProtocol)RPC.getProxy(BFTWrapperNamenodeProtocol.class,
								BFTWrapperNamenodeProtocol.versionID, nameNodeAddr, UnixUserGroupInformation.login(conf, true), conf,
								NetUtils.getSocketFactory(conf, BFTWrapperNamenodeProtocol.class));
					} catch(Exception e){
						wrapperNamenode = null;
					}
				}

				if(or.runThreadFunctions && wrapperNamenode != null){
					wrapperNamenode.executeThreadFunctions();
				}
				if(or.time > 0 && wrapperNamenode != null){
					wrapperNamenode.setBftTime(or.time);
				}

				if(or.isVoid){
					/*
					if(!initialized && initReq != null){
						synchronized(initReq){
							initReq.notifyAll();
						}
					}
					*/
					continue;
				}
				
				

				BFTRequest bftReq = or.getRequest();
				assert bftReq != null;


				SocketAddress clientAddr = NetUtils.createSocketAddr(bftReq.getReturnAddr());
				//SocketAddress clientAddr = packet.getSocketAddress();

				// Check out replyCache
				BFTReply bftReply = replyCache.get(clientAddr);


				if(bftReply == null || bftReply.getReqID() < bftReq.getRequestID()){
					boolean error = false;
					String errorClass = null;
					String errorStr = null;

					ticket = bftReq.getTicket();

					if (LOG.isDebugEnabled())
						LOG.debug("bft request received : #" + bftReq.getRequestID() + ", type : " + bftReq.getProtocolType() );


					//if(bftReq.getFormatFlag()){
					if(false){
						//
						// FileSystem format request
						//
						/*
						try{
							NameNode.format(conf, or.time, or.randomSeed);
						} catch (IOException e){
							error = true;
							errorClass = e.getClass().getName();
							errorStr = StringUtils.stringifyException(e);
						}

						if(!error){
							Writable retval = new ObjectWritable(void.class, null);
							bftReply = new BFTReply(bftReq.getRequestID(), retval);
						}else{
							bftReply = new BFTReply(bftReq.getRequestID(), errorClass, errorStr);
						}
						*/

					} else {
						//
						// Normal RPC call to namenode
						// 
						try {
							BFTRequest.ProtocolType protocolType = bftReq.getProtocolType();
							if(protocolType == BFTRequest.ProtocolType.CLIENT){
								namenode = (ClientProtocol)RPC.getProxy(ClientProtocol.class,
										ClientProtocol.versionID, nameNodeAddr, ticket, conf,
										NetUtils.getSocketFactory(conf, ClientProtocol.class));
							}else if(protocolType == BFTRequest.ProtocolType.DATANODE){
								namenode = (DatanodeProtocol)RPC.getProxy(DatanodeProtocol.class,
										DatanodeProtocol.versionID, nameNodeAddr, ticket, conf,
										NetUtils.getSocketFactory(conf, DatanodeProtocol.class));
							}else{
								throw new IOException();
							}
						} catch (IOException e) {
							e.printStackTrace();
							continue;
						}

						Class implementation = namenode.getClass();
						Invocation invocation = bftReq.getInvocation();
						Method method = null;
						Object value = null;

						try {
							method =
								implementation.getMethod(invocation.getMethodName(),
										invocation.getParameterClasses());
						} catch (SecurityException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							continue;
						} catch (NoSuchMethodException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							continue;
						}

						//if (LOG.isDebugEnabled())
						LOG.debug("invoking :" + method);

						try {
							value = method.invoke(namenode, invocation.getParameters());
						} catch (Throwable e){
							error = true;
							errorClass = e.getClass().getName();
							errorStr = StringUtils.stringifyException(e);
						}
						if(!error){
							Writable retval = new ObjectWritable(method.getReturnType(), value);

							bftReply = new BFTReply(bftReq.getRequestID(), retval);
						}else{
							bftReply = new BFTReply(bftReq.getRequestID(), errorClass, errorStr);
						}
					}
					
					bftReply.setReplicaID(myID);

					//  Cache the result
					replyCache.put(clientAddr, bftReply);          
				} else {

					if (LOG.isDebugEnabled())
						LOG.debug("got result from the cache!" + bftReply.getReqID());

				}

				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutput dataOutput = new DataOutputStream(baos);

				try {
					bftReply.write(dataOutput);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}

				try {
					System.out.println("reply #" +bftReply.getReqID() + " sent to " + clientAddr);
					DatagramPacket p = new DatagramPacket(baos.toByteArray(), baos.size(), clientAddr);
					socket.send(p);


					if (LOG.isDebugEnabled())
						LOG.debug("reply #" +bftReply.getReqID() + " sent to " + clientAddr);


				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				} 

			}
		}
	}
	
	private BftCheckpointSignature takeCP() throws IOException{
		BftCheckpointSignature bftSig;
		
		bftSig = bftSNameNode.takeCheckpoint();		
		
		return bftSig;						
	}
	
	private void loadCP(BftCheckpointSignature bftSig) throws IOException{
		bftSNameNode.uploadCheckpoint(bftSig);
		wrapperNamenode.reloadImage();
	}
	

	/**
	 * This consumes queued requests by making hadoop rpc
	 *
	 */

	private class Handler extends Thread {

		public void run() {
			while(running){

				//read a request from the request queue

				// make an rpc to namenode

				// create a reply and send it back to the client

			}
		}

	}

	private class RequestLog {



		RequestLog(){

		}

	}

	public static void main(String[] args) throws Exception{

		BftNamenodeWrapper wrapper = new BftNamenodeWrapper(new Configuration());
		wrapper.initialize();
		try {
			wrapper.listener.join();
		} catch (InterruptedException e) {
			LOG.error(StringUtils.stringifyException(e));
			System.exit(-1);
		}

	}
}

