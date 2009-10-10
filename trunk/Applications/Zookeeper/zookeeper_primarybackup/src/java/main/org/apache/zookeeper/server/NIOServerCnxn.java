// $Id$

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import BFT.Debug;
import BFT.generalcp.*;
import org.apache.jute.*;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 */
public class NIOServerCnxn implements Watcher, ServerCnxn, Record {

	public enum ReplyType {
		Normal, Readonly, WatchEvent
	}

	private static final Logger LOG = Logger.getLogger(NIOServerCnxn.class);
	

	static public class Factory implements Record, AppCPInterface {
		ZooKeeperServer zks;
		GeneralCP gcp;
		//RequestLogger requestLogger=new RequestLogger(this);
		boolean inLoadLog = false;

		boolean isBackup = false;

		public void setBackup(){
			this.isBackup = true;
		}

		/**
		 * We use this buffer to do efficient socket I/O. Since there is a
		 * single sender thread per NIOServerCnxn instance, we can use a member
		 * variable to only allocate it once.
		 */
		ByteBuffer directBuffer = ByteBuffer.allocateDirect(64 * 1024);

		TreeMap<Integer, NIOServerCnxn> cnxns = new TreeMap<Integer, NIOServerCnxn>();

		int outstandingLimit = 1;

		// This is a temp field to transfer information from Factory to
		// ZooKeeperServer
		long nextSessionId = -1;

		// added by iodine, for adding transient states in snapshot
		public void deserialize(InputArchive archive, String tag)
				throws IOException {
			Debug.println("Deserialize NIOServerCnxn.Factory");
			archive.startRecord("NIOServerCnxn.Factory");
			this.nextSessionId = archive.readLong("nextSessionId");
			int size = archive.readInt("cnxnsSize");
			cnxns.clear();
			// Debug.println("cnxns.size=" + size);
			for (int i = 0; i < size; i++) {
				NIOServerCnxn cnxn = new NIOServerCnxn(zks, gcp, -1, this);
				archive.readRecord(cnxn, "cnxn" + i);
				cnxns.put(cnxn.getClientId(), cnxn);
			}

			archive.endRecord("NIOServerCnxn.Factory");
		}

		synchronized public void serialize(OutputArchive archive, String tag)
				throws IOException {
			Debug.println("Serialize NIOServerCnxn.Factory");
			archive.startRecord(this, "NIOServerCnxn.Factory");
			archive.writeLong(((SessionTrackerImpl) zks.sessionTracker)
					.getNextSessionId(), "nextSessionId");
			archive.writeInt(cnxns.size(), "cnxnsSize");
			int index = 0;
			TreeMap<Integer, NIOServerCnxn> tmp = new TreeMap<Integer, NIOServerCnxn>(
					cnxns);
			for (NIOServerCnxn cnxn : tmp.values()) {
				archive.writeRecord(cnxn, "cnxn" + index);
				index++;
			}

			archive.endRecord(this, "NIOServerCnxn.Factory");
			// Debug.println("End Serialize NIOServerCnxn.Factory");
		}

		public Factory() throws IOException {

		}

		public void startup(ZooKeeperServer zks) throws IOException,
				InterruptedException {
			zks.startup();
			setZooKeeperServer(zks);
		}

		public void setZooKeeperServer(ZooKeeperServer zks) {
			this.zks = zks;
			if (zks != null) {
				this.outstandingLimit = zks.getGlobalOutstandingLimit();
				zks.setServerCnxnFactory(this);
			} else {
				this.outstandingLimit = 1;
			}
		}

		public void setGeneralCP(GeneralCP gcp) {
			this.gcp = gcp;
		}

		public GeneralCP getGeneralCP() {
			return this.gcp;
		}

		public ZooKeeperServer getZooKeeperServer() {
			return this.zks;
		}

		private void addCnxn(int clientId, NIOServerCnxn cnxn) {
			synchronized (cnxns) {
				cnxns.put(clientId, cnxn);
			}
		}

		public NIOServerCnxn getCnxn(int clientId) {
			return cnxns.get(clientId);
		}

		protected NIOServerCnxn createConnection(int clientId)
				throws IOException {
			return new NIOServerCnxn(zks, gcp, clientId, this);
		}

		private long lastSeqNo=-1;
		@Override
		public void execAsync(byte[] request, RequestInfo info) {
			// Debug.println(info.clientId+" "+request.length+" "+cnxns.keys().nextElement());
			//if(!inLoadLog)
			//	requestLogger.addRequest(request, zks.getZxid(), info);
			if(lastSeqNo!=-1 && info.getSeqNo()!=lastSeqNo && info.getSeqNo()!=lastSeqNo+1)
				throw new RuntimeException("last="+lastSeqNo+" now="+info.getSeqNo());
			lastSeqNo = info.getSeqNo();
			//System.out.println("ZK exec "+info+" inLoadLog="+inLoadLog);
			try {
				if (request.length == 48
						&& cnxns.containsKey(info.getClientId())) {
					long sessionId = cnxns.get(info.getClientId())
							.getSessionId();
					System.out.println("Duplicate connectRequest for client "
							+ info.getClientId());
					this.closeSession(sessionId);
					zks.sessionTracker.removeSession(sessionId);
					// zks.closeSession(sessionId);

				}
				if (!cnxns.containsKey(info.getClientId())) {
					if (info.isReadonly())
						throw new RuntimeException(
								"Readonly should not be the first request");
					NIOServerCnxn cnxn = createConnection(info.getClientId());
					cnxns.put(info.getClientId(), cnxn);
				}
				// System.out.println(cnxns.get(info.clientId));
				cnxns.get(info.getClientId()).execRequest(request, info);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void execReadonly(byte[] request, int clientId, long requestId) {
			try {
				cnxns.get(clientId).execRequest(request,
						new RequestInfo(true, clientId, -1, requestId, -1, -1));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void sync() {
			System.out.println("ZK sync");
			zks.submitRequest(null, -1, OpCode.takeCP, 0, null, null, null);
		}

		@Override
		public void loadSnapshot(String fileName) {
			lastSeqNo=-1;
			try {
				//clear log before loading snapshot
				//requestLogger.clear();
				zks.loadData(new File(fileName));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//gcp.loadSnapshotDone();
		}

		/*@Override
		public void flushAndStartNewLog(long seqNo) {
			System.out.println("ZK flush log "+seqNo);
			requestLogger.flushLogs(seqNo);
		}*/
		
		/*public void flushDone(long seqNo, String fileName){
			System.out.println("ZK flush done "+seqNo+" "+fileName);
			gcp.flushDone(seqNo, fileName);
		}*/

		/*@Override
		public void consumeLog(String fileName) {
			System.out.println("ZK consumeLog "+fileName);
			// If need to consume log, then should clear previous log first.
			// For backup instance, this is redundant, since backup instance
			// should not have any in-memory logs.
			requestLogger.clear();
			this.inLoadLog = true;
			requestLogger.loadLog(fileName, zks.getZxid());
			gcp.consumeLogDone(fileName);
			this.inLoadLog = false;
		}*/

		public void enqueueReply(long watchSeqNo,
				ByteBuffer data, int clientId, RequestInfo info) {
			// If info is null, then this is a watcher event without request.
			if(isBackup)
				return;
			//System.out.println("ZK exec done "+info);
			if(info!=null){
				if(info.isReadonly())
					gcp.execReadonlyDone(data.array(), info.getClientId(), info
							.getRequestId());
				else
					gcp.execDone(data.array(), info);
			}
			else{
				//throw new RuntimeException("Watcher Not Implemented Yet");
				gcp.sendEvent(data.array(), clientId, watchSeqNo);
			}
		}

		public void syncDone(String fileName) {
			System.out.println("ZK syncDone");
			gcp.syncDone(fileName);
		}

		/**
		 * clear all the connections in the selector
		 */
		synchronized public void clear() {
			synchronized (cnxns) {
				// got to clear all the connections that we have in the selector
				for (Iterator<NIOServerCnxn> it = cnxns.values().iterator(); it
						.hasNext();) {
					NIOServerCnxn cnxn = it.next();
					it.remove();
					try {
						cnxn.close();
					} catch (Exception e) {
						// Do nothing.
					}
				}
			}
			try {
				zks.startup();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		public void shutdown() {
			if (zks != null) {
				zks.shutdown();
			}
		}

		synchronized void closeSession(long sessionId) {
			synchronized (cnxns) {
				for (Iterator<NIOServerCnxn> it = cnxns.values().iterator(); it
						.hasNext();) {
					NIOServerCnxn cnxn = it.next();
					if (cnxn.sessionId == sessionId) {
						it.remove();
						try {
							cnxn.close();
						} catch (Exception e) {
							LOG.warn("exception during session close", e);
						}
						break;
					}
				}
			}
		}
	}

	/**
	 * The buffer will cause the connection to be close when we do a send.
	 */
	static final ByteBuffer closeConn = ByteBuffer.allocate(0);

	Factory factory;

	ZooKeeperServer zk;

	GeneralCP gcp;

	boolean initialized;

	int clientId;

	ByteBuffer incomingBuffer = null;

	int sessionTimeout;

	ArrayList<Id> authInfo = new ArrayList<Id>();

	/**
	 * The number of requests that have been submitted but not yet responded to.
	 */
	int outstandingRequests;

	/**
	 * This is the id that uniquely identifies the session of a client. Once
	 * this session is no longer active, the ephemeral nodes will go away.
	 */
	long sessionId;

	private CnxnStats stats = new CnxnStats();

	private long watchSeqNo = 0;

	// added by iodine, for adding Watches in snapshot
	public void deserialize(InputArchive archive, String tag)
			throws IOException {
		Debug.println("Deserialize NIOServerCnxn");
		archive.startRecord("NIOServerCnxn");
		initialized = archive.readBool("initialized");
		clientId = archive.readInt("clientId");
		sessionTimeout = archive.readInt("sessionTimeout");
		authInfo.clear();
		int authInfoSize = archive.readInt("authInfoSize");
		for (int i = 0; i < authInfoSize; i++) {
			Id id = new Id();
			archive.readRecord(id, "authInfo" + i);
			authInfo.add(id);
		}
		// outstandingRequests = archive.readInt("outstandingRequests");
		sessionId = archive.readLong("sessionId");
		watchSeqNo = archive.readLong("watchSeqNo");
		// Debug.println("Deserialize NIOServerCnxn.sessionId=" + sessionId);
		// Debug.println("Deserialize NIOServerCnxn.clientId=" + clientId);
		// archive.readRecord(stats, "stats");
		archive.endRecord("NIOServerCnxn");
	}

	synchronized public void serialize(OutputArchive archive, String tag)
			throws IOException {
		Debug.profileStart("SerializeNIOServerCnxn");
		archive.startRecord(this, "NIOServerCnxn");
		archive.writeBool(initialized, "initialized");
		archive.writeInt(clientId, "clientId");
		archive.writeInt(sessionTimeout, "sessionTimeout");
		archive.writeInt(authInfo.size(), "authInfoSize");
		for (int i = 0; i < authInfo.size(); i++) {
			archive.writeRecord(authInfo.get(i), "authInfo" + i);
		}
		archive.writeLong(sessionId, "sessionId");
		archive.writeLong(watchSeqNo, "watchSeqNo");
		archive.endRecord(this, "NIOServerCnxn");
		Debug.profileFinis("SerializeNIOServerCnxn");
	}

	void sendBuffer(ByteBuffer bb, RequestInfo info) {
		factory.enqueueReply(this.watchSeqNo, bb, this.clientId, info);
		if (info == null) {
			// This is a watcher event
			this.watchSeqNo++;
		}
		stats.packetsSent++;
		ServerStats.getInstance().incrementPacketsSent();
	}

	public void execRequest(byte[] request, RequestInfo info)
			throws IOException, InterruptedException {
		incomingBuffer = ByteBuffer.wrap(request, 4, request.length - 4);
		// This is a duplicated connect request
		// System.out.println("Initialized "+this+" ="+initialized);
		if (!initialized) {
			// stats.packetsReceived++;
			// ServerStats.getInstance().incrementPacketsReceived();
			readConnectRequest(info);
		} else {
			// stats.packetsReceived++;
			// ServerStats.getInstance().incrementPacketsReceived();
			// Debug.println("Read request");
			readRequest(info);
			// Debug.println("End Read request");
		}
	}

	private void readRequest(RequestInfo info) throws IOException {
		// We have the request, now process and setup for next
		InputStream bais = new ByteBufferInputStream(incomingBuffer);
		BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
		RequestHeader h = new RequestHeader();
		h.deserialize(bia, "header");
		// Through the magic of byte buffers, txn will not be
		// pointing
		// to the start of the txn
		incomingBuffer = incomingBuffer.slice();
		if (h.getType() == OpCode.auth) {
			AuthPacket authPacket = new AuthPacket();
			ZooKeeperServer.byteBuffer2Record(incomingBuffer, authPacket);
			String scheme = authPacket.getScheme();
			AuthenticationProvider ap = ProviderRegistry.getProvider(scheme);
			if (ap == null
					|| ap.handleAuthentication(this, authPacket.getAuth()) != KeeperException.Code.Ok) {
				if (ap == null)
					LOG.error("No authentication provider for scheme: "
							+ scheme + " has "
							+ ProviderRegistry.listProviders());
				else
					LOG.debug("Authentication failed for scheme: " + scheme);
				// send a response...
				ReplyHeader rh = new ReplyHeader(h.getXid(), 0,
						KeeperException.Code.AuthFailed);
				sendResponse(rh, null, null, info);
				// ... and close connection
				sendBuffer(NIOServerCnxn.closeConn, null);
				disableRecv();
			} else {
				LOG.debug("Authentication succeeded for scheme: " + scheme);
				ReplyHeader rh = new ReplyHeader(h.getXid(), 0,
						KeeperException.Code.Ok);
				sendResponse(rh, null, null, info);
			}
			return;
		} else {
			zk.submitRequest(this, sessionId, h.getType(), h.getXid(),
					incomingBuffer, authInfo, info);
		}
		if (h.getXid() >= 0) {
			synchronized (this) {
				outstandingRequests++;
				// check throttling
				if (zk.getInProcess() > factory.outstandingLimit) {
					LOG.warn("Throttling recv " + zk.getInProcess());
					disableRecv();
					// following lines should not be needed since we are already
					// reading
					// } else {
					// enableRecv();
				}
			}
		}
	}

	public void disableRecv() {

	}

	public void enableRecv() {

	}

	private void readConnectRequest(RequestInfo info) throws IOException,
			InterruptedException {
		BinaryInputArchive bia = BinaryInputArchive
				.getArchive(new ByteBufferInputStream(incomingBuffer));
		byte[] tmp = incomingBuffer.array();
		/*
		 * System.out.println("connectRequest"); for (int i = 0; i < tmp.length;
		 * i++) { System.out.print(" " + tmp[i]); }
		 */
		ConnectRequest connReq = new ConnectRequest();
		connReq.deserialize(bia, "connect");
		LOG.warn("Connected to " + clientId + " lastZxid "
				+ connReq.getLastZxidSeen());
		if (zk == null) {
			throw new IOException("ZooKeeperServer not running");
		}
		if (connReq.getLastZxidSeen() > zk.dataTree.lastProcessedZxid) {
			LOG.error("Client has seen zxid 0x"
					+ Long.toHexString(connReq.getLastZxidSeen())
					+ " our last zxid is 0x"
					+ Long.toHexString(zk.dataTree.lastProcessedZxid));
			throw new IOException("We are out of date");
		}
		sessionTimeout = connReq.getTimeOut();
		byte passwd[] = connReq.getPasswd();
		if (sessionTimeout < zk.tickTime * 2) {
			sessionTimeout = zk.tickTime * 2;
		}
		if (sessionTimeout > zk.tickTime * 20) {
			sessionTimeout = zk.tickTime * 20;
		}
		// We don't want to receive any packets until we are sure that the
		// session is setup
		disableRecv();
		if (connReq.getSessionId() != 0) {
			setSessionId(connReq.getSessionId());
			zk.reopenSession(this, sessionId, passwd, sessionTimeout, info);
			LOG.warn("Renewing session 0x" + Long.toHexString(sessionId));
		} else {
			zk.createSession(this, passwd, sessionTimeout, info);
			LOG.warn("Creating new session 0x" + Long.toHexString(sessionId));
		}
		initialized = true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
	 */
	public int getSessionTimeout() {
		return sessionTimeout;
	}

	public NIOServerCnxn(ZooKeeperServer zk, GeneralCP gcp, int clientId,
			Factory factory) {
		this.zk = zk;
		this.gcp = gcp;
		this.clientId = clientId;
		this.factory = factory;
		if (gcp != null && clientId != -1) {
			InetAddress addr = gcp.getIP(clientId);
			authInfo.add(new Id("ip", addr.getHostAddress()));
			authInfo.add(new Id("host", addr.getCanonicalHostName()));
		}
	}

	/*
	 * @Override public String toString() { return
	 * "NIOServerCnxn object with clientId = " + clientId; }
	 */

	boolean closed;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.zookeeper.server.ServerCnxnIface#close()
	 */
	public void close() {
		if (closed) {
			return;
		}
		closed = true;
		synchronized (factory.cnxns) {
			factory.cnxns.remove(this.clientId);
		}
		if (zk != null) {
			zk.removeCnxn(this);
		}

		ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
				"close  NIOServerCnxn: " + clientId);
	}

	private final static byte fourBytes[] = new byte[4];

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.zookeeper.server.ServerCnxnIface#sendResponse(org.apache.zookeeper
	 * .proto.ReplyHeader, org.apache.jute.Record, java.lang.String)
	 */
	// modified by iodine, seqno is needed in response
	synchronized public void sendResponse(ReplyHeader h, Record r, String tag,
			RequestInfo info) {
		// Debug.println("sendResponse:" + info);
		//if(!factory.inLoadLog){
		//	System.out.println("sendResponse:"+info+"zxid="+h.getZxid());
		//}
		if (closed) {
			return;
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// Make space for length
		BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
		try {
			baos.write(fourBytes);
			bos.writeRecord(h, "header");
			if (r != null) {
				bos.writeRecord(r, tag);
			}
			baos.close();
		} catch (IOException e) {
			LOG.error("Error serializing response");
		}
		byte b[] = baos.toByteArray();
		ByteBuffer bb = ByteBuffer.wrap(b);

		bb.putInt(b.length - 4).rewind();
		sendBuffer(bb, info);
		if (h.getXid() > 0) {
			synchronized (this.factory) {
				outstandingRequests--;
				// check throttling
				if (zk.getInProcess() < factory.outstandingLimit
						|| outstandingRequests < 1) {
					enableRecv();
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper
	 * .proto.WatcherEvent)
	 */
	synchronized public void process(WatchedEvent event) {
		ReplyHeader h = new ReplyHeader(-1, -1L, 0);
		ZooTrace
				.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
						"Deliver event " + event + " to 0x"
								+ Long.toHexString(this.sessionId)
								+ " through " + this);

		// Convert WatchedEvent to a type that can be sent over the wire
		WatcherEvent e = event.getWrapper();

		sendResponse(h, e, "notification", null);
	}

	public void finishSessionInit(boolean valid, RequestInfo info) {
		try {
			ConnectResponse rsp = new ConnectResponse(0, valid ? sessionTimeout
					: 0, valid ? sessionId : 0, // send 0 if session is no
					// longer valid
					valid ? zk.generatePasswd(sessionId) : new byte[16]);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
			bos.writeInt(-1, "len");
			rsp.serialize(bos, "connect");
			baos.close();
			ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
			bb.putInt(bb.remaining() - 4).rewind();
			sendBuffer(bb, info);
			LOG.warn("Finished init of 0x" + Long.toHexString(sessionId) + ": "
					+ valid);
			if (!valid) {
				sendBuffer(closeConn, null);
			}
			// Now that the session is ready we can start receiving packets
			synchronized (this.factory) {
				enableRecv();
			}
		} catch (Exception e) {
			LOG.error("FIXMSG", e);
			close();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
	 */
	public long getSessionId() {
		return sessionId;
	}

	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}

	public ArrayList<Id> getAuthInfo() {
		return authInfo;
	}

	public int getClientId() {
		return this.clientId;
	}

	public InetSocketAddress getRemoteAddress() {
		return new InetSocketAddress(gcp.getIP(clientId), gcp.getPort(clientId));
	}

	private class CnxnStats implements ServerCnxn.Stats, Record {
		long packetsReceived;
		long packetsSent;

		public void serialize(OutputArchive archive, String tag)
				throws IOException {
			archive.startRecord(this, "CnxnStats");
			archive.writeLong(packetsReceived, "packetsReceived");
			archive.writeLong(packetsSent, "packetsSent");
			archive.endRecord(this, "CnxnStats");

		}

		public void deserialize(InputArchive archive, String tag)
				throws IOException {
			archive.startRecord("CnxnStats");
			packetsReceived = archive.readLong("packetsReceived");
			packetsSent = archive.readLong("packetsSent");
			archive.endRecord("CnxnStats");

		}

		/**
		 * The number of requests that have been submitted but not yet responded
		 * to.
		 */
		public long getOutstandingRequests() {
			return outstandingRequests;
		}

		public long getPacketsReceived() {
			return packetsReceived;
		}

		public long getPacketsSent() {
			return packetsSent;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(" ").append("[").append(clientId).append("](queued=")
					.append(getOutstandingRequests()).append(",recved=")
					.append(getPacketsReceived()).append(",sent=").append(
							getPacketsSent()).append(")\n");

			return sb.toString();
		}
	}

	public Stats getStats() {
		return stats;
	}

}
