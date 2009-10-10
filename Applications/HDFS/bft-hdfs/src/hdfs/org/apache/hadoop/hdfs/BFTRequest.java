package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BftClientWrapper.Invocation;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

public class BFTRequest implements Writable, Configurable {
	//public static final Log LOG = LogFactory.getLog(BFTRequest.class);

	public static enum ProtocolType { CLIENT, DATANODE, NAMENODE }

	private static long lastID = -1;

	private long requestID;
	private ProtocolType protocolType;
	private Invocation invocation;
	private UserGroupInformation ticket;
	//execution replicas send their replies back to this addr
	private String returnAddr;
	private boolean format;

	private Configuration conf;

	public BFTRequest(){}

	public BFTRequest(ProtocolType type){
		protocolType = type;
		format = false;
	}

	public BFTRequest(Invocation inv, ProtocolType type, UserGroupInformation ugi, String localSockAddr){
		setRequestID(++lastID);      
		invocation = inv;
		setTicket(ugi);
		setProtocolType(type);

		returnAddr = localSockAddr;
		format = false;
	}

	public void readFields(DataInput in) throws IOException {
		requestID = in.readLong();

		int type = in.readByte();
		if(type==0){
			setProtocolType(ProtocolType.CLIENT);
		}else if(type==1){
			setProtocolType(ProtocolType.DATANODE);
		}else{
			throw new IOException("unknown client wrapper type");
		}

		format = in.readBoolean();
		if(!format){
			Writable param = (Writable)ReflectionUtils.newInstance(Invocation.class, conf);
			param.readFields(in);
			invocation = (Invocation)param;
		}else{
			invocation = null;
		}
		ticket = (UserGroupInformation) ObjectWritable.readObject(in, conf);
		returnAddr = in.readUTF();


	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(requestID);

		// write a protocol type
		if(getProtocolType() == ProtocolType.CLIENT){
			out.writeByte(0);
		} else if(getProtocolType() == ProtocolType.DATANODE){
			out.writeByte(1);
		} else {
			throw new IOException();
		}
		out.writeBoolean(format);
		if(!format){
			// write the invocation
			invocation.write(out);
		}
		// write the ticket
		ObjectWritable.writeObject(out, ticket, UserGroupInformation.class, conf);
		out.writeUTF(returnAddr);

	}

	public void setRequestID(long requestID) {
		this.requestID = requestID;
	}

	public long getRequestID() {
		return requestID;
	}

	public Invocation getInvocation(){
		return invocation;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

	public void setProtocolType(ProtocolType protocolType) {
		this.protocolType = protocolType;
	}

	public ProtocolType getProtocolType() {
		return protocolType;
	}

	public void setTicket(UserGroupInformation ticket) {
		this.ticket = ticket;
	}

	public UserGroupInformation getTicket() {
		return ticket;
	}

	public String getReturnAddr(){
		return returnAddr;
	}
	
	public void setFormatFlag(){
		this.format = true;
	}
	
	public boolean getFormatFlag(){
		return this.format;
	}

}
