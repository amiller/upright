//$Id: BftGlueRequest.java 2699 2009-02-24 10:57:08Z sangmin $

package org.apache.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

public class BftGlueRequest implements Writable, Configurable{
	
	public static enum NodeType { CLIENT, DATANODE, NAMENODE }
	
	private Configuration conf;
	
	// fields that are sent over network
	private BftGlueInvocation invocation;
	private NodeType nodeType;
	private UserGroupInformation ticket;

	public BftGlueRequest(){}
	
	private BftGlueRequest(Configuration _conf) {
		conf = _conf;
	}
	
	public static BftGlueRequest 
	getRequestFromBytes(byte[] bytesForm, Configuration conf) 
	throws IOException {
		BftGlueRequest req = new BftGlueRequest(conf);
		
		ByteArrayInputStream bais = new ByteArrayInputStream(bytesForm);
		DataInputStream dis = new DataInputStream(bais);
		req.readFields(dis);

		return req;
	}

	public BftGlueRequest(BftGlueInvocation _invocation, NodeType _nodeType,
			UserGroupInformation _ticket, Configuration _conf) {
		invocation = _invocation;
		nodeType = _nodeType;
		ticket = _ticket;
		conf = _conf;
	}


	private void setNodeType(NodeType type) {
		this.nodeType = type;	
	}

	public NodeType getNodeType() {
		return this.nodeType;
	}

	public BftGlueInvocation getInvocation() {
		return this.invocation;
	}
	
	public void setTicket(UserGroupInformation ticket) {
		this.ticket = ticket;
	}

	public UserGroupInformation getTicket() {
		return ticket;
	}
	
	//serialize to byte array
	public byte[] toBytes() {	
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);
		
		try {
			this.write(dataOutput);
		} catch (IOException e) {
			System.err.println("!!!!" + e.getMessage());
			System.exit(-1);
			return null;
		}
		
		return baos.toByteArray();
	}
	
	//
	// Writable
	//
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int type = in.readByte();
		if(type==0){
			nodeType = NodeType.CLIENT;
		}else if(type==1){
			nodeType = NodeType.DATANODE;
		}else{
			throw new IOException("unknown node type");
		}
		
		Writable param = (
				Writable)ReflectionUtils.newInstance(BftGlueInvocation.class, conf);
		param.readFields(in);
		invocation = (BftGlueInvocation)param;
		
		ticket = (UserGroupInformation) ObjectWritable.readObject(in, conf);
	}

	//
	// Configurable
	//
	
	@Override
	public void write(DataOutput out) throws IOException {
		// write the node type
		if(this.nodeType == NodeType.CLIENT){
			out.writeByte(0);
		} else if(this.nodeType == NodeType.DATANODE){
			out.writeByte(1);
		} else {
			throw new IOException("inappropriate node type");
		}

		// write the invocation
		invocation.write(out);

		// write the ticket
		ObjectWritable.writeObject(out,
				ticket, UserGroupInformation.class, conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration _conf) {
		this.conf = _conf;
	}

}

