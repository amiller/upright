//$Id: BftGlueReply.java 2325 2009-01-24 08:30:02Z sangmin $

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
import org.apache.hadoop.ipc.RemoteException;

public class BftGlueReply implements Writable, Configurable{

	private Configuration conf;
	
	private ObjectWritable returnValue;
	private boolean isError;
	private String strErrorClass;
	private String strErrorMsg;
	
	public BftGlueReply(){}
	
	// used by getReply()
	private BftGlueReply(Configuration _conf){
		conf = _conf;
	}
	
	public static BftGlueReply 
	getReplyFromBytes(byte[] bytesForm, Configuration conf)
	throws IOException {
		BftGlueReply rep = new BftGlueReply(conf);
		
		ByteArrayInputStream bais = new ByteArrayInputStream(bytesForm);
		DataInputStream dis = new DataInputStream(bais);
		rep.readFields(dis);

		return rep;
	}
	
	public BftGlueReply(ObjectWritable retval, Configuration _conf) {
		returnValue = retval;
		isError = false;
		this.conf = _conf;
	}

	public BftGlueReply(String errorClass, 
			String errorMsg, Configuration _conf) {
		isError = true;
		strErrorClass = errorClass;
		strErrorMsg = errorMsg;
		this.conf = _conf;
	}
	
	public ObjectWritable getReturnValue() throws RemoteException {
    if(isError){    	
      throw new RemoteException(strErrorClass, strErrorMsg);
    }
		return (ObjectWritable)returnValue;
	}
	
	// serialize to byte array
	public byte[] toBytes() {	
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);
		
		try {
			this.write(dataOutput);
		} catch (IOException e) {
			return null;
		}
		
		return baos.toByteArray();
	}
	
	//
	// Writable
	//

	@Override
	public void readFields(DataInput in) throws IOException {
	    isError = in.readBoolean();
	    if(isError){
	      strErrorClass = in.readUTF();
	      strErrorMsg = in.readUTF();
	      returnValue = null;
	    }else{
	      ObjectWritable ow = new ObjectWritable();
	      ow.setConf(conf);
	      returnValue = ow;
	      returnValue.readFields(in);
	      strErrorClass = null;
	      strErrorMsg = null;
	    }
	}

	@Override
	public void write(DataOutput out) throws IOException {
	    out.writeBoolean(isError);
	    if(isError){
	      out.writeUTF(strErrorClass);
	      out.writeUTF(strErrorMsg);
	    }else{
	      returnValue.write(out);
	    }
		
	}

	//
	// Configurable
	//

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration _conf) {
		this.conf = _conf;
	}

}
