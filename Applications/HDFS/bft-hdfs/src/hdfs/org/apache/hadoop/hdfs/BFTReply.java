package org.apache.hadoop.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;


public class BFTReply implements Writable, Configurable{
	private static MessageDigest md;
	
	static {
		try {
			md = MessageDigest.getInstance("SHA");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
  private long requestID;
  private String replicaID;
  private boolean error;
  private Writable retVal;
  private Configuration conf;
  private String errorClass;
  private String errorStr;

  public BFTReply(){}

  public BFTReply(long id, Writable retval){
    requestID = id;
    error = false;
    retVal = retval;
  }

  public BFTReply(long id, String errClass, String errStr) {
    requestID = id;
    replicaID = null;
    error = true;
    retVal = null;
    errorClass = errClass;
    errorStr = errStr;
  }
  
  public void setReplicaID(String replica){
  	replicaID = replica;
  }
  
  public String getReplicaID(){
  	return replicaID;
  }

  public long getReqID(){
    return requestID;
  }

  public Writable getRetVal(){
    return retVal;
  }

  public void readFields(DataInput in) throws IOException {
    requestID = in.readLong();
    replicaID = in.readUTF();
    error = in.readBoolean();
    if(error){
      errorClass = in.readUTF();
      errorStr = in.readUTF();
    }else{
      ObjectWritable ow = new ObjectWritable();
      ow.setConf(conf);
      retVal = ow;
      retVal.readFields(in);
    }

  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(requestID);
    out.writeUTF(replicaID);
    out.writeBoolean(error);
    if(error){
      out.writeUTF(errorClass);
      out.writeUTF(errorStr);
    }else{
      retVal.write(out);
    }
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  public boolean isError(String[] strs){
    if(error){
      strs[0] = errorClass;
      strs[1] = errorStr;
    }
    return error;
  }
  

  public int hashCode() {
  	int ret = (int)requestID;

		if(error){
			md.update(errorClass.getBytes());
			md.update(errorStr.getBytes());
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);
  	try {
			retVal.write(dataOutput);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] hash = md.digest(baos.toByteArray());
		for(byte b : hash){
			ret += b;
		}
		
		return ret;

  }

  public boolean equals(Object obj) {
  	if (obj instanceof BFTReply) {
			BFTReply newObj = (BFTReply) obj;
			return (this.hashCode() == newObj.hashCode());
		} else {
			return false;
		}

  }

}
