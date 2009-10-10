//$Id$

package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class BftCheckpointSignature //extends CheckpointSignature 
						implements Writable{//Comparable<CheckpointSignature> {
	
	public static long version = 0L;
	
	public static BftCheckpointSignature 
	getCheckpointSignatureFromBytes(byte[] bytesForm) throws IOException{
		BftCheckpointSignature ret = new BftCheckpointSignature();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(bytesForm);
		DataInputStream dis = new DataInputStream(bais);
		ret.readFields(dis);
		
		return ret;
	}
	
	private byte [] digest;	// digest of the checkpoint
	
	public BftCheckpointSignature() {}
	
	public BftCheckpointSignature(CheckpointSignature sig, byte[] hash) {
		//super(sig.toString());
		this.digest = hash;
	}
	
  public int hashCode() {
  	//int ret = super.hashCode();
  	int ret = (int)version;
  	for(byte b : digest){
  		ret += b;
  	}
  	return ret;
  }
  
  public boolean equals(Object o){
  	if(!(o instanceof BftCheckpointSignature)){
  		return false;
  	}
  	BftCheckpointSignature b = (BftCheckpointSignature)o;
  	
  	return Arrays.equals(this.digest, b.digest);
  	
  }
  
  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
  	out.writeLong(version);
  	System.out.println("version:"+version);
  	//super.write(out);
  	//System.out.println(super.toString());
  	out.writeInt(digest.length);
  	System.out.println("length:"+digest.length);
  	out.write(digest);
  	System.out.println("digest:" + new String(digest) );
  	
  }

  public void readFields(DataInput in) throws IOException {
	long ver = in.readLong();
	if(ver != version){
		throw new IOException("unmatching version");
	}
  	//super.readFields(in);
  	int len = in.readInt();
  	digest = new byte[len];
  	in.readFully(digest, 0, len);
  }

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

}
