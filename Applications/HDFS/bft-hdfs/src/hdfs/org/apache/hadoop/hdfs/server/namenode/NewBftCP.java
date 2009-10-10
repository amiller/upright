package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;
import org.mortbay.util.Credential.MD5;

public class NewBftCP implements Writable{
	String basefile;
	byte[] digest;
	LinkedList<String> logs;
	public static NewBftCP 
	getCheckpointSignatureFromBytes(byte[] bytesForm) throws IOException{
		NewBftCP ret = new NewBftCP();

		ByteArrayInputStream bais = new ByteArrayInputStream(bytesForm);
		DataInputStream dis = new DataInputStream(bais);
		ret.readFields(dis);

		return ret;
	}

	public NewBftCP(String base,byte [] di, LinkedList<String> logs){
		this.basefile = base;
		this.digest = di;
		this.logs = logs;
	}

	public NewBftCP(NewBftCP previousCP, String newLog){
		this.basefile = previousCP.basefile;
		this.digest = previousCP.digest;
		logs = new LinkedList<String>();
		logs.addAll(previousCP.logs);
		logs.add(newLog);

	}

	public NewBftCP() {
		// TODO Auto-generated constructor stub
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		basefile = in.readUTF();
		int len = in.readInt();
		digest = new byte[len];
		in.readFully(digest, 0, len);
		logs = new LinkedList<String>();
		len = in.readInt();
		for ( int i =0; i < len; i++){
			logs.add(in.readUTF());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(basefile);
		out.writeInt(digest.length);
		out.write(digest);
		out.writeInt(logs.size());
		for(String s : logs){
			out.writeUTF(s);
		}

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
	
	public int hashCode(){
  	int ret = 0;
  	for(byte b : digest){
  		ret += b;
  	}
  	ret += basefile.hashCode();
  	for(String s : logs){
  		ret += s.hashCode();
  	}
  	return ret;
	}
	
	public boolean equals(Object o){
		if(o instanceof NewBftCP){
			NewBftCP cp = (NewBftCP)o;
			
			if( cp.basefile.equals(basefile) 
					&& cp.logs.size() == logs.size()
					&& Arrays.equals(digest, cp.digest)){
				return true;
			}
			
			
		}
		return false;
	}
	
}