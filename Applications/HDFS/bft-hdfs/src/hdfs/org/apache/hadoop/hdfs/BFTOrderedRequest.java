package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BftClientWrapper.Invocation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class BFTOrderedRequest implements Writable, Configurable{

	private long sequenceNumber;
	public boolean isVoid; // true if it has no request. used for setting time or executing thread functions
	BFTRequest request;
	public long	time;
	public boolean runThreadFunctions;

	private Configuration conf;

	public BFTOrderedRequest(){}

	public BFTOrderedRequest(BFTRequest bftReq){
		sequenceNumber = -1;
		if(bftReq == null){
			isVoid = true;
		}else{
			request = bftReq;
			isVoid = false;
		}
		time = System.currentTimeMillis();
		runThreadFunctions = false;
	}
	
	public long getSequenceNumber(){
		return this.sequenceNumber;
	}

	public void setSequenceNumber(long n){
		this.sequenceNumber = n;
	}

	public BFTRequest getRequest(){
		return request;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sequenceNumber = in.readLong();

		isVoid = in.readBoolean();
		if(!isVoid){
			Writable param = (Writable)ReflectionUtils.newInstance(BFTRequest.class, conf);
			param.readFields(in);
			request = (BFTRequest)param;
		}else{
			request = null;
		}
		time = in.readLong();
		runThreadFunctions = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(sequenceNumber);
		out.writeBoolean(isVoid);
		if(!isVoid){
			request.write(out);
		}
		out.writeLong(time);
		out.writeBoolean(runThreadFunctions);
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}
	
	public String toString(){
		String ret = "";
		
		ret += "\nSequence Number: " + this.sequenceNumber;
		ret += "\nisVoid: " + this.isVoid;
		ret += "\ntime: " + this.time;
		ret += "\nrunThreadFunction: " + this.runThreadFunctions;
		
		return ret;
	}

}
