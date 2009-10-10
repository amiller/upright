package org.apache.hadoop.hdfs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.MD5Hash;

public class BFTMessageDigest {
	
	private static byte[] b;
	
	static {
		b = new byte[MD5Hash.MD5_LEN];
		for(int i=0; i < b.length; i++){
			b[i] = '0';
		}
	}
	
	private boolean fakeMD5;
	
	MessageDigest md5;
	
	public BFTMessageDigest(boolean fake) throws NoSuchAlgorithmException{
		fakeMD5 = fake;

			md5 = MessageDigest.getInstance("MD5");

	}
	
	public void update(byte[] b, int offset, int len){
		if(fakeMD5){
			return;
		}
		md5.update(b, offset, len);
	}
	
	public void update(byte[] b){
		if(fakeMD5){
			return;
		}
		md5.update(b);
	}
	
	public byte[] digest(){
		if(fakeMD5){
			return b;
		}
		return md5.digest();
	}

}
