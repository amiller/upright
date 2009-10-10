// $Id$

package BFT.messages;

import java.security.*;
import java.math.*;

public class Digest{

    

    protected Digest(){bytes = new byte[16];}


    int count = 0;
    public Digest(byte[] bits){
	this (bits, 0, bits.length);
    }
    
    public Digest(byte[] bits, int offset, int length){
	try{
	    MessageDigest m=MessageDigest.getInstance("MD5");
	    m.update(bits,offset,length);
	    bytes = m.digest(); 
	    count++;
	    if (count %100 == 0)
		m.reset();
	}catch(Exception e){ throw new RuntimeException(e);}	
    }


    public static Digest fromBytes(byte[] bits){
	Digest d = new Digest();
	d.bytes = new byte[bits.length];
	for (int i = 0; i < bits.length; i++)
	    d.bytes[i] = bits[i];
	return d;
    }
    
    protected byte[] bytes;
    
    public byte[] getBytes(){
	return bytes;
    }

    public String toString(){
	String str = "";
	for (int i = 0; i < bytes.length; i++)
	    str += bytes[i]+" ";

	return str;
    }

    public boolean equals(Digest d){
	boolean res = true && d != null;
	for (int i = 0; i < bytes.length; i++){
	    res = res && bytes[i] == d.bytes[i];
	}
	return res;
    }

    public int getSize(){ return 16;}

    public static int size(){return 16;} // size in bytes;

}
