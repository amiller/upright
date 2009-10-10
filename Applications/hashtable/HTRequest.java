package Applications.hashtable;

import java.io.*;

public class HTRequest implements Serializable{
	public static final byte READ=0;
	public static final byte WRITE=1;
	private byte type; 
	private String key;
	private int value;

	HTRequest(byte type, String key,int value){
		this.type = type;
		this.key = key;
		this.value = value;
	}

	public byte getType() {return type;}
	public String getKey() {return key;}
	public int getValue() {return value;}

	public HTRequest(){}

}
