package Applications.hashtable;
import java.io.*;

public class HTReply implements Serializable{
	
	private boolean error = false;
	private DataUnit data;
	public HTReply(boolean error, DataUnit data){
		this.error = error;
		this.data = data;
	}
	
	public boolean isError(){ return error;}
	public DataUnit getData() { return data;}
	public HTReply(){}
	
}
