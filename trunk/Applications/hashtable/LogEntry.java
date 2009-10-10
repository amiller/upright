package Applications.hashtable;
import java.io.*;
public class LogEntry implements Serializable{
	private String key;
        private DataUnit data;
        public LogEntry(){};
        public LogEntry(String key, DataUnit data){
        	this.key=key;
                this.data=data;
        }
	public String getKey() { return key;}
	public DataUnit getData() { return data;}
}

