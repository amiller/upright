package Applications.hashtable;
import java.io.*;
public class DataUnit implements Serializable{
                private int value;
                private long rand;
                private long timestamp;
                public DataUnit(){}
                public DataUnit(int value, long rand, long timestamp){
                        this.value=value;
                        this.rand=rand;
                        this.timestamp=timestamp;
                }
		public int getValue() { return value;}
		public long getRandom() { return rand;}
		public long getTimestamp() { return timestamp;} 

}

