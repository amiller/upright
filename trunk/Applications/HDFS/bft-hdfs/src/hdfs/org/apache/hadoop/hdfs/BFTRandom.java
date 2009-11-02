package org.apache.hadoop.hdfs;

import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

public class BFTRandom {
	static boolean bft;
	static long bftTime;
	// this is due to the case where some requests are executed with
	// same bftTime.. in this case two "allocating new block" with same
	// bfttime will have same block id without this hack..
	static long extra;  
	
	static LinkedList<Random> randoms = new LinkedList<Random>();
	
	static {
		Configuration conf = new Configuration();
		
		bft = conf.getBoolean("dfs.bft", false);
		bftTime = conf.getLong("dfs.bft.format.initialTimeValue", 0);
		if(bft){
		//	random = new Random(bftTime);
		}
		extra = 0;
	}
	
	public synchronized static Random getRandom(){
		if(bft){
			Random r = new Random(bftTime);
			randoms.addLast(r);
			return r;
		} else {
			return new Random();
		}
	}
	
	public synchronized static long now(){
		if(bft){
			return bftTime;
		} else {
			return System.currentTimeMillis();
		}
	}
	
	public synchronized static void setBftTime(long newTime){
		bftTime = newTime;
		extra++;
		for(Random r : randoms){
			r.setSeed(bftTime + extra);
		}
	}


}
