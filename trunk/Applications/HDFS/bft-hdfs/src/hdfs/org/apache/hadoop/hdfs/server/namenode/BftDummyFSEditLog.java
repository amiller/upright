package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.io.Writable;

//
// Use this dummy edit log when generalCP handles logging

public class BftDummyFSEditLog extends BftFSEditLog {

	BftDummyFSEditLog(FSImage image) {
		super(image);
	}

	
  synchronized void logEdit(byte op, Writable ... writables) {

  }
  
}
