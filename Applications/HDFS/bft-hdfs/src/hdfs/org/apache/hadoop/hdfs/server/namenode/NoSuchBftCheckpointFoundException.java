package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

public class NoSuchBftCheckpointFoundException extends IOException {

	public NoSuchBftCheckpointFoundException(String msg) {
		super(msg);
	}

}
