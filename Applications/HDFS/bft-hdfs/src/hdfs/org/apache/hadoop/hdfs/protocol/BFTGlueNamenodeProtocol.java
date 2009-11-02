package org.apache.hadoop.hdfs.protocol;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.ipc.VersionedProtocol;

/*
 * Protocol used for an UpRight execution server glue to communicate 
 * with its associated namenode
 */
public interface BFTGlueNamenodeProtocol extends VersionedProtocol {

	public static final long versionID = 1L;

	public void executeThreadFunctions();
	
	public void setBftTime(long time);
	
	public void reloadImage() throws IOException;
	
	public File getImageName() throws IOException;
	
	public FSImage getFSImage();
	
}
