package org.apache.hadoop.hdfs.protocol;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface BFTWrapperNamenodeProtocol extends VersionedProtocol {

	public static final long versionID = 1L;
	
	
	public void executeThreadFunctions();
	
	public void setBftTime(long time);
	
	public void reloadImage() throws IOException;
	
	public void shutdownFS();
	
	public File getImageName() throws IOException;
	public ArrayList<EditLogOutputStream> swapEditLog() throws IOException;
	//public File getEditLogName() throws IOException;
	
	public FSImage getFSImage();
	
}
