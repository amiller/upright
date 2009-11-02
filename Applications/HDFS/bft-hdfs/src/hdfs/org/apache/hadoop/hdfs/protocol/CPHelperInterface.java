//package org.apache.hadoop.hdfs.protocol;
//
//import org.apache.hadoop.ipc.VersionedProtocol;
//
//import BFT.generalcp.BackupAppCPInterface;
//
//public interface CPHelperInterface extends VersionedProtocol{ 
//	
//	public static final long versionID = 0L;
//	
//	public void consumeLog(String fileName);
//	public void loadSnapshot(String fileName);
//	public String sync();
//
//	void setCPDir(String generalCPDir);
//}
