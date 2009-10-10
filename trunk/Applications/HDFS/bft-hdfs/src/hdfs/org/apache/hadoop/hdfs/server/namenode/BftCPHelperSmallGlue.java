package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.CPHelperInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import BFT.generalcp.BackupAppCPInterface;
import BFT.generalcp.CPAppInterface;

public class BftCPHelperSmallGlue implements BackupAppCPInterface {

	CPHelperInterface cpHelper;
	CPAppInterface generalCP;
	
	public BftCPHelperSmallGlue(){
		
	}
	
	public void initialize(Configuration conf, String generalCPDir,
			CPAppInterface generalCP) throws IOException {
		// get rpc client		
		String helperRPCServerAddr =       	
			conf.get("dfs.bft.NameNodeCPHelper.ip","localhost") + ":"
			+ conf.getInt("dfs.bft.NameNodeCPHelper.rpcport", 5000);
		
		InetSocketAddress cpHelperAddr = 
			NetUtils.createSocketAddr(helperRPCServerAddr);

		if(cpHelper == null){
			try{
				cpHelper = (CPHelperInterface)RPC.waitForProxy(CPHelperInterface.class,
						CPHelperInterface.versionID,
						cpHelperAddr, conf);
			} catch(IOException e){
				cpHelper = null;
				//e.printStackTrace();
				throw e;
			}
		}
		
		cpHelper.setCPDir(generalCPDir);
		this.generalCP = generalCP;
	}
	
	@Override
	public void consumeLog(String fileName) {
		cpHelper.consumeLog(fileName);
		generalCP.consumeLogDone(fileName);
	}

	@Override
	public void loadSnapshot(String fileName) {
		cpHelper.loadSnapshot(fileName);
		generalCP.loadSnapshotDone();		
	}

	@Override
	public void sync() {
		
		generalCP.syncDone(cpHelper.sync());
		
	}

}
