package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.CPHelperInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdfs.server.namenode.BftNameNodeSmallGlue.LogConsumer;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode.CheckpointStorage;

public class BftNamenodeCPHelper implements CPHelperInterface{
	public static final Log LOG = LogFactory.getLog(BftNamenodeCPHelper.class.getName());
	private Server server;
	protected FSImage checkpointImage;	
	private LogConsumer logConsumer;
	private long lastSyncSeqNum;	
	private String generalCPDir;
	private FSNamesystem namesystem;	
	Configuration conf;
	
	public BftNamenodeCPHelper(Configuration conf) throws IOException {
		if(!conf.getBoolean("dfs.bft", false)){
			System.err.println("Namenode Checkpoint Helper cannot run in Non-BFT mode");
			System.exit(-1);
		}
		initialize(conf);
	}
	
	public void initialize(Configuration conf) throws IOException{		
		String helperRPCServerAddr =       	
			conf.get("dfs.bft.NameNodeCPHelper.ip","localhost") + ":"
			+ conf.getInt("dfs.bft.NameNodeCPHelper.rpcport", 5000);
		InetSocketAddress socAddr = 
			NetUtils.createSocketAddr(helperRPCServerAddr);
    try {
			this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
			    1, false, conf);
			this.server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] chkpointDirs_org = conf.getStrings("dfs.name.dir");
		String[] chkpointDirs = new String[chkpointDirs_org.length];
		for(int i=0; i < chkpointDirs_org.length; i++){
			chkpointDirs[i] = chkpointDirs_org[i] + "/cphelper/";
		}				
		String[] chkpointEditsDirs_org = conf.getStrings("dfs.name.edits.dir");
		String[] chkpointEditsDirs = new String[chkpointEditsDirs_org.length];
		for(int i=0; i < chkpointEditsDirs.length; i++){
			chkpointEditsDirs[i] = chkpointEditsDirs_org[i] + "/cphelper/";
		}
		conf.setStrings("dfs.name.dir", chkpointDirs);
		conf.setStrings("dfs.name.edits.dir", chkpointEditsDirs);
		
		this.conf = conf;
		
    try {
			namesystem = 
			  new FSNamesystem(conf, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		checkpointImage = namesystem.getFSImage();		
		logConsumer = new LogConsumer(this.checkpointImage);
		lastSyncSeqNum = 0;
		logConsumer.start();
	}
	
	public long getProtocolVersion(String protocol, 
      long clientVersion) throws IOException { 
		return CPHelperInterface.versionID;
	}
	
	@Override
	public void consumeLog(String fileName) {
		LOG.debug("ConsumeLog with log filename : " + fileName);

		int idx = fileName.lastIndexOf("_");
		long seqNum = Long.parseLong(fileName.substring(idx+1));
		lastSyncSeqNum = seqNum;
		logConsumer.addLogConsume(fileName);
		logConsumer.waitForEmptyQueue();		
	}

	@Override
	public void loadSnapshot(String fileName) {
		LOG.debug("#######\n########\n####### loadSnapshot : " + fileName);
		try {
			int idx = fileName.lastIndexOf("_");
			long seqNum = Long.parseLong(fileName.substring(idx+1));
			lastSyncSeqNum = seqNum;
			
			BftNameNodeSmallGlue.copyfile(fileName, checkpointImage.getFsImageName().getAbsolutePath());
			namesystem.shutdown();
			checkpointImage.close();
			namesystem = new FSNamesystem(conf, false);
			checkpointImage = namesystem.getFSImage();
			logConsumer.shutdown();
			logConsumer = new LogConsumer(this.checkpointImage);
			logConsumer.start();
			
			//checkpointImage.loadFSImage(new File(fileName));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private String getSnapshotFileName(long syncSeqNum){
		return generalCPDir + File.separator + "snapshots" + File.separator + "Snapshot_" + syncSeqNum;
	}
	@Override
	public String sync() {
		LOG.debug("sync");
		logConsumer.addSync(getSnapshotFileName(lastSyncSeqNum));
		return logConsumer.waitForSyncDone();
	}

	private void join(){
    try {
      this.server.join();
    } catch (InterruptedException ie) {
    }
	}
	
	public static void main(String[] args) throws IOException{		
		BftNamenodeCPHelper bftSNN = new BftNamenodeCPHelper(new Configuration());
		bftSNN.join();
		
	}

	@Override
	public void setCPDir(String generalCPDir) {
		this.generalCPDir = generalCPDir;
		File cpdir = new File(generalCPDir);
		if(!cpdir.exists()){
			cpdir.mkdirs();
		}
	}

}
