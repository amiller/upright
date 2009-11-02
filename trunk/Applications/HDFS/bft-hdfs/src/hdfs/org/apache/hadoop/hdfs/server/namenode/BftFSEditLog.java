package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.BFTRandom;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;

public class BftFSEditLog extends FSEditLog {

  private static final byte BFT_OP_START_FILE = 50;
  private static final byte BFT_OP_APPEND = 51;
  private static final byte BFT_OP_SET_REPLICATION = 52;
  private static final byte BFT_OP_SET_PERMISSION = 53;
  private static final byte BFT_OP_SET_OWNER = 54;
  private static final byte BFT_OP_ADD_BLOCK = 55;
  private static final byte BFT_OP_ABANDON_BLOCK = 56;
  private static final byte BFT_OP_COMPLETE = 57;
  private static final byte BFT_OP_REPORT_BAD_BLOCKS = 58;
  private static final byte BFT_OP_NEXT_GENERATION_TIME = 59;
  private static final byte BFT_OP_COMMIT_BLOCK_SYNCHRONIZATION = 60;
  private static final byte BFT_OP_RENAME = 61;
  private static final byte BFT_OP_DELETE = 62;
  private static final byte BFT_OP_MKDIR = 63;
  private static final byte BFT_OP_RENEW_LEASE = 64;
  private static final byte BFT_OP_SET_SAFEMODE = 65;
  private static final byte BFT_OP_REFRESH_NODES = 66;
  private static final byte BFT_OP_SET_QUOTA = 67;
  private static final byte BFT_OP_SET_TIMES = 68;
  private static final byte BFT_OP_REGISTER = 69;
  private static final byte BFT_OP_SEND_HEARTBEAT = 70;
  private static final byte BFT_OP_BLOCK_REPORT = 71;
  private static final byte BFT_OP_BLOCK_RECEIVED = 72;
  private static final byte BFT_OP_REMOVE_DATANODE = 73;
  private static final byte BFT_OP_GET_BLOCKLOCATIONS = 74;
  
  private static final byte BFT_OP_BLOCK_REPORT_BFTDN = 80;
  private static final byte BFT_OP_ADD_BLOCK_BFTDN = 81;
  private static final byte BFT_OP_COMPLETE_BFTDN = 82;
  
  private static final byte BFT_OP_HB_MON = 90;
  private static final byte BFT_OP_DECOMM_MON = 91;
  private static final byte BFT_OP_REP_MON = 92;
  private static final byte BFT_OP_LEASE_MON = 93;
  private static final byte BFT_OP_PENDING_REP_MON = 94;
  
  private long lastEditTime = 0;

  BftFSEditLog(FSImage image) {
    super(image);
    // TODO Auto-generated constructor stub
  }

  static int loadFSEdits(EditLogInputStream edits) throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    Configuration conf = new Configuration();
    fsNamesys.bftReplaying = true;
    boolean tmpIsPermissionEnabled = fsNamesys.isPermissionEnabled;
    fsNamesys.isPermissionEnabled = false;
    
    int numEdits = 0;

    int logVersion = 0;
    
    DataInputStream in = new DataInputStream(new BufferedInputStream(edits));

    try{
      
      // Read log file version. Could be missing. 
      in.mark(4);
      // If edits log is greater than 2G, available method will return negative
      // numbers, so we avoid having to call available
      boolean available = true;

      try {
        logVersion = in.readByte();
      } catch (EOFException e) {
        available = false;
      }
      if (available) {
        in.reset();
        logVersion = in.readInt();
        if (logVersion < FSConstants.LAYOUT_VERSION) // future version
          throw new IOException(
                          "Unexpected version of the file system log file: "
                          + logVersion + ". Current version = " 
                          + FSConstants.LAYOUT_VERSION + ".");
      }
      assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
                            "Unsupported version " + logVersion;

      
      //
      // XXX: we may want to add some version information here
      //

      while (true) {
	long timestamp = 0;
	long mtime = 0;
	long atime = 0;
	long blockSize = 0;
	byte opcode = -1;
	try {
	  opcode = in.readByte();
	  if (opcode == OP_INVALID) {
	    FSNamesystem.LOG.info("Invalid opcode, reached end of edit log " +
		"Number of transactions found " + numEdits);
	    break; // no more transactions
	  }
	} catch (EOFException e) {
	  break; // no more transactions
	}
	
	FSImage.LOG.info("OPCODE : " + opcode );
	//System.out.println("OPCODE: " + opcode);
	
	numEdits++;
	switch (opcode) {

	case BFT_OP_START_FILE:{
		UserGroupInformation.setCurrentUGI((UserGroupInformation) ObjectWritable.readObject(in, conf));
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  String src = FSImage.readString(in);
	  PermissionStatus ps = PermissionStatus.read(in);
	  String holder = FSImage.readString(in);
	  String clientMachine = FSImage.readString(in);
	  boolean overwrite = readLong(in) > 0 ? true : false;
	  short replication = readShort(in);
	  blockSize = readLong(in);
	  fsNamesys.startFile(src, ps, holder, clientMachine, overwrite, replication, blockSize);
	  break;
	} 

	case BFT_OP_APPEND:{
		UserGroupInformation.setCurrentUGI((UserGroupInformation) ObjectWritable.readObject(in, conf));
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  fsNamesys.appendFile(FSImage.readString(in), FSImage.readString(in), FSImage.readString(in));
	  break;
	}

	case BFT_OP_SET_REPLICATION:{
		UserGroupInformation.setCurrentUGI((UserGroupInformation) ObjectWritable.readObject(in, conf));
	  fsNamesys.setReplication(FSImage.readString(in), readShort(in));
	  break;
	}

	case BFT_OP_SET_PERMISSION:{		
		UserGroupInformation.setCurrentUGI((UserGroupInformation) ObjectWritable.readObject(in, conf));
		String src = FSImage.readString(in);
		FsPermission p = new FsPermission(readShort(in));		
	  fsNamesys.setPermission(src, p);
	  break;
	}

	case BFT_OP_SET_OWNER:{
		UserGroupInformation.setCurrentUGI((UserGroupInformation) ObjectWritable.readObject(in, conf));
	  fsNamesys.setOwner(FSImage.readString(in), FSImage.readString(in), FSImage.readString(in));
	  break;
	}

	case BFT_OP_ADD_BLOCK:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		
	  String src = FSImage.readString(in);
	  String clientName = FSImage.readString(in);
	  Block b = new Block();
	  b.readFields(in);
	  //System.out.println("### Adding block : " +b.getBlockId());
	  
	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] sids = a.toStrings();
	  try{
	  	fsNamesys.bftGetAddtionalBlock(src,clientName, b, sids);
	  }catch(NotReplicatedYetException e){
	  	System.err.println(e);
	  }
	  break;
	}
	
	case BFT_OP_ADD_BLOCK_BFTDN:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		
	  String src = FSImage.readString(in);
	  String clientName = FSImage.readString(in);
	  Block b = new Block();
	  b.readFields(in);
	  //System.out.println("### Adding block BFTDN: " +b.getBlockId());
	  
	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] sids = a.toStrings();

	  //String hash = FSImage.readString(in);
	  
	  MD5Hash md5 = MD5Hash.read(in);
	  
	  fsNamesys.setHashOfBlock(src,clientName, md5.getDigest());
	  try{
	  	fsNamesys.bftGetAddtionalBlock(src,clientName, b, sids);
	  }catch(NotReplicatedYetException e){
	  	System.err.println(e);
	  }
	  break;
	}

	case BFT_OP_ABANDON_BLOCK:{
	  Block b = new Block();
	  b.readFields(in);
	  fsNamesys.abandonBlock(b, FSImage.readString(in), FSImage.readString(in));
	  break;
	}

	case BFT_OP_COMPLETE: {
	  fsNamesys.completeFile(FSImage.readString(in), FSImage.readString(in));
	  break;
	}
	case BFT_OP_COMPLETE_BFTDN: {
		String src = FSImage.readString(in);
		String clientName = FSImage.readString(in);
		MD5Hash md5 = MD5Hash.read(in);
		//String hash = FSImage.readString(in);
		fsNamesys.setHashOfBlock(src,clientName, md5.getDigest());
	  fsNamesys.completeFile(src, clientName);
	  break;
	}

	case BFT_OP_REPORT_BAD_BLOCKS:{

	  ArrayWritable a = new ArrayWritable(LocatedBlock.class);
	  a.readFields(in);
	  LocatedBlock[] blocks = (LocatedBlock[]) a.toArray();
	  for(int i=0; i < blocks.length; i++){
	    Block blk = blocks[i].getBlock();
	    DatanodeInfo[] nodes = blocks[i].getLocations();
	    for (int j = 0; j < nodes.length; j++) {
	      DatanodeInfo dn = nodes[j];
	      fsNamesys.markBlockAsCorrupt(blk, dn);
	    }
	  }
	  break;
	}

	case BFT_OP_NEXT_GENERATION_TIME:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  Block b = new Block();
	  b.readFields(in);
	  fsNamesys.nextGenerationStampForBlock(b);
	  break;
	}

	case BFT_OP_COMMIT_BLOCK_SYNCHRONIZATION:{
	  Block b = new Block();
	  b.readFields(in);
	  
	  long newGenerationStamp = readLong(in);
	  long newlength = readLong(in);
	  boolean closeFile = readLong(in) > 0 ? true : false;
	  boolean deleteblock = readLong(in) > 0 ? true : false;
	  
	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] sids = a.toStrings();
	  
	  DatanodeDescriptor[] targetDescriptors = new DatanodeDescriptor[sids.length];
    for(int i=0; i < sids.length; i++){
      targetDescriptors[i] = fsNamesys.datanodeMap.get(sids[i]);
    }
	  
	  //ArrayWritable a = new ArrayWritable(DatanodeID.class);
	  //a.readFields(in);
	  //DatanodeID[] newtargets = (DatanodeID[])a.toArray();
	  fsNamesys.commitBlockSynchronization(b, newGenerationStamp, newlength,
	      closeFile, deleteblock, targetDescriptors);
	  break;
	}
	case BFT_OP_RENAME:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  fsNamesys.renameTo(FSImage.readString(in), FSImage.readString(in));
	  break;
	}
	case BFT_OP_DELETE:{
		UserGroupInformation.setCurrentUGI((UserGroupInformation) ObjectWritable.readObject(in, conf));
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  String src = FSImage.readString(in);
	  boolean recursive = readLong(in) > 0 ? true:false;
	  fsNamesys.delete(src, recursive);
	  break;
	}
	case BFT_OP_MKDIR:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		
		String src = FSImage.readString(in);
		PermissionStatus permission = PermissionStatus.read(in);		
	  fsNamesys.mkdirs(src, permission);
	  //System.out.println("Num files : " + fsNamesys.dir.rootDir.numItemsInTree());
	  //System.out.println("root dir : " + System.identityHashCode(fsNamesys.dir.rootDir));
	  //fsNamesys.mkdirs(FSImage.readString(in), PermissionStatus.read(in));
	  break;
	}
	case BFT_OP_RENEW_LEASE:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  fsNamesys.renewLease(FSImage.readString(in));
	  break;
	}
	case BFT_OP_SET_SAFEMODE:{
	  int index = (int) readLong(in);
	  fsNamesys.setSafeMode(SafeModeAction.values()[index]);
	  break;
	}
	case BFT_OP_REFRESH_NODES:{
	  fsNamesys.refreshNodes(null);
	  break;          
	}
	case BFT_OP_SET_QUOTA:{
	  fsNamesys.setQuota(FSImage.readString(in), 
	      readLong(in), readLong(in));
	  break;
	}
	case BFT_OP_SET_TIMES:{
	  fsNamesys.setTimes(FSImage.readString(in),
	      readLong(in), readLong(in));
	  break;
	}
	case BFT_OP_REGISTER:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  fsNamesys.registerDatanode(nodeReg);
	  break;
	}
	case BFT_OP_SEND_HEARTBEAT:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  int len = in.readInt();
	  assert len == 5;
	  long capacity = readLong(in);
	  long dfsUsed = readLong(in);
	  long remaining = readLong(in);
	  int xmitsInProgress = (int) readLong(in);
	  int xceiverCount = (int) readLong(in);	  
	  fsNamesys.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining, xceiverCount, xmitsInProgress);
	  break;          
	}
	case BFT_OP_BLOCK_REPORT:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);

	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  int len = in.readInt();
	  long[] blocks = new long[len];
	  for(int i=0; i < len; i++){
	    blocks[i] = readLong(in);
	  }

	  BlockListAsLongs blist = new BlockListAsLongs(blocks);

	  fsNamesys.processReport(nodeReg, blist);
	  break;                    
	}
	
	case BFT_OP_BLOCK_REPORT_BFTDN:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  ArrayWritable a = new ArrayWritable(Block.class);
	  a.readFields(in);
	  Block[] blocks = (Block[])a.toArray();
	  
	  fsNamesys.processReport(nodeReg, blocks);
	  break;                    
	}
	
	case BFT_OP_BLOCK_RECEIVED:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  ArrayWritable a = new ArrayWritable(Block.class);
	  a.readFields(in);
	  Block[] blocks = (Block[]) a.toArray();
	  a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] delHints = a.toStrings();
	  for(int i=0; i < blocks.length; i++){
	    fsNamesys.blockReceived(nodeReg, blocks[i], delHints[i]);
	  }
	  break;

	}
	case BFT_OP_REMOVE_DATANODE:{
	  DatanodeID datanodeID = new DatanodeID();
	  datanodeID.readFields(in);
	  fsNamesys.removeDatanode(datanodeID);
	  break;
	}
	case BFT_OP_GET_BLOCKLOCATIONS: {
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		String clientMachine = FSImage.readString(in);
		String src = FSImage.readString(in);
		long offset = readLong(in);
		long length = readLong(in);		
		fsNamesys.getBlockLocations(clientMachine, src, offset, length);
		break;
	}
	
	case BFT_OP_HB_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);		
		fsNamesys.heartbeatCheck();
		fsNamesys.lastHeartBeatCheck = bftTime;
		break;
	}
	
	case BFT_OP_REP_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		fsNamesys.computeDatanodeWork();
		fsNamesys.processPendingReplications();
		fsNamesys.lastReplicationCheck = bftTime;
		break;
	}
	
	case BFT_OP_DECOMM_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);		
		fsNamesys.decommissionedDatanodeCheck();
		fsNamesys.lastDecommissionCheck = bftTime;
		break;
	}
	
	case BFT_OP_LEASE_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);		
		fsNamesys.checkLease();
		fsNamesys.lastLeaseCheck = bftTime;
		break;
	}
	case BFT_OP_PENDING_REP_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);		
		fsNamesys.pendingReplications.pendingReplicationMonitor.pendingReplicationCheck();
		fsNamesys.lastPendingReplicationBlockCheck = bftTime;
		
		break;
	}

	default :
	  break;
	}
      } // end of while
    } finally {
      in.close();
    }

    fsNamesys.bftReplaying = false;
    fsNamesys.isPermissionEnabled = tmpIsPermissionEnabled;
    

    FSImage.LOG.info("BFT Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded." );
    
    return numEdits;
  }

  synchronized long getFsEditTime() {
    return lastEditTime;
  }
  
  synchronized void logEdit(byte op, Writable ... writables) {
  	super.logEdit(op, writables);
  	lastEditTime = FSNamesystem.now();
  }

  public void bftLogStartFile(long bftTime, String src, PermissionStatus permissions,
      String holder, String clientMachine,
      boolean overwrite, short replication, long blockSize
  ) throws IOException {
  	UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
  	ObjectWritable ow = new ObjectWritable(ugi.getClass(), ugi);
  	ow.setConf(new Configuration());
    logEdit(BFT_OP_START_FILE, ow, toLogLong(bftTime), new UTF8(src), permissions, 
	new UTF8(holder), new UTF8(clientMachine),
	overwrite ? toLogLong(1):toLogLong(0),
	    toLogReplication(replication), toLogLong(blockSize));
  } 

  public void bftLogAppend(long bftTime, String src, String holder, String clientName) throws  IOException{
  	UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
  	ObjectWritable ow = new ObjectWritable(ugi.getClass(), ugi);
  	ow.setConf(new Configuration());
    logEdit(BFT_OP_APPEND, ow, toLogLong(bftTime), new UTF8(src), new UTF8(holder), new UTF8(clientName));
  }

  public void bftLogSetReplication(String src, short replication) 
  throws IOException{
  	UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
  	ObjectWritable ow = new ObjectWritable(ugi.getClass(), ugi);
  	ow.setConf(new Configuration());
    logEdit(BFT_OP_SET_REPLICATION, ow, 
	new UTF8(src), FSEditLog.toLogReplication(replication));
  }

  public void bftLogSetPermission(String src, FsPermission permissions) 
  throws IOException{
  	UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
  	ObjectWritable ow = new ObjectWritable(ugi.getClass(), ugi);
  	ow.setConf(new Configuration());
    logEdit(BFT_OP_SET_PERMISSION, ow, new UTF8(src), FSEditLog.toLogReplication(permissions.toShort()));
  }

  public void bftLogSetOwner(String src, String username, String groupname) 
  throws IOException {
  	UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
  	ObjectWritable ow = new ObjectWritable(ugi.getClass(), ugi);
  	ow.setConf(new Configuration());
    UTF8 u = new UTF8(username == null? "": username);
    UTF8 g = new UTF8(groupname == null? "": groupname);
    logEdit(BFT_OP_SET_OWNER, ow, new UTF8(src), u, g);
  }

  public void bftLogAddBlock(long bfttime, String src, String clientName, Block newBlock,
  		DatanodeInfo[] datanodeInfos) throws IOException{

  	String[] targetSIDs = new String[datanodeInfos.length];
  	for(int i=0; i<datanodeInfos.length; i++){
  		targetSIDs[i] = datanodeInfos[i].storageID;
  	}

  	logEdit(BFT_OP_ADD_BLOCK, toLogLong(bfttime), new UTF8(src), new UTF8(clientName), newBlock,
  			new ArrayWritable(targetSIDs));
  }

  public void bftLogAddBlock(long bfttime, String src, String clientName, Block newBlock,
  		DatanodeInfo[] datanodeInfos, byte[] hash) throws IOException{

  	String[] targetSIDs = new String[datanodeInfos.length];
  	for(int i=0; i<datanodeInfos.length; i++){
  		targetSIDs[i] = datanodeInfos[i].storageID;
  	}
  	logEdit(BFT_OP_ADD_BLOCK_BFTDN, toLogLong(bfttime), new UTF8(src), new UTF8(clientName), newBlock,
  			new ArrayWritable(targetSIDs), new MD5Hash(hash));
  }

  public void bftLogAbandonBlock(Block b, String src, String holder) 
  throws IOException {
    logEdit(BFT_OP_ABANDON_BLOCK, b, new UTF8(src), new UTF8(holder));
  }

  public void bftLogComplete(String src, String clientName) throws IOException{
    logEdit(BFT_OP_COMPLETE, new UTF8(src), new UTF8(clientName));
  }
  
  public void bftLogComplete(String src, String clientName, byte[] hash) throws IOException{
    logEdit(BFT_OP_COMPLETE_BFTDN, new UTF8(src), new UTF8(clientName),
    		new MD5Hash(hash));
  }

  public void bftLogReportBadBlocks(LocatedBlock[] blocks) throws IOException{
    logEdit(BFT_OP_REPORT_BAD_BLOCKS, new ArrayWritable(LocatedBlock.class, blocks));
  }

  public void bftLogNextGenerationStamp(long time, Block block) throws IOException {
    logEdit(BFT_OP_NEXT_GENERATION_TIME, toLogLong(time), block);
  }

  public void bftLogCommitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
  ) throws IOException {
  	
    String[] targetSIDs = new String[newtargets.length];
    for(int i=0; i< newtargets.length; i++){
      targetSIDs[i] = newtargets[i].storageID;
    }
  	
    logEdit(BFT_OP_COMMIT_BLOCK_SYNCHRONIZATION,
    		block,
    		toLogLong(newgenerationstamp),
    		toLogLong(newlength),
    		closeFile ? toLogLong(1):toLogLong(0),
	    deleteblock ? toLogLong(1):toLogLong(0),
		new ArrayWritable(targetSIDs));
  }

  public void bftLogRename(long time, String src, String dst) throws IOException {
    logEdit(BFT_OP_RENAME, toLogLong(time), new UTF8(src), new UTF8(dst));
  }

  public void bftLogDelete(long time, String src, boolean recursive) throws IOException {
  	UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
  	ObjectWritable ow = new ObjectWritable(ugi.getClass(), ugi);
  	ow.setConf(new Configuration());
    logEdit(BFT_OP_DELETE, ow, toLogLong(time), new UTF8(src), 
	recursive ? toLogLong(1) : toLogLong(0));
  }

  public void bftLogMkdirs(long time, String src, PermissionStatus permissions) throws IOException{
    logEdit(BFT_OP_MKDIR, toLogLong(time), new UTF8(src), permissions);
  }

  public void bftLogRenewLease(long time, String clientName) throws IOException {
    logEdit(BFT_OP_RENEW_LEASE, toLogLong(time), new UTF8(clientName));
  }

  public void bftLogSetSafeMode(SafeModeAction action) throws IOException {
    logEdit(BFT_OP_SET_SAFEMODE, toLogLong(action.ordinal()));
  }

  public void bftLogRefreshNodes() throws IOException {
    logEdit(BFT_OP_REFRESH_NODES);
  }

  public void bftLogSetQuota(String path, long namespaceQuota, 
      long diskspaceQuota)  throws IOException {
    logEdit(BFT_OP_SET_QUOTA, new UTF8(path), toLogLong(namespaceQuota), 
	toLogLong(diskspaceQuota));
  }

  public void bftLogSetTimes(String src, long mtime, long atime) 
  throws IOException {
    logEdit(BFT_OP_SET_TIMES, new UTF8(src), toLogLong(mtime),
	toLogLong(atime));
  }

  public void bftLogRegister(DatanodeRegistration nodeReg) 
  throws IOException {
    logEdit(BFT_OP_REGISTER, nodeReg);
  }

  public void bftLogSendHeartbeat(long time, DatanodeRegistration nodeReg,
      long capacity,
      long dfsUsed,
      long remaining,
      int xmitsInProgress,
      int xceiverCount) throws IOException {

    UTF8[] info = new UTF8[]{
	toLogLong(capacity), toLogLong(dfsUsed), toLogLong(remaining),
	toLogLong(xmitsInProgress), toLogLong(xceiverCount) };

    logEdit(BFT_OP_SEND_HEARTBEAT, toLogLong(time), nodeReg, 
	new ArrayWritable(UTF8.class, info));

  }

  public void bftLogBlockReport(DatanodeRegistration nodeReg, long[] blocks) 
  throws IOException {
    UTF8[] blocksToLog = new UTF8[blocks.length];
    for(int i=0; i< blocks.length; i++){
      blocksToLog[i] = toLogLong(blocks[i]);
    }
    logEdit(BFT_OP_BLOCK_REPORT, nodeReg, 
	new ArrayWritable(UTF8.class, blocksToLog));
  }
  
  public void bftLogBlockReport(DatanodeRegistration nodeReg, Block[] blocks)
  throws IOException {
  	logEdit(BFT_OP_BLOCK_REPORT_BFTDN, nodeReg,
  			new ArrayWritable(Block.class, blocks));
  }

  public void bftLogBlockReceived(DatanodeRegistration nodeReg, 
      Block blocks[],
      String delHints[]) throws IOException {

    logEdit(BFT_OP_BLOCK_RECEIVED, nodeReg, 
	new ArrayWritable(Block.class, blocks), new ArrayWritable(delHints));
  }

  public void bftLogRemoveDatanode(DatanodeID nodeID) throws IOException {

    logEdit(BFT_OP_REMOVE_DATANODE, nodeID);
  }
  
  public void bftLogGetBlockLocations(long time, String clientMachine, 
  		String src, long offset, long length){
  	logEdit(BFT_OP_GET_BLOCKLOCATIONS, toLogLong(time), 
  			new UTF8(clientMachine), new UTF8(src), toLogLong(offset),
  			toLogLong(length));
  }
  
	public void bftLogHeartBeatMonitor(long time) {
		logEdit(BFT_OP_HB_MON, toLogLong(time));
	}
	public void bftLogDecommisionMonitor(long time) {
		logEdit(BFT_OP_DECOMM_MON, toLogLong(time));
	}
	public void bftLogReplicationMonitor(long time) {
		logEdit(BFT_OP_REP_MON, toLogLong(time));
	}
	public void bftLogLeaseMonitor(long time) {
		logEdit(BFT_OP_LEASE_MON, toLogLong(time));
	}
	public void bftLogPendingReplicationMonitor(long time) {
		logEdit(BFT_OP_PENDING_REP_MON, toLogLong(time));
	}	
	
  //
  // We do not log actual namespace modification
  // Instead, we log each operation that modifies system state
  // Followings are for nullifying logging namespce modification of original hdfs
  //
  
  public void logOpenFile(String path, INodeFileUnderConstruction newNode) 
                   throws IOException {
  }

  public void logCloseFile(String path, INodeFile newNode) {
  }
  
  public void logMkDir(String path, INode newNode) {
  }
  
  void logRename(String src, String dst, long timestamp) {
  }
  
  void logSetReplication(String src, short replication) {
  }

  void logSetQuota(String src, long nsQuota, long dsQuota) {
  }

  void logSetPermissions(String src, FsPermission permissions) {
  }

  void logSetOwner(String src, String username, String groupname) {
  }

  void logDelete(String src, long timestamp) {
  }

  void logGenerationStamp(long genstamp) {
  }

  void logTimes(String src, long mtime, long atime) {
  }

  public void logSync() throws IOException {    
  }
  
  private static void printLog(EditLogFileInputStream edits) throws IOException{
  	
    
    int numEdits = 0;

    int logVersion = 0;
    
    DataInputStream in = new DataInputStream(new BufferedInputStream(edits));

    try{
      
      // Read log file version. Could be missing. 
      in.mark(4);
      // If edits log is greater than 2G, available method will return negative
      // numbers, so we avoid having to call available
      boolean available = true;

      try {
        logVersion = in.readByte();
      } catch (EOFException e) {
        available = false;
      }
      if (available) {
        in.reset();
        logVersion = in.readInt();
        if (logVersion < FSConstants.LAYOUT_VERSION) // future version
          throw new IOException(
                          "Unexpected version of the file system log file: "
                          + logVersion + ". Current version = " 
                          + FSConstants.LAYOUT_VERSION + ".");
      }
      assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
                            "Unsupported version " + logVersion;

      
      //
      // XXX: we may want to add some version information here
      //

      while (true) {
	long timestamp = 0;
	long mtime = 0;
	long atime = 0;
	long blockSize = 0;
	byte opcode = -1;
	try {
	  opcode = in.readByte();
	  if (opcode == OP_INVALID) {
	    System.out.println("Invalid opcode, reached end of edit log " +
		"Number of transactions found " + numEdits);
	    break; // no more transactions
	  }
	} catch (EOFException e) {
	  break; // no more transactions
	}
	
	System.out.println("OPCODE : " + opcode );
	//System.out.println("OPCODE: " + opcode);
	
	numEdits++;
	switch (opcode) {

	case BFT_OP_START_FILE:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  String src = FSImage.readString(in);
	  PermissionStatus ps = PermissionStatus.read(in);
	  String holder = FSImage.readString(in);
	  String clientMachine = FSImage.readString(in);
	  boolean overwrite = readLong(in) > 0 ? true : false;
	  short replication = readShort(in);
	  blockSize = readLong(in);
	  //fsNamesys.startFile(src, ps, holder, clientMachine, overwrite, replication, blockSize);
	  System.out.println("BFT_OP_START_FILE");
	  break;
	} 

	case BFT_OP_APPEND:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  //fsNamesys.appendFile(FSImage.readString(in), FSImage.readString(in), FSImage.readString(in));
		System.out.println("BFT_OP_APPEND");
	  break;
	}

	case BFT_OP_SET_REPLICATION:{
	  //fsNamesys.setReplication(FSImage.readString(in), readShort(in));
	  System.out.println("BFT_OP_SET_REPLICATION : " + FSImage.readString(in) +" "+ readShort(in));
	  break;
	}

	case BFT_OP_SET_PERMISSION:{
	  //fsNamesys.setPermission(FSImage.readString(in), FsPermission.read(in));
	  System.out.println("BFT_OP_SET_PERMISSION : " + FSImage.readString(in) +" "+ new FsPermission(readShort(in)));
	  break;
	}

	case BFT_OP_SET_OWNER:{
	  //fsNamesys.setOwner(FSImage.readString(in), FSImage.readString(in), FSImage.readString(in));
	  System.out.println("BFT_OP_SET_OWNER" + " " + FSImage.readString(in) + FSImage.readString(in)
	  	+	" " + FSImage.readString(in));
	  break;
	}

	case BFT_OP_ADD_BLOCK:{
	  String src = FSImage.readString(in);
	  String clientName = FSImage.readString(in);
	  Block b = new Block();
	  b.readFields(in);
	  //System.out.println("### Adding block : " +b.getBlockId());
	  
	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] sids = a.toStrings();
	  //fsNamesys.bftGetAddtionalBlock(src,clientName, b, sids);
		System.out.println("BFT_OP_ADD_BLOCK : " + src + " " + clientName + " " + b);
		for(String s : sids){
			System.out.println(s);
		}
	  break;
	}
	
	case BFT_OP_ADD_BLOCK_BFTDN:{
	  String src = FSImage.readString(in);
	  String clientName = FSImage.readString(in);
	  Block b = new Block();
	  b.readFields(in);
	  //System.out.println("### Adding block BFTDN: " +b.getBlockId());
	  
	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] sids = a.toStrings();

	  //String hash = FSImage.readString(in);
	  
	  MD5Hash md5 = MD5Hash.read(in);
	  
	  //fsNamesys.bftGetAddtionalBlock(src,clientName, b, sids);
		System.out.println("BFT_OP_ADD_BLOCK_BFTDN");
	  break;
	}

	case BFT_OP_ABANDON_BLOCK:{
	  Block b = new Block();
	  b.readFields(in);
	  //fsNamesys.abandonBlock(b, FSImage.readString(in), FSImage.readString(in));
	  System.out.println("BFT_OP_ABANDON_BLOCK : "+FSImage.readString(in) + " " + FSImage.readString(in));
	  break;
	}

	case BFT_OP_COMPLETE: {
	  //fsNamesys.completeFile(FSImage.readString(in), FSImage.readString(in));
	  System.out.println("BFT_OP_COMPLETE : "+FSImage.readString(in) + " " + FSImage.readString(in));
	  break;
	}
	case BFT_OP_COMPLETE_BFTDN: {
		String src = FSImage.readString(in);
		String clientName = FSImage.readString(in);
		MD5Hash md5 = MD5Hash.read(in);
		//String hash = FSImage.readString(in);
		//fsNamesys.setHashOfBlock(src,clientName, md5.getDigest());
	  //fsNamesys.completeFile(src, clientName);
		System.out.println("BFT_OP_COMPLETE_BFTDN");
	  break;
	}

	case BFT_OP_REPORT_BAD_BLOCKS:{

	  ArrayWritable a = new ArrayWritable(LocatedBlock.class);
	  a.readFields(in);
	  LocatedBlock[] blocks = (LocatedBlock[]) a.toArray();
	  for(int i=0; i < blocks.length; i++){
	    Block blk = blocks[i].getBlock();
	    DatanodeInfo[] nodes = blocks[i].getLocations();
	    for (int j = 0; j < nodes.length; j++) {
	      DatanodeInfo dn = nodes[j];
	      //fsNamesys.markBlockAsCorrupt(blk, dn);
	    }
	  }
	  System.out.println("BFT_OP_REPORT_BAD_BLOCKS");
	  break;
	}

	case BFT_OP_NEXT_GENERATION_TIME:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  Block b = new Block();
	  b.readFields(in);
	  //fsNamesys.nextGenerationStampForBlock(b);
	  System.out.println("BFT_OP_NEXT_GENERATION_TIME");
	  break;
	}

	case BFT_OP_COMMIT_BLOCK_SYNCHRONIZATION:{
	  Block b = new Block();
	  b.readFields(in);
	  
	  long newGenerationStamp = readLong(in);
	  long newlength = readLong(in);
	  boolean closeFile = readLong(in) > 0 ? true : false;
	  boolean deleteblock = readLong(in) > 0 ? true : false;
	  
	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] sids = a.toStrings();
	  
	  DatanodeDescriptor[] targetDescriptors = new DatanodeDescriptor[sids.length];
    for(int i=0; i < sids.length; i++){
      //targetDescriptors[i] = fsNamesys.datanodeMap.get(sids[i]);
    }
	  
    //fsNamesys.commitBlockSynchronization(b, newGenerationStamp, newlength,
	  //    closeFile, deleteblock, targetDescriptors);
    System.out.println("BFT_OP_COMMIT_BLOCK_SYNCHRONIZATION");
	  break;
	}
	case BFT_OP_RENAME:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  //fsNamesys.renameTo(FSImage.readString(in), FSImage.readString(in));
		System.out.println("BFT_OP_RENAME :" + FSImage.readString(in)+ " " + FSImage.readString(in));
	  break;
	}
	case BFT_OP_DELETE:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  String src = FSImage.readString(in);
	  boolean recursive = readLong(in) > 0 ? true:false;
	  //fsNamesys.delete(src, recursive);
	  System.out.println("BFT_OP_DELETE");
	  break;
	}
	case BFT_OP_MKDIR:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		
		String src = FSImage.readString(in);
		PermissionStatus permission = PermissionStatus.read(in);		
	  //fsNamesys.mkdirs(src, permission);
	  //System.out.println("Num files : " + fsNamesys.dir.rootDir.numItemsInTree());
	  //System.out.println("root dir : " + System.identityHashCode(fsNamesys.dir.rootDir));
	  System.out.println("BFT_OP_MKDIR");
	  break;
	}
	case BFT_OP_RENEW_LEASE:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  //fsNamesys.renewLease(FSImage.readString(in));
	  System.out.println("BFT_OP_RENEW_LEASE :" + FSImage.readString(in));
	  break;
	}
	case BFT_OP_SET_SAFEMODE:{
	  int index = (int) readLong(in);
	  //fsNamesys.setSafeMode(SafeModeAction.values()[index]);
	  System.out.println("BFT_OP_SET_SAFEMODE");
	  break;
	}
	case BFT_OP_REFRESH_NODES:{
	  //fsNamesys.refreshNodes(null);
		System.out.println("BFT_OP_REFRESH_NODES");
	  break;          
	}
	case BFT_OP_SET_QUOTA:{
	  //fsNamesys.setQuota(FSImage.readString(in), 
	  //    readLong(in), readLong(in));
	  System.out.println("BFT_OP_SET_QUOTA : "+FSImage.readString(in)+" "+readLong(in)
	  		+ " " + readLong(in));
	  break;
	}
	case BFT_OP_SET_TIMES:{
	  //fsNamesys.setTimes(FSImage.readString(in),
	  //    readLong(in), readLong(in));
	  System.out.println("BFT_OP_SET_TIMES : " + FSImage.readString(in) +
	  		" " +readLong(in)+" "+readLong(in));
	  break;
	}
	case BFT_OP_REGISTER:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  //fsNamesys.registerDatanode(nodeReg);
	  System.out.println("BFT_OP_REGISTER");
	  break;
	}
	case BFT_OP_SEND_HEARTBEAT:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  int len = in.readInt();
	  assert len == 5;
	  long capacity = readLong(in);
	  long dfsUsed = readLong(in);
	  long remaining = readLong(in);
	  int xceiverCount = (int) readLong(in);
	  int xmitsInProgress = (int) readLong(in);
	  System.out.println("BFT_OP_SEND_HEARTBEAT : " + nodeReg +
	  		" " + dfsUsed +
	  		" " + remaining +
	  		" " + xceiverCount +
	  		" " + xmitsInProgress);
	  //fsNamesys.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining, xceiverCount, xmitsInProgress);
	  break;          
	}
	case BFT_OP_BLOCK_REPORT:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);

	  ArrayWritable a = new ArrayWritable(UTF8.class);
	  int len = in.readInt();
	  long[] blocks = new long[len];
	  for(int i=0; i < len; i++){
	    blocks[i] = readLong(in);
	  }

	  BlockListAsLongs blist = new BlockListAsLongs(blocks);

	  //fsNamesys.processReport(nodeReg, blist);
	  System.out.println("BFT_OP_BLOCK_REPORT");
	  break;                    
	}
	
	case BFT_OP_BLOCK_REPORT_BFTDN:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  ArrayWritable a = new ArrayWritable(Block.class);
	  a.readFields(in);
	  Block[] blocks = (Block[])a.toArray();
	  
	  //fsNamesys.processReport(nodeReg, blocks);
	  System.out.println("BFT_OP_BLOCK_REPORT_BFTDN");
	  break;                    
	}
	
	case BFT_OP_BLOCK_RECEIVED:{
	  DatanodeRegistration nodeReg = new  DatanodeRegistration();
	  nodeReg.readFields(in);
	  ArrayWritable a = new ArrayWritable(Block.class);
	  a.readFields(in);
	  Block[] blocks = (Block[]) a.toArray();
	  a = new ArrayWritable(UTF8.class);
	  a.readFields(in);
	  String[] delHints = a.toStrings();
	  for(int i=0; i < blocks.length; i++){
	    //fsNamesys.blockReceived(nodeReg, blocks[i], delHints[i]);
	  }
	  System.out.println("BFT_OP_BLOCK_RECEIVED");
	  for(int i=0; i < blocks.length; i++){
	    System.out.println(nodeReg + " " + blocks[i] + " " + delHints[i]);
	  }
	  break;

	}
	case BFT_OP_REMOVE_DATANODE:{
	  DatanodeID datanodeID = new DatanodeID();
	  datanodeID.readFields(in);
	  //fsNamesys.removeDatanode(datanodeID);
	  System.out.println("BFT_OP_REMOVE_DATANODE");
	  break;
	}
	case BFT_OP_GET_BLOCKLOCATIONS: {
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		String clientMachine = FSImage.readString(in);
		String src = FSImage.readString(in);
		long offset = readLong(in);
		long length = readLong(in);		
		//fsNamesys.getBlockLocations(clientMachine, src, offset, length);
		System.out.println("BFT_OP_GET_BLOCKLOCATIONS");
		break;
	}
	
	case BFT_OP_HB_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);		
		//fsNamesys.heartbeatCheck();
		//fsNamesys.lastHeartBeatCheck = bftTime;
		System.out.println("BFT_OP_HB_MON");
		break;
	}
	
	case BFT_OP_REP_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		/*
		fsNamesys.computeDatanodeWork();
		fsNamesys.processPendingReplications();
		fsNamesys.lastReplicationCheck = bftTime;
		*/
		System.out.println("BFT_OP_REP_MON");
		break;
	}
	
	case BFT_OP_DECOMM_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		/*
		fsNamesys.decommissionedDatanodeCheck();
		fsNamesys.lastDecommissionCheck = bftTime;
		*/
		System.out.println("BFT_OP_DECOMM_MON");
		break;
	}
	
	case BFT_OP_LEASE_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		/*
		fsNamesys.checkLease();
		fsNamesys.lastLeaseCheck = bftTime;
		*/
		System.out.println("BFT_OP_LEASE_MON");
		break;
	}
	case BFT_OP_PENDING_REP_MON:{
		long bftTime = readLong(in);
		BFTRandom.setBftTime(bftTime);
		/*
		fsNamesys.pendingReplications.pendingReplicationMonitor.pendingReplicationCheck();
		fsNamesys.lastPendingReplicationBlockCheck = bftTime;
		*/
		System.out.println("BFT_OP_PENDING_REP_MON");
		break;
	}

	default :
	  break;
	}
      } // end of while
    } finally {
      in.close();
    }

    //fsNamesys.bftReplaying = false;
    //fsNamesys.isPermissionEnabled = tmpIsPermissionEnabled;
    

    //System.out.println("BFT Edits file " + edits.getName() 
    //    + " of size " + edits.length() + " edits # " + numEdits 
    //    + " loaded." );
    
    System.out.println("numEdits : " + numEdits);
  	
  	
  }
  
  public static void main(String args[]){
  	
  	File logfile = new File(args[0]);
  	EditLogFileInputStream stream;
		try {
			stream = new EditLogFileInputStream(logfile);
	  	
	  	printLog(stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

  
  }
}
