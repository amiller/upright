/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {
	/** Block and targets pair */
	public static class BlockTargetPair {
		public final Block block;
		public final DatanodeDescriptor[] targets;    

		BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
			this.block = block;
			this.targets = targets;
		}
	}

	/** A BlockTargetPair queue. */
	private static class BlockQueue {
		private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

		/** Size of the queue */
		synchronized int size() {return blockq.size();}

		/** Enqueue */
		synchronized boolean offer(Block block, DatanodeDescriptor[] targets) { 
			return blockq.offer(new BlockTargetPair(block, targets));
		}

		/** Dequeue */
		synchronized List<BlockTargetPair> poll(int numTargets) {
			if (numTargets <= 0 || blockq.isEmpty()) {
				return null;
			}
			else {
				List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
				for(; !blockq.isEmpty() && numTargets > 0; ) {
					numTargets -= blockq.peek().targets.length; 
					if (numTargets >= 0) {
						results.add(blockq.poll());
					}
				}
				return results;
			}
		}
	}

	private volatile BlockInfo blockList = null;
	// isAlive == heartbeats.contains(this)
	// This is an optimization, because contains takes O(n) time on Arraylist
	protected boolean isAlive = false;

	/** A queue of blocks to be replicated by this datanode */
	private BlockQueue replicateBlocks = new BlockQueue();
	/** A queue of blocks to be recovered by this datanode */
	private BlockQueue recoverBlocks = new BlockQueue();
	/** A set of blocks to be invalidated by this datanode */
	private Set<Block> invalidateBlocks = new TreeSet<Block>();
	/** A set of blocks to be rolled back by this datanode (for bft-datanode) */
	private Set<Block> rollbackBlocks = new TreeSet<Block>();

	/* Variables for maintaning number of blocks scheduled to be written to
	 * this datanode. This count is approximate and might be slightly higger
	 * in case of errors (e.g. datanode does not report if an error occurs 
	 * while writing the block).
	 */
	private int currApproxBlocksScheduled = 0;
	private int prevApproxBlocksScheduled = 0;
	private long lastBlocksScheduledRollTime = 0;
	private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min

	/** Default constructor */
	public DatanodeDescriptor() {}

	/** DatanodeDescriptor constructor
	 * @param nodeID id of the data node
	 */
	public DatanodeDescriptor(DatanodeID nodeID) {
		this(nodeID, 0L, 0L, 0L, 0);
	}

	/** DatanodeDescriptor constructor
	 * 
	 * @param nodeID id of the data node
	 * @param networkLocation location of the data node in network
	 */
	public DatanodeDescriptor(DatanodeID nodeID, 
			String networkLocation) {
		this(nodeID, networkLocation, null);
	}

	/** DatanodeDescriptor constructor
	 * 
	 * @param nodeID id of the data node
	 * @param networkLocation location of the data node in network
	 * @param hostName it could be different from host specified for DatanodeID
	 */
	public DatanodeDescriptor(DatanodeID nodeID, 
			String networkLocation,
			String hostName) {
		this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0);
	}

	/** DatanodeDescriptor constructor
	 * 
	 * @param nodeID id of the data node
	 * @param capacity capacity of the data node
	 * @param dfsUsed space used by the data node
	 * @param remaining remaing capacity of the data node
	 * @param xceiverCount # of data transfers at the data node
	 */
	public DatanodeDescriptor(DatanodeID nodeID, 
			long capacity,
			long dfsUsed,
			long remaining,
			int xceiverCount) {
		super(nodeID);
		updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
	}

	/** DatanodeDescriptor constructor
	 * 
	 * @param nodeID id of the data node
	 * @param networkLocation location of the data node in network
	 * @param capacity capacity of the data node, including space used by non-dfs
	 * @param dfsUsed the used space by dfs datanode
	 * @param remaining remaing capacity of the data node
	 * @param xceiverCount # of data transfers at the data node
	 */
	public DatanodeDescriptor(DatanodeID nodeID,
			String networkLocation,
			String hostName,
			long capacity,
			long dfsUsed,
			long remaining,
			int xceiverCount) {
		super(nodeID, networkLocation, hostName);
		updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
	}

	/**
	 * Add data-node to the block.
	 * Add block to the head of the list of blocks belonging to the data-node.
	 */
	boolean addBlock(BlockInfo b) {
		if(!b.addNode(this))
			return false;
		// add to the head of the data-node list
		blockList = b.listInsert(blockList, this);
		return true;
	}

	/**
	 * Remove block from the list of blocks belonging to the data-node.
	 * Remove data-node from the block.
	 */
	boolean removeBlock(BlockInfo b) {
		blockList = b.listRemove(blockList, this);
		return b.removeNode(this);
	}

	/**
	 * Move block to the head of the list of blocks belonging to the data-node.
	 */
	void moveBlockToHead(BlockInfo b) {
		blockList = b.listRemove(blockList, this);
		blockList = b.listInsert(blockList, this);
	}

	void resetBlocks() {
		this.capacity = 0;
		this.remaining = 0;
		this.dfsUsed = 0;
		this.xceiverCount = 0;
		this.blockList = null;
	}

	public int numBlocks() {
		return blockList == null ? 0 : blockList.listCount(this);
	}

	/**
	 */
	synchronized void updateHeartbeat(long capacity, long dfsUsed, long remaining,
			 int xceiverCount) {
		 this.capacity = capacity;
		 this.dfsUsed = dfsUsed;
		 this.remaining = remaining;
		 this.lastUpdate = FSNamesystem.now();//System.currentTimeMillis();
		 this.xceiverCount = xceiverCount;
		 rollBlocksScheduled(lastUpdate);
	 }

	 /**
	  * Iterates over the list of blocks belonging to the data-node.
	  */
	 static private class BlockIterator implements Iterator<Block> {
		 private BlockInfo current;
		 private DatanodeDescriptor node;

		 BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
			 this.current = head;
			 this.node = dn;
		 }

		 public boolean hasNext() {
			 return current != null;
		 }

		 public BlockInfo next() {
			 BlockInfo res = current;
			 current = current.getNext(current.findDatanode(node));
			 return res;
		 }

		 public void remove()  {
			 throw new UnsupportedOperationException("Sorry. can't remove.");
		 }
	 }

	 Iterator<Block> getBlockIterator() {
		 return new BlockIterator(this.blockList, this);
	 }

	 /**
	  * Store block replication work.
	  */
	 void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
		 assert(block != null && targets != null && targets.length > 0);
		 replicateBlocks.offer(block, targets);
	 }

	 /**
	  * Store block recovery work.
	  */
	 void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
		 assert(block != null && targets != null && targets.length > 0);
		 recoverBlocks.offer(block, targets);
	 }

	 /**
	  * Store block invalidation work.
	  */
	 void addBlocksToBeInvalidated(List<Block> blocklist) {
		 assert(blocklist != null && blocklist.size() > 0);
		 synchronized (invalidateBlocks) {
			 for(Block blk : blocklist) {
				 invalidateBlocks.add(blk);
			 }
		 }
	 }
	 
	 /**
	  * Store blocks to be rolled back
	  * @param oldBlk Block with old generation stamp
	  * @param newBlk Block with new generation stamp
	  */
	 void addBlockToBeRolledBack(Block oldBlk, Block newBlk){
		 synchronized (rollbackBlocks){
			 rollbackBlocks.add(oldBlk);
			 rollbackBlocks.add(newBlk);
		 }
	 }

	 /**
	  * The number of work items that are pending to be replicated
	  */
	 int getNumberOfBlocksToBeReplicated() {
		 return replicateBlocks.size();
	 }

	 /**
	  * The number of block invalidation items that are pending to 
	  * be sent to the datanode
	  */
	 int getNumberOfBlocksToBeInvalidated() {
		 synchronized (invalidateBlocks) {
			 return invalidateBlocks.size();
		 }
	 }

	 BlockCommand getReplicationCommand(int maxTransfers) {
		 List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
		 return blocktargetlist == null? null:
			 new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
	 }

	 BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
		 List<BlockTargetPair> blocktargetlist = recoverBlocks.poll(maxTransfers);
		 return blocktargetlist == null? null:
			 new BlockCommand(DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
	 }
	 
	 int getNumberOfBlocksToBeRolledback() {
		 synchronized (rollbackBlocks) {
			 return rollbackBlocks.size();
		 }
	 }
	 
	 BlockCommand getRollbackBlocks(){
		 Block[] rollbackList = null;
		 synchronized(rollbackBlocks){
			 if(rollbackBlocks.size() > 0){
				 rollbackList = (Block[]) rollbackBlocks.toArray(new Block[rollbackBlocks.size()]);
				 rollbackBlocks.clear();
			 }
		 }
		 return rollbackList == null? 
				 null: new BlockCommand(DatanodeProtocol.DNA_ROLLBACKBLOCK, rollbackList);
	 }

	 /**
	  * Remove the specified number of blocks to be invalidated
	  */
	 BlockCommand getInvalidateBlocks(int maxblocks) {
		 Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks); 
		 return deleteList == null? 
				 null: new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList);
	 }

	 static private Block[] getBlockArray(Collection<Block> blocks, int max) {
		 Block[] blockarray = null;
		 synchronized(blocks) {
			 int n = blocks.size();
			 if (max > 0 && n > 0) {
				 if (max < n) {
					 n = max;
				 }
				 blockarray = blocks.toArray(new Block[n]);
				 blocks.clear();
				 assert(blockarray.length > 0);
			 }
		 }
		 return blockarray;
	 }

	 void reportDiff(BlocksMap blocksMap,
			 BlockListAsLongs newReport,
			 Collection<Block> toAdd,
			 Collection<Block> toRemove,
			 Collection<Block> toInvalidate) {
		 // place a deilimiter in the list which separates blocks 
		 // that have been reported from those that have not
		 BlockInfo delimiter = new BlockInfo(new Block(), 1);
		 boolean added = this.addBlock(delimiter);
		 assert added : "Delimiting block cannot be present in the node";
		 if(newReport == null)
			 newReport = new BlockListAsLongs( new long[0]);
		 // scan the report and collect newly reported blocks
		 // Note we are taking special precaution to limit tmp blocks allocated
		 // as part this block report - which why block list is stored as longs
		 Block iblk = new Block(); // a fixed new'ed block to be reused with index i
		 for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
			 iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i), 
					 newReport.getBlockGenStamp(i));
			 BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
			 if(storedBlock == null) {
				 // If block is not in blocksMap it does not belong to any file
				 toInvalidate.add(new Block(iblk));
				 continue;
			 }
			 if(storedBlock.findDatanode(this) < 0) {// Known block, but not on the DN
				 // if the size differs from what is in the blockmap, then return
				 // the new block. addStoredBlock will then pick up the right size of this
				 // block and will update the block object in the BlocksMap
				 if (storedBlock.getNumBytes() != iblk.getNumBytes()) {
					 toAdd.add(new Block(iblk));
				 } else {
					 toAdd.add(storedBlock);
				 }
				 continue;
			 }
			 // move block to the head of the list
			 this.moveBlockToHead(storedBlock);
		 }
		 // collect blocks that have not been reported
		 // all of them are next to the delimiter
		 Iterator<Block> it = new BlockIterator(delimiter.getNext(0), this);
		 while(it.hasNext())
			 toRemove.add(it.next());
		 this.removeBlock(delimiter);
	 }
	 
	 /**
	  * Support for BFT datanode.
	  * In addition to the original functionality,
	  * it validates hashes of each reported block 
	  */
	 void reportDiff(BlocksMap blocksMap,
			 Block[] newReport,
			 Collection<Block> toAdd,
			 Collection<Block> toRemove,
			 Collection<Block> toInvalidate) {
		 // place a deilimiter in the list which separates blocks 
		 // that have been reported from those that have not
		 BlockInfo delimiter = new BlockInfo(new Block(), 1);
		 boolean added = this.addBlock(delimiter);
		 assert added : "Delimiting block cannot be present in the node";
		 if(newReport == null)
			 newReport = new Block[0];
		 // scan the report and collect newly reported blocks
		 // Note we are taking special precaution to limit tmp blocks allocated
		 // as part this block report - which why block list is stored as longs
		 //Block iblk = new Block(); // a fixed new'ed block to be reused with index i
		 for (int i = 0; i < newReport.length; ++i) {
			 //iblk.set(newReport[i].getBlockId(), newReport[i].getNumBytes(), 
			 //		 newReport[i].getGenerationStamp());
			 BlockInfo storedBlock = blocksMap.getStoredBlock(newReport[i]);
			 if(storedBlock == null) {
				 // If block is not in blocksMap it does not belong to any file
				 // If hash does not match, invalidate it
				 toInvalidate.add(newReport[i]);
				 continue;
			 }
			 if(!Arrays.equals(storedBlock.getHash(), newReport[i].getHash())){
				 // If hash does not match, invalidate it
				 System.out.println("storedBlock.hash : " + (new MD5Hash(storedBlock.getHash())));
				 System.out.println("newReport.hash : " + (new MD5Hash(newReport[i].getHash())));
				 toInvalidate.add(newReport[i]);
				 continue;
			 }
			 if(storedBlock.findDatanode(this) < 0) {// Known block, but not on the DN
				 // if the size differs from what is in the blockmap, then return
				 // the new block. addStoredBlock will then pick up the right size of this
				 // block and will update the block object in the BlocksMap
				 if (storedBlock.getNumBytes() != newReport[i].getNumBytes()) {
					 toAdd.add(newReport[i]);
				 } else {
					 toAdd.add(storedBlock);
				 }
				 continue;
			 }
			 // move block to the head of the list
			 this.moveBlockToHead(storedBlock);
		 }
		 // collect blocks that have not been reported
		 // all of them are next to the delimiter
		 Iterator<Block> it = new BlockIterator(delimiter.getNext(0), this);
		 while(it.hasNext())
			 toRemove.add(it.next());
		 this.removeBlock(delimiter);
	 }
	 

	 /** Serialization for FSEditLog */
	 void readFieldsFromFSEditLog(DataInput in) throws IOException {
		 this.name = UTF8.readString(in);
		 this.storageID = UTF8.readString(in);
		 this.infoPort = in.readShort() & 0x0000ffff;

		 this.capacity = in.readLong();
		 this.dfsUsed = in.readLong();
		 this.remaining = in.readLong();
		 this.lastUpdate = in.readLong();
		 this.xceiverCount = in.readInt();
		 this.location = Text.readString(in);
		 this.hostName = Text.readString(in);
		 setAdminState(WritableUtils.readEnum(in, AdminStates.class));
	 }

	 /**
	  * @return Approximate number of blocks currently scheduled to be written 
	  * to this datanode.
	  */
	 public int getBlocksScheduled() {
		 return currApproxBlocksScheduled + prevApproxBlocksScheduled;
	 }

	 /**
	  * Increments counter for number of blocks scheduled. 
	  */
	 void incBlocksScheduled() {
		 currApproxBlocksScheduled++;
	 }

	 /**
	  * Decrements counter for number of blocks scheduled.
	  */
	 void decBlocksScheduled() {
		 if (prevApproxBlocksScheduled > 0) {
			 prevApproxBlocksScheduled--;
		 } else if (currApproxBlocksScheduled > 0) {
			 currApproxBlocksScheduled--;
		 } 
		 // its ok if both counters are zero.
	 }

	 /**
	  * Adjusts curr and prev number of blocks scheduled every few minutes.
	  */
	 private void rollBlocksScheduled(long now) {
		 if ((now - lastBlocksScheduledRollTime) > 
		 BLOCKS_SCHEDULED_ROLL_INTERVAL) {
			 prevApproxBlocksScheduled = currApproxBlocksScheduled;
			 currApproxBlocksScheduled = 0;
			 lastBlocksScheduledRollTime = now;
		 }
	 }
	 /** 
	  * BFT<p>
	  * Serialization of DatanodeDescriptor is divided into two parts 
	  * : write() and write2().
	  * This is because replicateBlocks and recoverBlocks have references 
	  * to other DatanodeDescriptors
	  * which can be restored only after all DatanodeDescriptors are loaded.
	  * write()/readFields() saves/reads all other fields other than 
	  * replicatedBlocks and recoverBlocks.
	  */
	 public void write1(DataOutput out) throws IOException {
		 super.write(out);

		 out.writeBoolean(isAlive);
		 out.writeInt(currApproxBlocksScheduled);
		 out.writeInt(prevApproxBlocksScheduled);
		 out.writeLong(lastBlocksScheduledRollTime);

		 //TODO : other states ( blockList, replicateBlocks, recoverBlocks, invalidateBlocks )

		 //
		 // blockList
		 //
		 int nBlocks = numBlocks();
		 out.writeInt(nBlocks); // number of blocks in blockList
		 Iterator<Block> iter = this.getBlockIterator();
		 Block[] blocks = new Block[nBlocks];
		 int i = 0;
		 while(iter.hasNext()){
			 blocks[i] = iter.next();
			 i++;
		 }
		 assert i == nBlocks;
		 // we does this such that, when we read this list, the order of blocks in the list remains same
		 for(i=nBlocks-1; i >= 0; i--){
			 Block b = blocks[i];
			 b.write(out);
		 }

		 //
		 // invalidateBlocks
		 //
		 nBlocks = invalidateBlocks.size();
		 out.writeInt(nBlocks);
		 for(Block b : invalidateBlocks){
			 //
			 // Since we won't be able to find BlockInfo for this block when we load it from image,
			 // we should write all info rather than just block id
			 //
			 b.write(out);
		 }
		 
		 if(FSNamesystem.bftdatanode){
			 //
			 // rollback Blocks
			 //
			 nBlocks = rollbackBlocks.size();
			 out.writeInt(nBlocks);
			 for(Block b : rollbackBlocks){
				 //
				 // Since we won't be able to find BlockInfo for this block when we load it from image,
				 // we should write all info rather than just block id
				 //
				 b.write(out);
			 }
		 }

	 }

	 /**
	  * BFT<p>
	  * Serialization of replicateBlocks and recoverBlocks
	  */

	 public void write2(DataOutput out) throws IOException {
		 FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
		 //
		 // replicateBlocks
		 //
		 int nBlocks = replicateBlocks.size();
		 out.writeInt(nBlocks); // number of entries in replicateBlocks
		 LinkedList<BlockTargetPair> la = (LinkedList<BlockTargetPair>) replicateBlocks.blockq;
		 Iterator<BlockTargetPair> iter2 = replicateBlocks.blockq.iterator();
		 int n=0;
		 while(iter2.hasNext()){
			 BlockTargetPair btp = iter2.next();
			 Block b = btp.block;
			 out.writeLong(b.getBlockId());
			 int targetLen = btp.targets.length;
			 out.writeInt(targetLen);
			 for(int i=0; i < targetLen; i++){
				 DatanodeDescriptor dd = btp.targets[i];
				 assert dd != null;
				 assert (fsNamesystem.getDataNodeInfo(dd.storageID) != null) : "**** " +  dd.storageID + " is missing in datanodeMap";
				 Text.writeString(out, dd.storageID);
			 }
		 }

		 //
		 // recoverBlocks
		 //
		 nBlocks = recoverBlocks.size();
		 out.writeInt(nBlocks); // number of entries in recoverBlocks
		 iter2 = recoverBlocks.blockq.iterator();  
		 while(iter2.hasNext()){
			 BlockTargetPair btp = iter2.next();
			 out.writeLong(btp.block.getBlockId());
			 int targetLen = btp.targets.length;
			 out.writeInt(targetLen);
			 for(int i=0; i < targetLen; i++){
				 DatanodeDescriptor dd = btp.targets[i];
				 assert dd != null;
				 Text.writeString(out, dd.storageID);
			 }
		 }

	 }

	 /**
	  * BFT<p>
	  * Refer to write()
	  */
	 public void readFields1(DataInput in) throws IOException {
		 super.readFields(in);

		 isAlive = in.readBoolean();
		 currApproxBlocksScheduled = in.readInt();
		 prevApproxBlocksScheduled = in.readInt();
		 lastBlocksScheduledRollTime = in.readLong();

		 //TODO : other states ( blockList, replicateBlocks, recoverBlocks, invalidateBlocks )
		 BlocksMap blocksMap = FSNamesystem.getFSNamesystem().blocksMap;

		 //
		 // blockList
		 //

		 int nBlocks = in.readInt();
		 Block[] blocks = new Block[nBlocks];
		 for(int i=0; i < nBlocks; i++){
			 blocks[i] = new Block();
			 blocks[i].readFields(in);
			 
			 BlockInfo bi = blocksMap.getStoredBlock(blocks[i]);
			 			 
			 INodeFile inode = bi.getINode();
			 //if(inode == null){
			 //throw new IOException("INodeFile for block " + blocks[i] + "does not exist in blocksMap");
			 //}
			 // inode can be null if it is deleted while its blocks remain
			 if(inode != null){
				 short replication = inode.getReplication();
				 blocksMap.addNode(blocks[i], this, replication);
			 }else{
				 blocksMap.addNode(blocks[i], this, bi.getCapacity());
			 }
		 }

		 //
		 // invalidateBlocks
		 //
		 nBlocks = in.readInt();
		 for(int i=0; i < nBlocks; i++){
			 //BlockInfo b = blocksMap.getStoredBlock(new Block(in.readLong()));
			 Block b = new Block();
			 b.readFields(in);
			 
			 BlockInfo bi = blocksMap.getStoredBlock(b);
			 if(bi!=null){
				 this.invalidateBlocks.add(bi);
			 }else{			 
				 this.invalidateBlocks.add(b);
			 }
		 }
		 
		 if(FSNamesystem.bftdatanode){
			 // rollback blocks
			 nBlocks = in.readInt();
			 for(int i=0; i < nBlocks; i++){
				 //BlockInfo b = blocksMap.getStoredBlock(new Block(in.readLong()));
				 Block b = new Block();
				 b.readFields(in);

				 BlockInfo bi = blocksMap.getStoredBlock(b);
				 if(bi!=null){
					 this.rollbackBlocks.add(bi);
				 }else{			 
					 this.rollbackBlocks.add(b);
				 }
			 }
		 }

	 }

	 /**
	  * BFT<p>
	  * 
	  */
	 public void readFields2(DataInput in) throws IOException {
		 FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
		 BlocksMap blocksMap = fsNamesystem.blocksMap;

		 //
		 // replicateBlocks
		 //

		 int nBlocks = in.readInt();
		 for(int i=0; i < nBlocks; i++){
			 Block block = new Block(in.readLong());
			 BlockInfo b = blocksMap.getStoredBlock(block);
			 int nTargets = in.readInt();
			 DatanodeDescriptor[] targets = new DatanodeDescriptor[nTargets];
			 for(int j=0; j < nTargets; j++){
				 String sID = Text.readString(in);
				 targets[j] = (DatanodeDescriptor) fsNamesystem.getDataNodeInfo(sID);
				 if( targets[j] == null){
					 System.err.println("SID " + sID + " is not in datanodeMap");
					 System.exit(-1);
				 }
				 assert targets[j]!=null;
			 }
			 if(b == null){
				 // This case happens when the file is deleted before the block is
				 // fully replicated
				 replicateBlocks.offer(block, targets);
			 } else{
				 replicateBlocks.offer(b, targets);
			 }
		 }

		 //
		 // recoverBlocks
		 //

		 nBlocks = in.readInt();
		 for(int i=0; i < nBlocks; i++){
			 Block block = new Block(in.readLong());
			 BlockInfo b = blocksMap.getStoredBlock(block);			 
			 int nTargets = in.readInt();
			 DatanodeDescriptor[] targets = new DatanodeDescriptor[nTargets];
			 for(int j=0; j < nTargets; j++){
				 String sID = Text.readString(in);
				 targets[j] = (DatanodeDescriptor) fsNamesystem.getDataNodeInfo(sID);
				 assert targets[j]!=null;
			 }
			 if(b==null){
				 recoverBlocks.offer(block, targets);
			 }else{
				 recoverBlocks.offer(b, targets);
			 }
		 }

	 }

	 public String dumpDatanode(){
		 StringBuffer buffer = new StringBuffer();
		 buffer.append(super.dumpDatanode());
		 buffer.append("\n xceiverCount : " + xceiverCount);
		 buffer.append("\n currApprox: " + currApproxBlocksScheduled);
		 buffer.append("\n prevApprox: " + prevApproxBlocksScheduled);
		 buffer.append("\n lastBlocksScheduledRollTime: " + lastBlocksScheduledRollTime);
		 if(isAlive){
			 buffer.append("\n ALIVE");
		 }else{
			 buffer.append("\n DEAD");
		 }

		 buffer.append("\n Block List : ");

		 Iterator<Block> bi = getBlockIterator();
		 while(bi.hasNext()){
			 buffer.append(bi.next().getBlockId());
			 buffer.append("     ");
		 }

		 buffer.append("\n Invalidate Blocks : ");

		 for(Block b : invalidateBlocks){
			 buffer.append(b.getBlockId());
			 buffer.append("     ");
		 }

		 buffer.append("\n Replicate Blocks : ");

		 Iterator<BlockTargetPair> bti = replicateBlocks.blockq.iterator();
		 while(bti.hasNext()){
			 BlockTargetPair btp = bti.next();
			 buffer.append(btp.block.getBlockId() + "(");
			 for(int i=0; i < btp.targets.length; i++){
				 buffer.append(btp.targets[i].storageID + ", ");
			 }
			 buffer.append(")");
		 }

		 buffer.append("\n Recover Blocks : ");

		 bti = recoverBlocks.blockq.iterator();
		 while(bti.hasNext()){
			 BlockTargetPair btp = bti.next();
			 buffer.append(btp.block.getBlockId() + "(");
			 for(int i=0; i < btp.targets.length; i++){
				 buffer.append(btp.targets[i].storageID + ", ");
			 }
			 buffer.append(")");
		 }


		 return buffer.toString();
	 }
}
