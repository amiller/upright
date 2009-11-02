package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.hdfs.BFTMessageDigest;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.ActiveFile;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSDir;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolumeSet;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.BlockWriteStreams;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataChecksum;

/**
 * This class extends {@link FSDataset}
 * to handle files that store md5s of sub-blocks
 */
public class BFTFSDataset extends FSDataset implements BFTFSDatasetInterface{

	class BFTFSDir extends FSDir{
		BFTFSDir children[];
		public BFTFSDir(File dir) throws IOException {			
      this.dir = dir;
      this.children = null;
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Mkdirs failed to create " + 
                                dir.toString());
        }
      } else {
        File[] files = dir.listFiles();
        int numChildren = 0;
        for (int idx = 0; idx < files.length; idx++) {
          if (files[idx].isDirectory()) {
            numChildren++;
          } else if (Block.isBlockFilename(files[idx])) {
            numBlocks++;
          }
        }
        if (numChildren > 0) {
          children = new BFTFSDir[numChildren];
          int curdir = 0;
          for (int idx = 0; idx < files.length; idx++) {
            if (files[idx].isDirectory()) {
              children[curdir] = new BFTFSDir(files[idx]);
              curdir++;
            }
          }
        }
      }
		}
		
		public File addBlock(Block b, File src) throws IOException {
      //First try without creating subdirectories
      File file = addBlock(b, src, false, false);          
      return (file != null) ? file : addBlock(b, src, true, true);
    }

    private File addBlock(Block b, File src, boolean createOk, 
                          boolean resetIdx) throws IOException {
      if (numBlocks < maxBlocksPerDir) {
        File dest = new File(dir, b.getBlockName());
        File metaData = getMetaFile( src, b );
        File newmeta = getMetaFile(dest, b);
        File mdData = getMDFile( src, b );
        File newmd = getMDFile(dest, b);
        if ( ! mdData.renameTo( newmd ) ||
        		! metaData.renameTo( newmeta ) ||
            ! src.renameTo( dest ) ) {
          throw new IOException( "could not move files for " + b +
                                 " from tmp to " + 
                                 dest.getAbsolutePath() );
        }
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("addBlock: Moved " + metaData + " to " + newmeta);
          DataNode.LOG.debug("addBlock: Moved " + mdData + " to " + newmd);
          DataNode.LOG.debug("addBlock: Moved " + src + " to " + dest);
        }

        numBlocks += 1;
        return dest;
      }
            
      if (lastChildIdx < 0 && resetIdx) {
        //reset so that all children will be checked
        lastChildIdx = random.nextInt(children.length);              
      }
            
      if (lastChildIdx >= 0 && children != null) {
        //Check if any child-tree has room for a block.
        for (int i=0; i < children.length; i++) {
          int idx = (lastChildIdx + i)%children.length;
          File file = children[idx].addBlock(b, src, false, resetIdx);
          if (file != null) {
            lastChildIdx = idx;
            return file; 
          }
        }
        lastChildIdx = -1;
      }
            
      if (!createOk) {
        return null;
      }
            
      if (children == null || children.length == 0) {
        children = new BFTFSDir[maxBlocksPerDir];
        for (int idx = 0; idx < maxBlocksPerDir; idx++) {
          children[idx] = new BFTFSDir(new File(dir, DataStorage.BLOCK_SUBDIR_PREFIX+idx));
        }
      }
            
      //now pick a child randomly for creating a new set of subdirs.
      lastChildIdx = random.nextInt(children.length);
      return children[ lastChildIdx ].addBlock(b, src, true, false); 
    }
		
    /**
     * Find md file, calculate hash from it and return the value 
     */
    private byte[] getHash(File[] listdir, File blockFile) {
    	BFTMessageDigest digester = null;
    	try {
				digester = new BFTMessageDigest(DataNode.fakemd5);
			} catch (NoSuchAlgorithmException e1) {
			}
    	
      String blockName = blockFile.getName();
      for (int j = 0; j < listdir.length; j++) {
        String path = listdir[j].getName();
        if (!path.startsWith(blockName) || !path.endsWith(MDDATA_EXTENSION)) {
          continue;
        }
        DataNode.LOG.debug("Calculating hash from file : "+path+" for block "+blockName);        
        try {
					DataInputStream mdIn = new DataInputStream(new FileInputStream(listdir[j]));
					long len = listdir[j].length();
					DataNode.LOG.debug("File len : "+len);
					long read = 0;
					while(read < len){
						int size = mdIn.readInt();
						
						read += 4;
						byte[] buf = new byte[size];
						int n = mdIn.read(buf, 0, size);
						if(n != size){
							DataNode.LOG.debug("Failed to read full md5");
							return null;
						}
						read += n;
						DataNode.LOG.debug("DIGEST size: "+size+", value: "+(new MD5Hash(buf)));
						digester.update(buf);						
					}					
					mdIn.close();
					byte[] hash = digester.digest();
					DataNode.LOG.debug("FINAL HASH : "+(new MD5Hash(hash)));
					return hash;
					
				} catch (FileNotFoundException e) {
					return null;
				} catch (IOException e) {
					return null;
				}
      }
      return null;
    }
    
    /**
     * Populate the given blockSet with any child blocks
     * found at this node.
     */
    public void getBlockInfo(TreeSet<Block> blockSet) {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getBlockInfo(blockSet);
        }
      }

      File blockFiles[] = dir.listFiles();
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFiles[i])) {
          long genStamp = getGenerationStampFromFile(blockFiles, blockFiles[i]);
          byte[] hash = getHash(blockFiles, blockFiles[i]);
          blockSet.add(new Block(blockFiles[i], blockFiles[i].length(), genStamp, hash));
        }
      }
    }
    
    void getVolumeMap(HashMap<Block, DatanodeBlockInfo> volumeMap, FSVolume volume) {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getVolumeMap(volumeMap, volume);
        }
      }

      File blockFiles[] = dir.listFiles();
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFiles[i])) {
          long genStamp = getGenerationStampFromFile(blockFiles, blockFiles[i]);
          byte[] hash = getHash(blockFiles, blockFiles[i]);
          volumeMap.put(new Block(blockFiles[i], blockFiles[i].length(), genStamp, hash), 
                        new DatanodeBlockInfo(volume, blockFiles[i]));
        }
      }
    }
		
		
	}
	
	class BFTFSVolume extends FSVolume{

		BFTFSVolume(File currentDir, Configuration conf) throws IOException {
      this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
      this.usableDiskPct = conf.getFloat("dfs.datanode.du.pct",
                                         (float) USABLE_DISK_PCT_DEFAULT);
      File parent = currentDir.getParentFile();

      this.detachDir = new File(parent, "detach");
      if (detachDir.exists()) {
        recoverDetachedBlocks(currentDir, detachDir);
      }

      // Files that were being written when the datanode was last shutdown
      // are now moved back to the data directory. It is possible that
      // in the future, we might want to do some sort of datanode-local
      // recovery for these blocks. For example, crc validation.
      //
      this.tmpDir = new File(parent, "tmp");
      if (tmpDir.exists()) {
        recoverDetachedBlocks(currentDir, tmpDir);
      }
      this.dataDir = new BFTFSDir(currentDir);
      if (!tmpDir.mkdirs()) {
        if (!tmpDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + tmpDir.toString());
        }
      }
      if (!detachDir.mkdirs()) {
        if (!detachDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + detachDir.toString());
        }
      }
      this.usage = new DF(parent, conf);
      this.dfsUsage = new DU(parent, conf);
      this.dfsUsage.start();
		}
		
    File addBlock(Block b, File f) throws IOException {
      File blockFile = dataDir.addBlock(b, f);
      File metaFile = getMetaFile( blockFile , b);
      File mdFile = getMDFile(blockFile, b);
      dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length()+mdFile.length());
      return blockFile;
    }
		
	}
	
	public BFTFSDataset(DataStorage storage, Configuration conf)
			throws IOException {

    this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
    FSVolume[] volArray = new BFTFSVolume[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      volArray[idx] = new BFTFSVolume(storage.getStorageDir(idx).getCurrentDir(), conf);
    }
    volumes = new FSVolumeSet(volArray);
    volumeMap = new HashMap<Block, DatanodeBlockInfo>();
    volumes.getVolumeMap(volumeMap);
    registerMBean(storage.getStorageID());
	}
	
  public static final String MDDATA_EXTENSION = ".md";
  public static final short MDDATA_VERSION = 1;
  
  static String getMDFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + MDDATA_EXTENSION;
  }
  
  static File getMDFile(File f , Block b) {
    return new File(getMDFileName(f.getAbsolutePath(),
                                    b.getGenerationStamp())); 
  }
  protected File getMDFile(Block b) throws IOException {
    return getMDFile(getBlockFile(b), b);
  }
  
  /** Find the corresponding md file from a given block file */
  protected static File findMDFile(final File blockFile) throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    File[] matches = parent.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return dir.equals(parent)
            && name.startsWith(prefix) && name.endsWith(MDDATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      throw new IOException("Meta file not found, blockFile=" + blockFile);
    }
    else if (matches.length > 1) {
      throw new IOException("Found more than one meta files: " 
          + Arrays.asList(matches));
    }
    return matches[0];
  }
  
  /**
   * Start writing to a block file
   * If isRecovery is true and the block pre-exists, then we kill all
      volumeMap.put(b, v);
      volumeMap.put(b, v);
   * other threads that might be writing to this block, and then reopen the file.
   */
  public BlockWriteStreams writeToBlock(Block b, boolean isRecovery) throws IOException {
    //
    // Make sure the block isn't a valid one - we're still creating it!
    //
    if (isValidBlock(b)) {
      if (!isRecovery) {
        throw new IOException("Block " + b + " is valid, and cannot be written to.");
      }
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client 
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.
      detachBlock(b, 1);
    }
    long blockSize = b.getNumBytes();

    //
    // Serialize access to /tmp, and check if file already there.
    //
    File f = null;
    List<Thread> threads = null;
    synchronized (this) {
      //
      // Is it already in the create process?
      //
      ActiveFile activeFile = ongoingCreates.get(b);
      if (activeFile != null) {
        f = activeFile.file;
        threads = activeFile.threads;
        
        if (!isRecovery) {
          throw new IOException("Block " + b +
                                  " has already been started (though not completed), and thus cannot be created.");
        } else {
          for (Thread thread:threads) {
            thread.interrupt();
          }
        }
        ongoingCreates.remove(b);
      }
      FSVolume v = null;
      if (!isRecovery) {
        v = volumes.getNextVolume(blockSize);
        // create temporary file to hold block in the designated volume
        f = createTmpFile(v, b);
        volumeMap.put(b, new DatanodeBlockInfo(v));
      } else if (f != null) {
        DataNode.LOG.info("Reopen already-open Block for append " + b);
        // create or reuse temporary file to hold block in the designated volume
        v = volumeMap.get(b).getVolume();
        volumeMap.put(b, new DatanodeBlockInfo(v));
      } else {
        // reopening block for appending to it.
        DataNode.LOG.info("Reopen Block for append " + b);
        v = volumeMap.get(b).getVolume();
        f = createTmpFile(v, b);
        File blkfile = getBlockFile(b);
        File oldmeta = getMetaFile(b);
        File newmeta = getMetaFile(f, b);
        File oldmd = getMDFile(b);
        File newmd = getMDFile(f,b);
        
        // rename md file to tmp directory
        DataNode.LOG.debug("Renaming " + oldmd + " to " + newmd);
        if (!oldmd.renameTo(newmd)) {
          throw new IOException("Block " + b + " reopen failed. " +
                                " Unable to move md file  " + oldmd +
                                " to tmp dir " + newmd);
        }

        // rename meta file to tmp directory
        DataNode.LOG.debug("Renaming " + oldmeta + " to " + newmeta);
        if (!oldmeta.renameTo(newmeta)) {
          throw new IOException("Block " + b + " reopen failed. " +
                                " Unable to move meta file  " + oldmeta +
                                " to tmp dir " + newmeta);
        }

        // rename block file to tmp directory
        DataNode.LOG.debug("Renaming " + blkfile + " to " + f);
        if (!blkfile.renameTo(f)) {
          if (!f.delete()) {
            throw new IOException("Block " + b + " reopen failed. " +
                                  " Unable to remove file " + f);
          }
          if (!blkfile.renameTo(f)) {
            throw new IOException("Block " + b + " reopen failed. " +
                                  " Unable to move block file " + blkfile +
                                  " to tmp dir " + f);
          }
        }
        volumeMap.put(b, new DatanodeBlockInfo(v));
      }
      if (f == null) {
        DataNode.LOG.warn("Block " + b + " reopen failed " +
                          " Unable to locate tmp file.");
        throw new IOException("Block " + b + " reopen failed " +
                              " Unable to locate tmp file.");
      }
      ongoingCreates.put(b, new ActiveFile(f, threads));
    }

    try {
      if (threads != null) {
        for (Thread thread:threads) {
          thread.join();
        }
      }
    } catch (InterruptedException e) {
      throw new IOException("Recovery waiting for thread interrupted.");
    }

    //
    // Finally, allow a writer to the block file
    // REMIND - mjc - make this a filter stream that enforces a max
    // block size, so clients can't go crazy
    //
    File metafile = getMetaFile(f, b);
  	File mdfile = getMDFile(f, b);
    DataNode.LOG.debug("writeTo blockfile is " + f + " of size " + f.length());
    DataNode.LOG.debug("writeTo metafile is " + metafile + " of size " + metafile.length());
    DataNode.LOG.debug("writeTo mdfile is " + mdfile + " of size " + mdfile.length());
    return new BFTBlockWriteStreams(createBlockWriteStreams( f , metafile),
    		new FileOutputStream( new RandomAccessFile( mdfile , "rw" ).getFD() ));
  }
  /*
  public BFTBlockWriteStreams writeToBlock(Block b, boolean isRecovery)
  	throws IOException {
  	
  	BlockWriteStreams bws = super.writeToBlock(b, isRecovery);
  	
  	
  	File mdfile = getMDFile(findBlockFile(b.getBlockId()), b);
  	    
    DataNode.LOG.debug("writeTo mdfile is " + mdfile + " of size " + mdfile.length());
    
    return new BFTBlockWriteStreams(bws,
  			new FileOutputStream( new RandomAccessFile( mdfile , "rw" ).getFD() ));
  	
  }*/
  
  /**
   * Returns handles to the block file and its metadata file
   */
  public synchronized BFTBlockInputStreams getTmpInputStreams(Block b, 
                          long blkOffset, long ckoff, long mdoff) throws IOException {

    DatanodeBlockInfo info = volumeMap.get(b);
    if (info == null) {
      throw new IOException("Block " + b + " does not exist in volumeMap.");
    }
    FSVolume v = info.getVolume();
    File blockFile = v.getTmpFile(b);
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (blkOffset > 0) {
      blockInFile.seek(blkOffset);
    }
    File metaFile = getMetaFile(blockFile, b);
    RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
    if (ckoff > 0) {
      metaInFile.seek(ckoff);
    }
    
    File mdFile = getMDFile(blockFile, b);
    RandomAccessFile mdInFile = new RandomAccessFile(mdFile, "r");
    if (mdoff > 0) {
    	mdInFile.seek(mdoff);
    }
    
    return new BFTBlockInputStreams(new FileInputStream(blockInFile.getFD()),
                                new FileInputStream(metaInFile.getFD()),
                                new FileInputStream(mdInFile.getFD()));
  }
  
  public boolean mdFileExists(Block b) throws IOException {
    return getMDFile(b).exists();
  }
  
  public long getMDDataLength(Block b) throws IOException {
    File mdFile = getMDFile( b );
    return mdFile.length();
  }

  public MDDataInputStream getMDDataInputStream(Block b)
      throws IOException {
    File mdFile = getMDFile( b );
    return new MDDataInputStream(new FileInputStream(mdFile),
                                                    mdFile.length());
  }
  
  /** {@inheritDoc} */
  public synchronized void updateBlock(Block oldblock, Block newblock
      ) throws IOException {
    if (oldblock.getBlockId() != newblock.getBlockId()) {
      throw new IOException("Cannot update oldblock (=" + oldblock
          + ") to newblock (=" + newblock + ").");
    }
    File blockFile = findBlockFile(oldblock.getBlockId());
    if (blockFile == null) {
      throw new IOException("Block " + oldblock + " does not exist.");
    }
    interruptOngoingCreates(oldblock);
    
    File oldMetaFile = findMetaFile(blockFile);
    File oldMDFile = findMDFile(blockFile);
    long oldgs = parseGenerationStamp(blockFile, oldMetaFile);
    
    //rename meta file to a tmp file
    File tmpMetaFile = new File(oldMetaFile.getParent(),
        oldMetaFile.getName()+"_tmp" + newblock.getGenerationStamp());
    if (!oldMetaFile.renameTo(tmpMetaFile)){
      throw new IOException("Cannot rename block meta file to " + tmpMetaFile);
    }
    //rename md file to a tmp file
    File tmpMDFile = new File(oldMDFile.getParent(),
        oldMDFile.getName()+"_tmp" + newblock.getGenerationStamp());
    if (!oldMDFile.renameTo(tmpMDFile)){
      throw new IOException("Cannot rename block meta file to " + tmpMDFile);
    }

    //update generation stamp
    if (oldgs > newblock.getGenerationStamp()) {
      throw new IOException("Cannot update block (id=" + newblock.getBlockId()
          + ") generation stamp from " + oldgs
          + " to " + newblock.getGenerationStamp());
    }
    
    //update length
    if (newblock.getNumBytes() > oldblock.getNumBytes()) {
      throw new IOException("Cannot update block file (=" + blockFile
          + ") length from " + oldblock.getNumBytes() + " to " + newblock.getNumBytes());
    }
    if (newblock.getNumBytes() < oldblock.getNumBytes()) {
      truncateBlock(blockFile, tmpMetaFile, tmpMDFile, oldblock.getNumBytes(), newblock.getNumBytes());
    }

    //rename the tmp file to the new meta file (with new generation stamp)
    File newMetaFile = getMetaFile(blockFile, newblock);
    if (!tmpMetaFile.renameTo(newMetaFile)) {
      throw new IOException("Cannot rename tmp meta file to " + newMetaFile);
    }
    //rename the tmp file to the new meta file (with new generation stamp)
    File newMDFile = getMDFile(blockFile, newblock);
    if (!tmpMDFile.renameTo(newMDFile)) {
      throw new IOException("Cannot rename tmp meta file to " + newMDFile);
    }

    updateBlockMap(ongoingCreates, oldblock, newblock);
    updateBlockMap(volumeMap, oldblock, newblock);

    // paranoia! verify that the contents of the stored block 
    // matches the block file on disk.
    validateBlockMetadata(newblock);
  }
  
  protected static void truncateBlock(File blockFile, File metaFile,
  		File mdFile, long oldlen, long newlen) throws IOException {
    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannout truncate block to from oldlen (=" + oldlen
          + ") to newlen (=" + newlen + ")");
    }
    FSDataset.truncateBlock(blockFile, metaFile, oldlen, newlen);
    
    int bytesPerMD5 = BFTBlockReceiver.bytesPerMD;
    int newNumMD5s = (int) ( (newlen-1) / bytesPerMD5) + 1;
    int lastpartialsubblocksize = (int) (newlen % bytesPerMD5);
    
    byte[] b = new byte[lastpartialsubblocksize];
    if(lastpartialsubblocksize>0){
    	// read the last partially filled sub-block
    	RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    	try{    		
    		long lastsubblockoffset = (newNumMD5s-1)*bytesPerMD5;
    		blockRAF.seek(lastsubblockoffset);
    		blockRAF.readFully(b, 0, lastpartialsubblocksize);
    	} finally{
    		blockRAF.close();
    	}
    }
    
    RandomAccessFile mdRAF = new RandomAccessFile(mdFile, "rw");
    try{
    	if(lastpartialsubblocksize > 0){
    		// if we have a partially filled sub-block
    		
    		// first, skipped through the offset that sub-block
    		long newLength = 0; 
    		for(int i=0; i < newNumMD5s -1; i++){
    			int size = mdRAF.readInt();
    			int skipped = 0;
    			while(skipped < size){
    				skipped += mdRAF.skipBytes(size);
    			}
    			newLength += (4+size);
    		}
    		BFTMessageDigest smallDigester = null;
    		try {
    			smallDigester = new BFTMessageDigest(DataNode.fakemd5);
    		} catch (NoSuchAlgorithmException e) {
    		}
    		// then, calculate and write the hash of the sub-block
    		smallDigester.update(b);
    		byte[] digest = smallDigester.digest();
    		mdRAF.setLength(newLength + 4 + digest.length);
    		mdRAF.seek(newLength);
    		mdRAF.write(digest);
    		newLength += (4 + digest.length);      
    	} else {
    		// if we don't have any partially filled sub-block
    		// just truncate the md file
    		long newLength = 0; 
    		for(int i=0; i < newNumMD5s; i++){
    			int size = mdRAF.readInt();
    			int skipped = 0;
    			while(skipped < size){
    				skipped += mdRAF.skipBytes(size);
    			}
    			newLength += (4+size);
    		}
    		mdRAF.setLength(newLength);
    	}
    } finally {
    	mdRAF.close();
    }
  }
  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  public void invalidate(Block invalidBlks[]) throws IOException {
    boolean error = false;
    for (int i = 0; i < invalidBlks.length; i++) {
      File f = null;
      FSVolume v;
      synchronized (this) {
        f = getFile(invalidBlks[i]);
        DatanodeBlockInfo dinfo = volumeMap.get(invalidBlks[i]);
        if (dinfo == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                           + invalidBlks[i] + 
                           ". BlockInfo not found in volumeMap.");
          error = true;
          continue;
        }
        v = dinfo.getVolume();
        if (f == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Block not found in blockMap." +
                            ((v == null) ? " " : " Block found in volumeMap."));
          error = true;
          continue;
        }
        if (v == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". No volume for this block." +
                            " Block found in blockMap. " + f + ".");
          error = true;
          continue;
        }
        File parent = f.getParentFile();
        if (parent == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Parent not found for file " + f + ".");
          error = true;
          continue;
        }
        v.clearPath(parent);
        volumeMap.remove(invalidBlks[i]);
      }
      File metaFile = getMetaFile( f, invalidBlks[i] );
      File mdFile = getMDFile(f, invalidBlks[i]);
      long blockSize = f.length()+metaFile.length()+mdFile.length();
      
      if ( !f.delete() || ( !metaFile.delete() && metaFile.exists() )
      		|| (!mdFile.delete() && mdFile.exists())) {
        DataNode.LOG.warn("Unexpected error trying to delete block "
                          + invalidBlks[i] + " at file " + f);
        error = true;
        continue;
      }
      v.decDfsUsed(blockSize);
      DataNode.LOG.info("Deleting block " + invalidBlks[i] + " file " + f);
      if (f.exists()) {
        //
        // This is a temporary check especially for hadoop-1220. 
        // This will go away in the future.
        //
        DataNode.LOG.info("File " + f + " was deleted but still exists!");
      }
    }
    if (error) {
      throw new IOException("Error in deleting blocks.");
    }
  }
  
}
