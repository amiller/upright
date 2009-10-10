package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import junit.framework.TestCase;

public class TestDFSSaveWholeStates extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  static final int numDatanodes = 3;
  short replication = 3;
  
  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        fileSys.getConf().getInt("io.file.buffer.size", 4096),
        (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    assertTrue(fileSys.exists(name));
    int replication = fileSys.getFileStatus(name).getReplication();
    assertEquals("replication for " + name, repl, replication);
    //We should probably test for more of the file properties.    
  }

  private void cleanupFile(FileSystem fileSys, Path name)
  throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  public void testDFSSaveWholeStates() throws Exception {

    Configuration conf = new Configuration();
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    //short replication = (short)conf.getInt("dfs.replication", 3);  

    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();

    NameNode namenode = cluster.getNameNode();
    namenode.metaSave("z0z0");
    
    FileSystem fs = cluster.getFileSystem();
    
    Path testFile = new Path("test");
    writeFile(fs, testFile, replication);
    checkFile(fs, testFile, replication);
    
    FSImage fsimage = namenode.getFSImage();
    fsimage.saveFSImage();
    
    fs.close();
    
    //cluster.shutdownNameNode();
    cluster.shutdown();
    
    //namenode = cluster.startNameNode(StartupOption.REGULAR);
    cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    namenode.metaSave("z1z1");
    
  }
}
