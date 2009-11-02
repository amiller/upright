package org.apache.hadoop.hdfs.server.datanode;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.IOUtils;

/**
 * This interface extends {@link FSDatasetInterface} to
 * handle input/output streams of files that store md5s of sub-block 
 */
public interface BFTFSDatasetInterface extends FSDatasetInterface {
	
  static class BFTBlockWriteStreams extends BlockWriteStreams {
 	 OutputStream md5Out;
 	 BFTBlockWriteStreams(BlockWriteStreams bws, OutputStream mOut) {
 		 super(bws.dataOut, bws.checksumOut);
 		 md5Out = mOut;
 	 }
 	 BFTBlockWriteStreams(OutputStream dOut, OutputStream cOut, OutputStream mOut) {
 		 super(dOut,cOut);
 		 md5Out = mOut;
 	 }
  }	
	
  static class BFTBlockInputStreams extends BlockInputStreams implements Closeable {
    final InputStream mdIn;

    BFTBlockInputStreams(InputStream dataIn, InputStream checksumIn, InputStream mdIn) {
      super(dataIn, checksumIn);
      this.mdIn = mdIn;
    }

    /** {@inheritDoc} */
    public void close() {
      super.close();
      IOUtils.closeStream(mdIn);
    }
  }
  static class MDDataInputStream extends FilterInputStream {
    MDDataInputStream(InputStream stream, long len) {
      super(stream);
      length = len;
    }
    private long length;
    
    public long getLength() {
      return length;
    }
  }
  
  public MDDataInputStream getMDDataInputStream(Block b)
  throws IOException;

  public boolean mdFileExists(Block b) throws IOException;

  
}
