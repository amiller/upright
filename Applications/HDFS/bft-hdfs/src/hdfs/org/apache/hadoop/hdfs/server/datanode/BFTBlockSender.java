package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;

public class BFTBlockSender extends BlockSender{
	
	private DataInputStream mdIn; // md5 stream to read md5s of sub-blocks
	private long mdFileSize;
	
	BFTBlockSender(Block block, long startOffset, long length,
			boolean corruptChecksumOk, boolean chunkOffsetOK, boolean verifyChecksum,
			DataNode datanode) throws IOException {
		this(block, startOffset, length, corruptChecksumOk, chunkOffsetOK,
				verifyChecksum, datanode,null);
	}

	BFTBlockSender(Block block, long startOffset, long length,
			boolean corruptChecksumOk, boolean chunkOffsetOK,
			boolean verifyChecksum, DataNode datanode, String clientTraceFmt)
			throws IOException {
	  try {
	  	
      this.block = block;
      this.chunkOffsetOK = chunkOffsetOK;
      this.corruptChecksumOk = corruptChecksumOk;
      this.verifyChecksum = verifyChecksum;
      this.blockLength = datanode.data.getLength(block);
      this.transferToAllowed = datanode.transferToAllowed;
      this.clientTraceFmt = clientTraceFmt;

      if ( !corruptChecksumOk || datanode.data.metaFileExists(block) ) {
        checksumIn = new DataInputStream(
                new BufferedInputStream(datanode.data.getMetaDataInputStream(block),
                                        BUFFER_SIZE));

        // read and handle the common header here. For now just a version
       BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
       short version = header.getVersion();

        if (version != FSDataset.METADATA_VERSION) {
          LOG.warn("Wrong version (" + version + ") for metadata file for "
              + block + " ignoring ...");
        }
        checksum = header.getChecksum();
      } else {
        LOG.warn("Could not find metadata file for " + block);
        // This only decides the buffer size. Use BUFFER_SIZE?
        checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL,
            16 * 1024);
      }
      
      // create input stream for reading a message digest of this block
      mdIn = new DataInputStream(
          new BufferedInputStream(((BFTFSDataset)datanode.data).getMDDataInputStream(block),
              BUFFER_SIZE));
      mdFileSize = ((BFTFSDataset)datanode.data).getMDDataLength(block);

      /* If bytesPerChecksum is very large, then the metadata file
       * is mostly corrupted. For now just truncate bytesPerchecksum to
       * blockLength.
       */        
      bytesPerChecksum = checksum.getBytesPerChecksum();
      if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > blockLength){
        checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
                                   Math.max((int)blockLength, 10*1024*1024));
        bytesPerChecksum = checksum.getBytesPerChecksum();        
      }
      checksumSize = checksum.getChecksumSize();

      if (length < 0) {
        length = blockLength;
      }

      endOffset = blockLength;
      if (startOffset < 0 || startOffset > endOffset
          || (length + startOffset) > endOffset) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + endOffset + " )";
        LOG.warn(datanode.dnRegistration + ":sendBlock() : " + msg);
        throw new IOException(msg);
      }
      
      // XXX this need to use writePacketSize (unfortunately this class is static)
      int bytesPerPacket = 64*1024;
      // we want to send data as a unit of packet
      offset = (startOffset - (startOffset % bytesPerPacket));
      if (length >= 0) {
    	  // Make sure endOffset points to end of a checksumed chunk.
    	  long tmpLen = startOffset + length;
    	  if (tmpLen % bytesPerPacket != 0) {
    		  tmpLen += (bytesPerPacket - tmpLen % bytesPerPacket);
    	  }
    	  if (tmpLen < endOffset) {
    		  endOffset = tmpLen;
    	  }
      }

      // seek to the right offsets
      if (offset > 0) {
        long checksumSkip = (offset / bytesPerChecksum) * checksumSize;
        // note blockInStream is  seeked when created below
        if (checksumSkip > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }
      }
      seqno = 0;

      blockIn = datanode.data.getBlockInputStream(block, offset); // seek to offset
    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      IOUtils.closeStream(blockIn);
      throw ioe;
    }
	}

	long sendBlock(DataOutputStream out, OutputStream baseStream, 
			BlockTransferThrottler throttler) throws IOException {
		if( out == null ) {
			throw new IOException( "out stream is null" );
		}
		this.throttler = throttler;

		long initialOffset = offset;
		long totalRead = 0;
		OutputStream streamForSendChunks = out;

		try {
			checksum.writeHeader(out);
			if ( chunkOffsetOK ) {
				out.writeLong( offset );
			}
			
			// Send list of md5s of sub-blocks
			LOG.debug("mdFileSize : " + mdFileSize);
			out.writeLong(mdFileSize);
			byte[] buf = new byte[1024];
			int n;
			for(;;){
				n = mdIn.read(buf);
				if(n <= 0){
					break;
				} else {
					out.write(buf, 0, n);
				}
			}
			mdIn.close();
			
			out.flush();

			int maxChunksPerPacket;
			int pktSize = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;

			if (transferToAllowed && !verifyChecksum && 
					baseStream instanceof SocketOutputStream && 
					blockIn instanceof FileInputStream) {

				FileChannel fileChannel = ((FileInputStream)blockIn).getChannel();

				// blockInPosition also indicates sendChunks() uses transferTo.
				blockInPosition = fileChannel.position();
				streamForSendChunks = baseStream;

				// assure a mininum buffer size.
				maxChunksPerPacket = (Math.max(BUFFER_SIZE, 
						MIN_BUFFER_WITH_TRANSFERTO)
						+ bytesPerChecksum - 1)/bytesPerChecksum;

				// allocate smaller buffer while using transferTo(). 
				pktSize += checksumSize * maxChunksPerPacket;
			} else {
				maxChunksPerPacket = Math.max(1,
						(BUFFER_SIZE + bytesPerChecksum - 1)/bytesPerChecksum);
				pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
			}

			ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);

			while (endOffset > offset) {
				long len = sendChunks(pktBuf, maxChunksPerPacket, 
						streamForSendChunks);
				offset += len;
				totalRead += len + ((len + bytesPerChecksum - 1)/bytesPerChecksum*
						checksumSize);
				seqno++;
			}
			out.writeInt(0); // mark the end of block        
			out.flush();
		} finally {
			if (clientTraceFmt != null) {
				ClientTraceLog.info(String.format(clientTraceFmt, totalRead));
			}
			close();
		}

		blockReadFully = (initialOffset == 0 && offset >= blockLength);

		return totalRead;
	}


}
