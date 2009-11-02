package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.BFTMessageDigest;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.BFTFSDatasetInterface.BFTBlockWriteStreams;
import org.apache.hadoop.hdfs.server.datanode.BlockReceiver.PacketResponder;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;

public class BFTBlockReceiver extends BlockReceiver {

	DataOutputStream mdOut;

	public static final int bytesPerMD = 64*1024; // 64K, size of a sub-block
	private int updatedBytes = 0;
	private long mdSeqNo = 0;

	// Digester to generate a hash of hashes of sub-blocks
	private BFTMessageDigest digester;
	// Digester to generate a hash of sub-block
	private BFTMessageDigest smallDigester;

	// Digester to generate a hash for the appended part of this block
	private BFTMessageDigest appendDigester;
	private BFTMessageDigest appendSmallDigester;

	BFTBlockReceiver(Block block, DataInputStream in, String inAddr,
			String myAddr, boolean isRecovery, String clientName,
			DatanodeInfo srcDataNode, DataNode datanode) throws IOException {

		super(block, in, inAddr, myAddr, isRecovery, clientName, srcDataNode, datanode);		

		BFTBlockWriteStreams bbws = (BFTBlockWriteStreams)streams;
		this.mdOut = new DataOutputStream(new BufferedOutputStream(
				bbws.md5Out, 
				SMALL_BUFFER_SIZE));

		try {
			digester = new BFTMessageDigest(datanode.fakemd5);
			smallDigester = new BFTMessageDigest(datanode.fakemd5);

			if(isRecovery){
				BFTFSDataset.BFTBlockInputStreams instr = null;
				try { 
					instr = ((BFTFSDataset)datanode.data).getTmpInputStreams(block, -1, -1, -1);

					long curLen = ((BFTFSDataset)datanode.data).findBlockFile(block.getBlockId()).length();
					int sizePartialSubBlock = (int)(curLen % bytesPerMD);
					long numCompleteSubBlock = curLen / bytesPerMD;
					DataInputStream mdin = new DataInputStream(instr.mdIn);
					// read md5 of complete sub-blocks and update the digester
					long bytesMD5Read = 0;
					for(int i=0; i < numCompleteSubBlock; i++){
						int size = mdin.readInt();
						byte[] md5 = new byte[size];
						IOUtils.readFully(mdin, md5, 0, size);
						digester.update(md5);
						bytesMD5Read += (4+size);
					}
					// read the last partial sub-block and update the smallDigester
					if(sizePartialSubBlock > 0){
						byte[] partialSubBlock = new byte[sizePartialSubBlock];
						IOUtils.skipFully(instr.dataIn, numCompleteSubBlock * bytesPerMD);
						IOUtils.readFully(instr.dataIn, partialSubBlock, 0, sizePartialSubBlock);
						smallDigester.update(partialSubBlock);
						updatedBytes = sizePartialSubBlock;
					}
					// adjust the position of md5 out channel
					FileOutputStream file = (FileOutputStream) bbws.md5Out;
					file.getChannel().position(bytesMD5Read);

					appendDigester = new BFTMessageDigest(datanode.fakemd5);
					appendSmallDigester = new BFTMessageDigest(datanode.fakemd5);
				} finally {
					IOUtils.closeStream(instr);
				}
			}

		} catch (NoSuchAlgorithmException e) {
		}
	}

	/** 
	 * Receives and processes a packet. It can contain many chunks.
	 * returns size of the packet.
	 */
	protected int receivePacket() throws IOException {

		int payloadLen = readNextPacket();

		if (payloadLen <= 0) {
			return payloadLen;
		}

		buf.mark();
		//read the header
		buf.getInt(); // packet length
		offsetInBlock = buf.getLong(); // get offset of packet in block
		long seqno = buf.getLong();    // get seqno
		boolean lastPacketInBlock = (buf.get() != 0);

		int endOfHeader = buf.position();
		buf.reset();

		if (LOG.isDebugEnabled()){
			LOG.debug("BFT : Receiving one packet for block " + block +
					" of length " + payloadLen +
					" seqno " + seqno +
					" offsetInBlock " + offsetInBlock +
					" lastPacketInBlock " + lastPacketInBlock);
		}

		setBlockPosition(offsetInBlock);

		//First write the packet to the mirror:
		if (mirrorOut != null) {
			try {
				mirrorOut.write(buf.array(), buf.position(), buf.remaining());
				mirrorOut.flush();
			} catch (IOException e) {
				handleMirrorOutError(e);
			}
		}

		buf.position(endOfHeader);        
		int len = buf.getInt();

		if (len < 0) {
			throw new IOException("Got wrong length during writeBlock(" + block + 
					") from " + inAddr + " at offset " + 
					offsetInBlock + ": " + len); 
		} 

		if (len == 0) {
			LOG.debug("BFT : Receiving empty packet for block " + block);
		} else {
			offsetInBlock += len;

			int checksumLen = ((len + bytesPerChecksum - 1)/bytesPerChecksum)*
			checksumSize;

			if ( buf.remaining() != (checksumLen + len)) {
				throw new IOException("Data remaining in packet does not match " +
				"sum of checksumLen and dataLen");
			}
			int checksumOff = buf.position();
			int dataOff = checksumOff + checksumLen;
			byte pktBuf[] = buf.array();

			buf.position(buf.limit()); // move to the end of the data.

			/* skip verifying checksum iff this is not the last one in the 
			 * pipeline and clientName is non-null. i.e. Checksum is verified
			 * on all the datanodes when the data is being written by a 
			 * datanode rather than a client. Whe client is writing the data, 
			 * protocol includes acks and only the last datanode needs to verify 
			 * checksum.
			 */
			if (mirrorOut == null || clientName.length() == 0) {
				verifyChunks(pktBuf, dataOff, len, pktBuf, checksumOff);
			}

			try {
				if (!finalized) {
					//finally write to the disk :
					out.write(pktBuf, dataOff, len);

					if(updatedBytes + len >= bytesPerMD){
						int bytesToUpdate = bytesPerMD - updatedBytes;
						smallDigester.update(pktBuf, dataOff, bytesToUpdate);
						if(appendSmallDigester != null){
							// this must happen only once
							appendSmallDigester.update(pktBuf, dataOff, bytesToUpdate);
							appendDigester.update(appendSmallDigester.digest());
						}        		
						byte[] md5 = smallDigester.digest();
						mdOut.writeInt(md5.length);
						mdOut.write(md5);
						digester.update(md5);
						if(appendDigester != null && appendSmallDigester == null){
							appendDigester.update(md5);
						}
						LOG.debug("Packet seq no : " + seqno +"," +
								" dataLen=" + len +"\n" + "MDSeqNo: " + mdSeqNo
								+ "Digest : " + (new MD5Hash(md5)));

						smallDigester.update(pktBuf, dataOff + bytesToUpdate, len-bytesToUpdate);
						mdSeqNo++;
						updatedBytes = len-bytesToUpdate;
						appendSmallDigester = null;
					} else {
						smallDigester.update(pktBuf, dataOff, len);
						if(appendSmallDigester != null){
							appendSmallDigester.update(pktBuf, dataOff, len);
						}
						updatedBytes += len;
					}

					// If this is a partial chunk, then verify that this is the only
					// chunk in the packet. Calculate new crc for this chunk.
					if (partialCrc != null) {
						if (len > bytesPerChecksum) {
							throw new IOException("Got wrong length during writeBlock(" + 
									block + ") from " + inAddr + " " +
									"A packet can have only one partial chunk."+
									" len = " + len + 
									" bytesPerChecksum " + bytesPerChecksum);
						}
						partialCrc.update(pktBuf, dataOff, len);
						byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
						checksumOut.write(buf);
						LOG.debug("Writing out partial crc for data len " + len);
						partialCrc = null;
					} else {
						checksumOut.write(pktBuf, checksumOff, checksumLen);
					}
					LOG.debug("BFT : write checksome done");
					datanode.myMetrics.bytesWritten.inc(len);
				}
			} catch (IOException iex) {
				datanode.checkDiskError(iex);
				throw iex;
			}
		}
		/// flush entire packet before sending ack
		flush();

		if(lastPacketInBlock){
			// calculate md5 of hashes of sub-blocks
			// and attach it to the current block
			
			if(updatedBytes > 0){
				if(appendSmallDigester != null){
					appendDigester.update(appendSmallDigester.digest());  				
				}

				byte[] md5 = smallDigester.digest();
				digester.update(md5);
				mdOut.writeInt(md5.length);
				mdOut.write(md5);
				LOG.debug("Packet seq no : " + seqno +"," +
						" dataLen=" + updatedBytes +"\n" + "MDSeqNo: " + mdSeqNo
						+ "\nDigest : " + (new MD5Hash(md5)));
				updatedBytes = 0;
				mdSeqNo++;
			}
			byte[] digest = null;
			digest = digester.digest();
			block.setHash(digest);
			
			byte[] addedDigest = null;
			if(appendDigester != null){
				addedDigest = appendDigester.digest();
				block.setAddedHash(addedDigest);
			}

			LOG.debug("FINAL Hash Val : " + new MD5Hash(digest).toString());
			LOG.debug("ADDED Hash Val : " + (addedDigest==null?"None":new MD5Hash(addedDigest).toString()));
		}

		// put in queue for pending acks
		if (responder != null) {
			((PacketResponder)responder.getRunnable()).enqueue(seqno,
					lastPacketInBlock); 
		}

		if (throttler != null) { // throttle I/O
			throttler.throttle(payloadLen);
		}

		return payloadLen;
	}

	void flush() throws IOException {
		super.flush();
		if( mdOut != null){
			mdOut.flush();
		}
	}

	/**
	 * close files.
	 */
	public void close() throws IOException {
		IOException ioe = null;
		try{
			super.close();
		} catch(IOException e){
			ioe = e;
		}

		try{
			if(mdOut != null){
				mdOut.flush();
				mdOut.close();
				mdOut = null;
			}
		}  catch(IOException e) {
			ioe = e;
		}

		// disk check
		if(ioe != null) {
			datanode.checkDiskError(ioe);
			throw ioe;
		}
	}
}
