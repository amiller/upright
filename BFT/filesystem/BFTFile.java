/**
 * 
 */
package BFT.filesystem;

import BFT.filesystem.merkle.*;

/**
 * @author riche
 *
 */
public class BFTFile implements Indexable {
	
	private String filename;
	
	public BFTFile(String fn) {
		this.filename = fn;
	}

	/* (non-Javadoc)
	 * @see BFT.util.Bytable#getBytes()
	 */
	public byte[] getBytes() {
		return filename.getBytes();
	}
	
	public String getFilename() {
		return filename;
	}
}
