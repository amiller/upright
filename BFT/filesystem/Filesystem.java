/**	
 * $Id$
 */
package BFT.filesystem;

import java.util.*;
import java.io.*;

import BFT.filesystem.merkle.*;
import BFT.filesystem.merkle.exceptions.*;

/**
 * @author riche
 *
 */
public class Filesystem {
	
	protected MerkleTree current;
	protected MerkleTree shouldHave;
	protected MerkleTree last;
	protected MerkleTree lastShouldHave;
	protected MerkleTree disk;
	protected MerkleTree diskShouldHave;

	protected Vector<Map<Integer, BFTFile>> caches;
	protected Map<Integer, BFTFile> currCache;
	
	protected BufferedOutputStream log;
	
	protected Vector<BFTFile> filesOnDisk;
	
	public Filesystem() {
		for(int i = 0; i < Parameters.initialSize; i++) {
			filesOnDisk.add(null);
		}
		
	}
	
	
}
