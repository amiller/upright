/**
 * $Id$
 */
package BFT.filesystem.merkle;

import BFT.messages.Digest;
import BFT.messages.HistoryDigest;

/**
 * @author riche
 *
 */
public class MerkleTreeNode extends Digest {

	MerkleTreeNode leftChild;
	MerkleTreeNode rightChild;
	MerkleTreeNode parent;
	boolean dirty;
	int min;
	int max;
	
	public MerkleTreeNode() {
		this(new byte[1]);
	}
	
	public MerkleTreeNode(int index) {
		this(index, index);
	}
	
	public MerkleTreeNode(int start, int end) {
		this(new byte[1], start, end);
	}
	
	public MerkleTreeNode(byte[] data, int index) {
		this(data, index, index);
	}
	
	public MerkleTreeNode(byte[] data, int start, int end) {
		super(data);
		this.dirty = false;
		this.min = start;
		this.max = end;
	}
	
	public MerkleTreeNode(byte[] data) {
		super(data);
		this.dirty = false;
	}

	private MerkleTreeNode(byte[] data, MerkleTreeNode l, MerkleTreeNode r) {
		this(data);
		this.leftChild = l;
		this.rightChild = r;
		l.parent = this;
		r.parent = this;
		min = l.min;
		max = r.max;
	}
	
    public static MerkleTreeNode combineDigestFactory(MerkleTreeNode a, MerkleTreeNode b) {
    	byte[] res = combineArrays(a.getBytes(), b.getBytes());
    	return new MerkleTreeNode(res, a, b);
    }
    
    public boolean isLeaf() {
    	return (min == max) && (leftChild == null) && (rightChild == null);
    }
    
    public boolean isRoot() {
    	return parent == null;
    }
    
    public byte[] getToken() {
    	byte[] bits = new byte[MerkleTreeNode.size() + 4];
    	System.arraycopy(BFT.util.UnsignedTypes.intToBytes(min), 0, bits, 0, 2);
    	System.arraycopy(BFT.util.UnsignedTypes.intToBytes(max), 0, bits, 2, 2);
    	System.arraycopy(bytes, 0, bits, 4, bytes.length);
    	return bits;
    }
    
    public static MerkleTreeNode fromToken(byte[] token) {
		byte[] byteMin = new byte[2];
		System.arraycopy(token, 0, byteMin, 0, 2);
		int newMin = BFT.util.UnsignedTypes.bytesToInt(byteMin);
		byte[] byteMax = new byte[2];
		System.arraycopy(token, 2, byteMax, 0, 2);
		int newMax = BFT.util.UnsignedTypes.bytesToInt(byteMax);
		byte[] newBytes = new byte[MerkleTreeNode.size()];
		System.arraycopy(token, 4, newBytes, 0, MerkleTreeNode.size());
		MerkleTreeNode newNode = new MerkleTreeNode();
		newNode.bytes = newBytes;
		newNode.min = newMin;
		newNode.max = newMax;
    	return newNode;
    }

    private static byte[] combineArrays(byte[] b1, byte[] b2){
    	byte[] res = new byte[b1.length + b2.length];
    	int i = 0;
    	for (; i < b1.length; i++)
    		res[i] = b1[i];
    	for (int j = 0; j < b2.length; j++)
    		res[i+j] = b2[j];
    	return res;
    }
    
    public boolean equals(MerkleTreeNode node) {
    	boolean retVal = true;
    	retVal = retVal && (min == node.min);
    	retVal = retVal && (max == node.max);
    	retVal = retVal && super.equals(node);
    	return retVal;
    }

		
}