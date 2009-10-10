/**
 * $Id$
 */
package BFT.filesystem.merkle;

import java.util.*;

import BFT.util.*;
import BFT.filesystem.*;
import BFT.filesystem.merkle.exceptions.*;

/**
 * @author riche
 *
 */
public class MerkleTree {
	
	private MerkleTreeNode root;
	private Vector<Indexable> store;
	private Map<Integer, MerkleTreeNode> nodeMap;
	
	public MerkleTree(Vector<Indexable> v) {
		nodeMap = new HashMap<Integer, MerkleTreeNode>();
		this.store = v;
		int exp = 0;
		while((int)Math.pow(2, exp) < store.size()) {
			exp++;
		}
		for(int i = store.size(); i < (int)Math.pow(2, exp); i++) {
			store.add(i, null);
		}
		Vector<MerkleTreeNode> hashes = new Vector<MerkleTreeNode>();
		for(int i = 0; i < store.size(); i++) {
			if(store.get(i) != null) {
				MerkleTreeNode temp = new MerkleTreeNode(store.get(i).getBytes(), i);
				nodeMap.put(i, temp);
				hashes.add(temp);
			}
			else {
				MerkleTreeNode temp = new MerkleTreeNode(i);
				nodeMap.put(i, temp);
				hashes.add(temp);
			}
		}
		while(hashes.size() > 1) {
			Vector<MerkleTreeNode> temp = new Vector<MerkleTreeNode>();
			for(int i = 0; i < hashes.size(); i += 2) {
				temp.add(MerkleTreeNode.combineDigestFactory(hashes.get(i), hashes.get(i + 1)));
			}
			hashes = temp;
		}
		root = hashes.get(0);
	}
	
	public Indexable getLeafData(int index) {
		return store.get(index);
	}
	
	public void updateLeafData(int index, Indexable b) {
		// Check to see if we need to grow tree
		if(index <= root.max) {
			// node already exists, simply update
			store.set(index, b);
			MerkleTreeNode node = nodeMap.get(index);
			MerkleTreeNode newNode = new MerkleTreeNode(b.getBytes(), index);
			newNode.parent = node.parent;
			if (newNode.parent.leftChild == node) {
				newNode.parent.leftChild = newNode;
			} else {
				newNode.parent.rightChild = newNode;
			}
			markChainDirty(newNode);
		}
		else {
			// node is beyond our scope, grow tree
			while(index > root.max) {
				Vector<Indexable> vec = new Vector<Indexable>();
				int origSize = store.size();
				for(int i = origSize; i < 2*origSize; i++) {
					if(i == index) {
						vec.add(i - origSize, b);
					}
					else {
						vec.add(i - origSize, null);
					}
				}
				MerkleTree newtree = new MerkleTree(vec);
				root = MerkleTreeNode.combineDigestFactory(root, newtree.root);
				for(int i = origSize; i < 2*origSize; i++) {
					MerkleTreeNode tempNode = newtree.nodeMap.get(i - origSize);
					tempNode.max += origSize;
					tempNode.min += origSize;
					nodeMap.put(i, tempNode);
					store.add(i, newtree.store.get(i - origSize));
				}
			}
		}
	}
	
	private void markChainDirty(MerkleTreeNode node) {
		MerkleTreeNode curr = node.parent;
		while(curr != null) {
			curr.dirty = true;
			curr = curr.parent;
		}
	}
	
	public boolean equals(byte[] token) {
		boolean retVal = true;
		MerkleTreeNode newNode = MerkleTreeNode.fromToken(token);
		retVal = retVal && (this.equals(newNode));
		return retVal;
	}
	
	public boolean equals(MerkleTreeNode node) {
		boolean retVal = true;
		MerkleTreeNode testNode;
		try {
			testNode = getNode(node.min, node.max);
			retVal = retVal && (testNode.equals(node));
		} catch (NotInTreeException e) {
			retVal = false;
		}
		return retVal;
	}
	
	public MerkleTreeNode getNode(int index) {
		return nodeMap.get(index);
	}
	
	public MerkleTreeNode getNode(int nodeMin, int nodeMax) throws NotInTreeException {
		MerkleTreeNode retNode = null;
		if(nodeMin == nodeMax) {
			retNode = getNode(nodeMin);
		}
		else {
			retNode = getNode(nodeMin, nodeMax, root);
		}
		return retNode;
	}
	
	private MerkleTreeNode getNode(int nodeMin, int nodeMax, MerkleTreeNode node) throws NotInTreeException {
		MerkleTreeNode retNode = null;
		if(nodeMin == node.min && nodeMax == node.max) {
			retNode = node;
		}
		else {
			if(nodeMax <= node.leftChild.max) {
				retNode = getNode(nodeMin, nodeMax, node.leftChild);
			}
			else if(nodeMin >= node.rightChild.min && nodeMin <= node.rightChild.max) {
				retNode = getNode(nodeMin, nodeMax, node.rightChild);
			}
			else {
				throw new NotInTreeException(nodeMin + ":" + nodeMax);
			}
		}
		return retNode;
	}
	
	public void update() {
		root = update(root);
	}
	
	private MerkleTreeNode update(MerkleTreeNode node) {
		MerkleTreeNode retNode = null;
		if(!node.dirty) {
			retNode = node;
		}
		else {
			retNode = MerkleTreeNode.combineDigestFactory(update(node.leftChild), update(node.rightChild));
		}
		return retNode;
	}

	public MerkleTreeNode getRoot() {
		return root;
	}
	
	public static void main(String[] args) {
		Vector<Indexable> files = new Vector<Indexable>();
		for(int i = 0; i < 5; i++) {
			files.add(new BFTFile(new String("Taylor" + i)));
		}
		MerkleTree tree = new MerkleTree(files);
		//System.out.println(tree.getRoot().min + ":" + tree.getRoot().max);
		BFT.util.UnsignedTypes.printBytes(tree.getRoot().getBytes());
		
		//System.out.println("Changing a leaf!");
		BFTFile newFile = new BFTFile(new String("Hello World!"));
		tree.updateLeafData(3, newFile);
		tree.update();
		//System.out.println(tree.getRoot().min + ":" + tree.getRoot().max);
		BFT.util.UnsignedTypes.printBytes(tree.getRoot().getBytes());
		
		//System.out.println("Adding a leaf past the current capacity!");
		newFile = new BFTFile(new String("Goodbye Cruel World!"));
		tree.updateLeafData(12, newFile);
		tree.update();
		//System.out.println(tree.getRoot().min + ":" + tree.getRoot().max);
		BFT.util.UnsignedTypes.printBytes(tree.getRoot().getBytes());
		
		//System.out.println("Changing a leaf!");
		newFile = new BFTFile(new String("Hello World!"));
		tree.updateLeafData(14, newFile);
		tree.update();
		//System.out.println(tree.getRoot().min + ":" + tree.getRoot().max);
		BFT.util.UnsignedTypes.printBytes(tree.getRoot().getBytes());
		
		//System.out.println("Adding a leaf past the current capacity!");
		newFile = new BFTFile(new String("Goodbye Cruel World!"));
		tree.updateLeafData(49, newFile);
		tree.update();
		//System.out.println(tree.getRoot().min + ":" + tree.getRoot().max);
		BFT.util.UnsignedTypes.printBytes(tree.getRoot().getBytes());
		
		System.out.print("Testing leaf node retrieval 1: ");
		MerkleTreeNode test = new MerkleTreeNode(12);
		MerkleTreeNode test1 = tree.getNode(12);
		if(test.equals(test1)) {
			//System.out.println("FAIL");
			BFT.util.UnsignedTypes.printBytes(test.getBytes());
			//System.out.println();
			BFT.util.UnsignedTypes.printBytes(test1.getBytes());
		}
		else {
			//System.out.println("WIN");
		}

		System.out.print("Testing leaf node retrieval 2 (");
		newFile = (BFTFile)tree.getLeafData(49);
		System.out.print(newFile.getFilename() + "): ");
		test = new MerkleTreeNode(newFile.getBytes(), 49);
		test1 = tree.getNode(49);
		if(test.equals(test1)) {
			//System.out.println("WIN");
		}
		else {
			//System.out.println("FAIL");
			BFT.util.UnsignedTypes.printBytes(test.getBytes());
			//System.out.println();
			BFT.util.UnsignedTypes.printBytes(test1.getBytes());
		}
		
		System.out.print("Testing intermediate node retrieval (52,53): ");
		try {
			test = tree.getNode(52, 53);
			//if(test.min == 52 && test.max == 53) //System.out.println("WIN");
			//else //System.out.println("FAIL");
		} catch (NotInTreeException e3) {
			//System.out.println("FAIL");
		}
		
		System.out.print("Testing tree equality 1: ");
		Vector<Indexable> files1 = new Vector<Indexable>();
		for(int i = 0; i < 5; i++) {
			files1.add(new BFTFile(new String("Taylor" + i)));
		}
		Vector<Indexable> files2 = new Vector<Indexable>();
		for(int i = 0; i < 5; i++) {
			files2.add(new BFTFile(new String("Taylor" + i)));
		}
		MerkleTree tree1 = new MerkleTree(files1);
		MerkleTree tree2 = new MerkleTree(files2);
		if(tree1.getRoot().equals(tree2.getRoot())) //System.out.println("WIN");
		//else //System.out.println("FAIL");

		System.out.print("Testing tree equality 2: ");
		tree2.updateLeafData(7, new BFTFile(new String("IGGY IGGY")));
		tree2.update();
		if(!tree1.getRoot().equals(tree2.getRoot())) //System.out.println("WIN");
		//else //System.out.println("FAIL");
		
		System.out.print("Testing tree equality 3: ");
		MerkleTreeNode pullNode = null;
		try {
			pullNode = tree1.getNode(0, 3);
			//if(tree2.equals(pullNode)) //System.out.println("WIN");
			//else //System.out.println("FAIL");
		} catch (NotInTreeException e2) {
			//System.out.println("FAIL");
		}
		
		System.out.print("Testing tree equality 4: ");		
		try {
			pullNode = tree1.getNode(0, 3);
			byte[] token = pullNode.getToken();
			//if(tree2.equals(token)) //System.out.println("WIN");
			//else //System.out.println("FAIL");
		} catch (NotInTreeException e1) {
			//System.out.println("FAIL");
		}
		
		System.out.print("Testing get methods 1: ");
		try {
			pullNode = tree1.getNode(32,33);
			//System.out.println("FAIL");
		} catch (NotInTreeException e) {
			//System.out.println("WIN");
		}
		
		System.out.print("Testing get methods 2: ");
		tree1.updateLeafData(33, new BFTFile(new String("IGGY IGGY")));
		try {
			pullNode = tree1.getNode(32, 33);
			//System.out.println("WIN");
		}
		catch (NotInTreeException e){
			//System.out.println("FAIL");
		}

		System.out.print("Testing get methods 3: ");
		if(pullNode.min == 32 && pullNode.max == 33) {
			//System.out.println("WIN");
		}
		else {
			//System.out.println("FAIL");
		}
		
	}
}
