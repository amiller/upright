// $Id$

package BFT.order.statemanagement.viewchangetree;

import BFT.messages.Digest;
import BFT.messages.HistoryDigest;

import BFT.order.messages.ViewChange;

import java.util.Vector;
import java.util.Iterator;
import java.lang.Comparable;

public class TreeRoot implements Comparable<TreeRoot>{
    protected Digest dig;
    protected long seqno;
    protected boolean[] committedSupport;
    protected int committedSupportCount;
    protected boolean[] stableSupport;
    protected int stableSupportCount;

    protected Vector<TreeNode> children;

    public TreeRoot(ViewChange vc, boolean com){
	stableSupport = new boolean[BFT.Parameters.getOrderCount()];
	committedSupport = new boolean[BFT.Parameters.getOrderCount()];
	stableSupportCount = 0;
	committedSupportCount = 0;

	for (int i = 0; i < committedSupport.length; i++){
	    stableSupport[i] = false;
	    committedSupport[i] = false;
	}	

	if (com){
	    //System.out.println("creating a committed root");
	    seqno = vc.getCommittedCPSeqNo();
	    dig = vc.getCommittedCPHash();
	    addCommittedSupport((int)(vc.getSendingReplica()));
	}else{
	    //System.out.println("creating astable root");
	    seqno = vc.getStableCPSeqNo();
	    dig = vc.getStableCPHash();
	    addStableSupport((int)(vc.getSendingReplica()));
	}

	children = new Vector<TreeNode>();
    }

    public Digest getDigest(){
	return dig;
    }

    public long getSequenceNumber(){
	return seqno;
    }

    public void addCommittedSupport(int replica){
	if (!committedSupport[replica]){
	    committedSupport[replica] = true;
	    committedSupportCount++;
	}
    }

   public void addStableSupport(int replica){
	if (!stableSupport[replica]){
	    stableSupport[replica] = true;
	    stableSupportCount++;
	}
    }

    public TreeNode getChild(Digest d, HistoryDigest h){
	Iterator<TreeNode> it = children.iterator();
	boolean found = false;
	TreeNode tmp = null;
	while(it.hasNext() && !found){
	    tmp = it.next();
	    if (tmp.getHistory().equals(h) && tmp.getDigest().equals(d)){
		found = true;
	    }
	}
	
	if (!found){
	    tmp = new TreeNode(d,h,seqno);
	    children.add(tmp);
	}
	
	return tmp;
    }


    public Iterator<TreeNode> getChildren(){
	return children.iterator();
    }
    
    public int getStableSupportCount(){
	return stableSupportCount;
    }

    public boolean[] getStableSupport(){
	return stableSupport;
    }

    public int getCommittedSupportCount(){
	return committedSupportCount;
    }

    public boolean[] getCommittedSupport(){
	return committedSupport;
    }


    public boolean matchesCommitted(ViewChange vc){
	return vc.getCommittedCPSeqNo() == seqno &&
	    getDigest().equals(vc.getCommittedCPHash());
    }

    public boolean matchesStable(ViewChange vc){
	return vc.getStableCPSeqNo() == seqno &&
	    getDigest().equals(vc.getStableCPHash());
    }


    public int compareTo(TreeRoot i2){
	int out = (int) (getSequenceNumber() - i2.getSequenceNumber());
	return out;
    }


    public String toString(){
	String str = "Root("+seqno+", "+getCommittedSupportCount()+"[";
	for (int i = 0; i < committedSupport.length; i++)
	    str = str+(committedSupport[i]?" "+ i:" -");
	str = str+"], "+getStableSupportCount()+" [";
	for (int i = 0; i < stableSupport.length; i++)
	    str = str+(stableSupport[i]?" "+i:" -");
	str = str+")\n";
	Iterator<TreeNode> it = children.iterator();
	int count = 0;
	while (it.hasNext()){
	    TreeNode tmp = it.next();
	    str = str+tmp.print(" "+count);
	    count++;
	}
	return str;
    }


}
