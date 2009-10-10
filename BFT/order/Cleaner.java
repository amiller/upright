
// $Id$

package BFT.order;

import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.*;
import java.util.concurrent.*;
import java.util.Vector;

import BFT.membership.*;
import BFT.order.messages.PrePrepare;
import BFT.messages.Digest;
import BFT.messages.SignedRequestCore;
import BFT.messages.RequestCore;
import BFT.messages.FilteredRequestCore;
import BFT.messages.MessageTags;
import BFT.Debug;
import BFT.order.statemanagement.RequestQueue;
import BFT.network.concurrentNet.*;
import BFT.util.*;

public class Cleaner{
	private OrderWorkQueue outgoing = null;
    private CleanerWorkQueue incoming =null;
	//private int[] lastClientReq = null;
	//private ExecutorService pool = null;
	private Membership members;
	private Vector<Vector<Pair<Integer, Digest>>> lastClientReq = null;

    public Cleaner(CleanerWorkQueue in, OrderWorkQueue out, Membership m){
	this.incoming = in;
	this.outgoing = out;
		//lastClientReq = new int[BFT.Parameters.getNumberOfClients()];
		//pool = Executors.newCachedThreadPool();
		//pool = new ThreadPoolExecutor(0, 2, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1024));
		members = m;
		lastClientReq = new Vector<Vector<Pair<Integer, Digest>>>(BFT.Parameters.getNumberOfClients());
		Vector<Pair<Integer, Digest>> v;
		for(int i = 0; i < BFT.Parameters.getNumberOfClients(); i++) {
			v = new Vector<Pair<Integer, Digest>>((BFT.order.Parameters.checkPointInterval));
			lastClientReq.add(i, v);
			Pair<Integer, Digest> p = null;
			for(int j = 0; j < BFT.order.Parameters.checkPointInterval; j++) {
				//byte[] test = new String("Test").getBytes();
				//p = new Pair<Integer, Digest>(new Integer(0), new Digest(test));
				v.add(j, p);
			}
		}
	}

	/** Functions to get the public keys for specific nodes **/
	public RSAPublicKey getClientPublicKey(int id){ 
		return members.getClientNodes()[id].getPublicKey();
	}
	public RSAPublicKey getOrderReplicaPublicKey(int id){ 
		return members.getOrderNodes()[id].getPublicKey();
	}
	public RSAPublicKey getExecutionReplicaPublicKey(int id){ 
		return members.getExecNodes()[id].getPublicKey();
	}

	/** Functions to get my private key **/

	public RSAPrivateKey getMyPrivateKey(){return members.getMyPrivateKey();}
	public RSAPublicKey getMyPublicKey() {return members.getMyPublicKey();}



    public void clean(PrePrepare p){
	//Debug.println("\t\t\t\t\tCleaner.cleanPrePrepare");
	incoming.addWork(p);
    }

    public void clean(RequestCore rc){
	if (rc.getTag() == MessageTags.SignedRequestCore)
	    _clean((SignedRequestCore) rc);
	else
	    Debug.kill("Only clean SignedRequestCores!: "+rc.getTag());
    }

    protected void _clean(SignedRequestCore rc){
	//Debug.println("\t\t\t\t\tCleaner.cleanRequestCore");
	incoming.addWork(rc);
		//	reqQueue.addRequestCore(rc);
		//	if (reqQueue.size() > 0 && bn.canCreateBatch()){
		//	    RequestCore[] reqs = reqQueue.getRequestCores();
		//	    bn.handle(reqs);
		//	}
    }

    
    public boolean hasBeenCleaned(RequestCore rc) {
	boolean retVal = false;
	Pair<Integer, Digest> p = lastClientReq.get((int)rc.getSendingClient()).get((int)rc.getRequestId() % BFT.order.Parameters.checkPointInterval);
	//Digest d = lastClientReq[(int)rc.getSendingClient()][(int)rc.getRequestId() % BFT.order.Parameters.checkPointInterval];
	if(p != null) {
	    Digest d = p.getRight();
	    Integer sn = p.getLeft();
	    Digest newD = new Digest(rc.getBytes());
	    if(newD.equals(d)) {
		retVal = true;
	    }
	    else if (sn == (int)rc.getRequestId()) {
		Debug.println(rc);
		BFT.Debug.kill(new RuntimeException("Sequence number sending different requests! Somebody is being bad!"));
	    }
	}
	return retVal;
    }
	
	public void farm(SignedRequestCore rc){
		CleanerWork cw = new CleanerWork(rc, this);
		//		Debug.profileStart("FARM_RC");
		//pool.execute(cw);
		cw.run();
		//		Debug.profileFinis("FARM_RC");
	}
	
	public void farm(PrePrepare pp){
		CleanerWork cw = null;
		//		System.err.println("\tpreparing to FARM: " + pp.getRequestBatch().getEntries().length);
		for(int i = 0; i < pp.getRequestBatch().getEntries().length; i++) {
			if(i < pp.getRequestBatch().getEntries().length - 1) {
			    // AGC: casting the parameter to a signed
			    // request core
			    cw = new CleanerWork(pp, (SignedRequestCore)(pp.getRequestBatch().getEntries()[i]), false, this);
				//pool.execute(cw);
			    cw.run();
			}
			else {
			    // AGC: casting the parameter to a signed
			    // request core
			    cw = new CleanerWork(pp, (SignedRequestCore)(pp.getRequestBatch().getEntries()[i]), true, this);
				//pool.execute(cw);
			    cw.run();
			}
		}
	}
	
	public void done(PrePrepare pp) {
	    outgoing.addCleanWork(pp);
	    //		owq.cleanerDone(pp);
	}	
	
	public void done(SignedRequestCore rc) {
	    outgoing.addCleanWork(rc);
	    //owq.cleanerDone(rc);
	}
	
	public void updateHash(SignedRequestCore rc) {
		Digest d = new Digest(rc.getBytes());
		Integer sn = (int)rc.getRequestId();
		Pair<Integer, Digest> newP = new Pair<Integer, Digest>(sn, d);
		lastClientReq.get((int)rc.getSendingClient()).add((int)rc.getRequestId() % BFT.order.Parameters.checkPointInterval, newP);
	}
	
    private class CleanerWork implements Runnable {
		
	private final PrePrepare pp;
	private final SignedRequestCore rc;
	private final boolean isThisMaster;
	private final Cleaner cleaner;
		
	public CleanerWork(PrePrepare pp, SignedRequestCore rc, boolean master, Cleaner cleaner) {
	    this.pp = pp;
	    this.rc = rc;
	    this.isThisMaster = master;
	    this.cleaner = cleaner; 
	}
		
	public CleanerWork(SignedRequestCore rc, Cleaner cleaner) {
	    this.rc = rc;
	    this.pp = null;
	    this.isThisMaster = false;
	    this.cleaner = 	cleaner;
	}

	public void run() {
	    Debug.profileStart("CLEAN_WORK");
	    if(pp == null && rc != null) {
		long cid = rc.getSendingClient();
		// TODO : this could possibly be bad, but at least I know about the long->int cast
		//System.out.println("CID: " + (int)cid);
		if(!cleaner.hasBeenCleaned(this.rc)) {
		    RSAPublicKey pubkey = cleaner.getClientPublicKey((int)cid);
		    if(rc.verifySignature(pubkey)) {
			cleaner.updateHash(this.rc);
			cleaner.done(this.rc);
		    }
		    else {
			BFT.Debug.kill(new RuntimeException("Bad signature! from client "+cid));
		    }
		}
		else {
		    cleaner.done(this.rc);
		}
	    }
	    else if(pp != null ){
		if(isThisMaster) {
		    long cid = rc.getSendingClient();
		    // TODO : this could possibly be bad, but at least I know about the long->int cast
		    if(!cleaner.hasBeenCleaned(this.rc)) {
			RSAPublicKey pubkey = cleaner.getClientPublicKey((int)cid);
			if(rc.verifySignature(pubkey)) {
			    cleaner.updateHash(rc);
			    pp.rcMasterCleaned();
			    cleaner.done(pp);
			}
			else {
			    BFT.Debug.kill(new RuntimeException("Bad signature!"));
			}
		    }
		    else {
			//BFT.//Debug.println("SAVED WORK!");
			pp.rcMasterCleaned();
			cleaner.done(pp);
		    }
		}
		else {
		    //Debug.println("about to do slave clean");
		    long cid = rc.getSendingClient();
		    // TODO : this could possibly be bad, but at least I know about the long->int cast
		    if(!cleaner.hasBeenCleaned(this.rc)) {
			RSAPublicKey pubkey = cleaner.getClientPublicKey((int)cid);
			if(rc.verifySignature(pubkey)) {
			    cleaner.updateHash(rc);
			    pp.rcCleaned();
			}
			else {
			    BFT.Debug.kill(new RuntimeException("Bad signature!"));
			}
		    }
		    else {
			//BFT.//Debug.println("SAVED WORK!");
			pp.rcCleaned();
		    }
		}
	    }
	    else {
		throw new RuntimeException("Should not be here");
	    }
	    Debug.profileFinis("CLEAN_WORK");
	}
	
    }

}

