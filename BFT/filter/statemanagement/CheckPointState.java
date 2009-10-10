// $Id
package BFT.filter.statemanagement;

import BFT.messages.Quorum;
import BFT.messages.Entry;
import BFT.messages.BatchCompleted;

import java.util.Hashtable;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Enumeration;


public class CheckPointState{
    
    //    protected Hashtable<Long, Hashtable<Long, Entry>> batches;
    protected ArrayList<Hashtable<Long, Entry>>  batches;
    //    protected Hashtable<Long, Quorum<BatchCompleted>> quorums;
    protected ArrayList<Quorum<BatchCompleted>> quorums;
    protected long base;
    
    public CheckPointState(long baseSeqNo){
	base = baseSeqNo;
	//	batches = new Hashtable<Long, Hashtable<Long, Entry>>();
	batches = new ArrayList<Hashtable<Long, Entry>>(BFT.Parameters.getNumberOfClients());
	for (int i = 0; i < BFT.Parameters.getNumberOfClients(); i++)
	    batches.add(i, new Hashtable<Long, Entry>());
	//	quorums = new Hashtable<Long, Quorum<BatchCompleted>>();
	quorums = new ArrayList<Quorum<BatchCompleted>>(BFT.order.Parameters.checkPointInterval);
	for (int i = 0; i < BFT.order.Parameters.checkPointInterval; i++)
	    quorums.add(i,
	    //	    quorums.put(baseSeqNo+i, 
			new Quorum<BatchCompleted>(BFT.Parameters.getExecutionCount(),
						   BFT.Parameters.smallExecutionQuorumSize(),
						  0));
    }

    public long getBaseSequenceNumber(){
	return base;
    }

    public long getMaxSeqNo(){
	return base + BFT.order.Parameters.checkPointInterval;
    }

    public Quorum<BatchCompleted> getQuorum(long seqno){
	if (seqno < base)
	    BFT.Debug.kill("too low");
	if (seqno >= getMaxSeqNo())
	    BFT.Debug.kill("too high");
	return quorums.get((int)(seqno-base));
    }
    
    public void addRequest(Entry entry){
	Hashtable<Long, Entry> table = batches.get((int)entry.getClient());
	if (table == null)
	    BFT.Debug.kill("BAD THINGS");
	table.put(entry.getRequestId(), entry);
    }

    public Entry getRequest(long clientId, long requestId){
	Hashtable<Long, Entry> table = batches.get((int)clientId);
	if (table == null)
	    BFT.Debug.kill("BAD THINGS");
	return table.get(requestId);
    }


    public void clear(PriorityQueue<Entry> heap){
	for (int i = 0; i < batches.size(); i++){
	    Hashtable<Long, Entry> k = batches.get(i);
	    for (Enumeration<Entry> f = k.elements(); f.hasMoreElements();)
		heap.remove(f.nextElement());
	    k.clear();
	}

	for (int i = 0;i < quorums.size(); i++){
	    quorums.get(i).clear();
	}
    }
}