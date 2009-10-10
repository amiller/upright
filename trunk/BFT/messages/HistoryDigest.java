// $Id$

package BFT.messages;

import BFT.messages.Digest;
import BFT.messages.CommandBatch;
import BFT.messages.NonDeterminism;

public class HistoryDigest extends Digest{


    public HistoryDigest(HistoryDigest d, CommandBatch b, NonDeterminism n,
			 Digest cp){
	super(combineArrays(combineArrays(d.getBytes(), 
					  b.getBytes()), 
			    combineArrays(n.getBytes(), 
					  cp.getBytes())
			    )
	      );
    }

    /**
       Constructor that takes the previous history digest and the next
       command batch and creates a new history digest.
     **/
    public HistoryDigest(HistoryDigest d, CommandBatch b, NonDeterminism n){
	super(combineArrays(combineArrays(d.getBytes(), 
					  b.getBytes()), 
			    n.getBytes()));
    }

    public HistoryDigest(){
	super();
    }

    public static HistoryDigest fromBytes(byte[] bits){
	HistoryDigest d = new HistoryDigest();
	d.bytes = new byte[bits.length];
	for (int i = 0;i < bits.length; i++)
	    d.bytes[i] = bits[i];
	return d;
    }
    
    public HistoryDigest(byte[] b){
	super(b);
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

}