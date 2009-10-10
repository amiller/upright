// $Id$

package BFT.network.concurrentNet;

import BFT.messages.ClientRequest;
import BFT.messages.VerifiedMessageBase;
import BFT.order.MessageFactory;
import BFT.order.messages.MessageTags;
import BFT.util.*;
import java.util.*;

public class Listener implements Runnable {

    private ConcurrentNetwork net;
    private NetQueue queue;

    Role id;

    static int count= 0;
	
    public Listener(ConcurrentNetwork net, NetQueue queue) {
	this.net = net;
	this.queue = queue;
	id = net.getMyRole();
    }
	
    public void run() {
	Queue<Pair<Integer, byte[]> > msgBytes = null;
	net.waitForListening();
	long total = 0;
	long select = 0;
	long poll = 0;
	long start = 0;
	long finish = 0;
	long starttotal = 0;
	long finishtotal = 0;
	long count = 0;
	while(true) {
// 	    count++;
// 	    if (count %10000 == 0){
// 		finishtotal = System.nanoTime();
// 		total = finishtotal - starttotal;
// 		System.out.println(id +" total : "+ total);
// 		System.out.println(id +" poll  : "+poll + " "+((float)poll/(float)total));
// 		System.out.println(id +" select: "+select +" "+((float)select/(float)total));
// 		select = 0;
// 		poll = 0;
// 		starttotal = System.nanoTime();
// 		count = 0;
// 	    }
// 	    start = System.nanoTime();
	    msgBytes = net.select();
	    //	    finish = System.nanoTime();
	    //	    select += finish - start;
	    if(msgBytes != null) {
		while(!msgBytes.isEmpty()) {
		    //		    start = System.nanoTime();
		    Pair<Integer, byte[]> p = msgBytes.poll();
		    //		    finish = System.nanoTime();
		    //		    poll += finish - start;
		    queue.addWork(net.getMyRole(), p.getLeft(), p.getRight());
		}
		msgBytes = null;
	    }
	}

    }

}
