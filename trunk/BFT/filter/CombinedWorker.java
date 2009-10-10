/**
 * $Id$
 */
package BFT.filter;

import java.util.*;


import BFT.Debug;
import BFT.order.*;
import BFT.messages.*;
import BFT.network.MessageHandler;
import BFT.network.concurrentNet.NetworkWorkQueue;
import BFT.network.concurrentNet.RPChooser;
import BFT.network.concurrentNet.ReadPredicate;
import BFT.util.*;
import BFT.order.statemanagement.*;

/**
 * @author riche
 *
 */
public class CombinedWorker implements Runnable {

	NetworkWorkQueue netQueue = null;
    FilterBaseNode protocolHandler = null;

	public CombinedWorker(NetworkWorkQueue netQueue, FilterBaseNode protocolHandler) {
		this.netQueue = netQueue;
		this.protocolHandler = protocolHandler;
	}

    /**
       Can probably remove the vast majority of the listeners below
       --- we only care about the server here !
    **/

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		byte[] bytesRead = null;
		int count = 0;
		int spincount = 0;
		boolean spin = true;
		while(true) {
		    spin = true;
		    //		    count++;
// 		    if (count %10000 == 0){
// 			System.out.println("CW spin: "+spincount+"/"+count);
// 			spincount=0;
// 			count=0;
// 		    }
			
			netQueue.hasAnyNetWork();
			if (netQueue.hasNetWork(Role.CLIENT)){
			    bytesRead = netQueue.getWorkRR(Role.CLIENT);
			    if (bytesRead != null) {
				//				spin = false;
				Debug.profileStart("CLIENT_BYTES");
				protocolHandler.handle(bytesRead);
				Debug.profileFinis("CLIENT_BYTES");
				bytesRead = null;
				//continue;
			    }
			    //			    if (spin) System.out.println("client spin");
			}
			if (netQueue.hasNetWork(Role.EXEC)){	
			    bytesRead = netQueue.getWorkRR(Role.EXEC);
			    if (bytesRead != null) {
				//				spin = false;
				Debug.profileStart("EXEC_BYTES");
				protocolHandler.handle(bytesRead);
				Debug.profileFinis("EXEC_BYTES");
				bytesRead = null;
				//continue;
			    }
			    //			    if (spin)  System.out.println("exec spin");
			}
			//			if (spin)
			//spincount++;
			
		}
	}

}
