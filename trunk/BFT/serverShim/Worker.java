/**
 * $Id$
 */
package BFT.serverShim;

import java.util.*;


import BFT.Debug;
import BFT.order.*;
import BFT.messages.*;
import BFT.order.messages.*;
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
public class Worker implements Runnable {

	NetworkWorkQueue netQueue = null;
	ShimBaseNode protocolHandler = null;

	public Worker(NetworkWorkQueue netQueue, ShimBaseNode protocolHandler) {
		this.netQueue = netQueue;
		this.protocolHandler = protocolHandler;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
 	 */
	public void run() {
		byte[] bytesRead = null;
		int count = 0;
		while(true) {
			netQueue.hasAnyNetWork();
			count = 0;
			while(netQueue.hasNetWork(Role.EXEC)
			      && count < 1 ){//BFT.Parameters.getExecutionCount()){
			    bytesRead = netQueue.getWorkRR(Role.EXEC);
			    count++;
			    if (bytesRead != null) {
				Debug.profileStart("EXEC_BYTES");
				protocolHandler.handle(bytesRead);
				Debug.profileFinis("EXEC_BYTES");
				bytesRead = null;
				//continue;
			    }
			}
			count = 0;
			while (netQueue.hasNetWork(Role.ORDER)
			       && count < 1){//BFT.Parameters.getOrderCount() + BFT.Parameters.getOrderCount()){
			    bytesRead = netQueue.getWorkRR(Role.ORDER);
			    count++;
			    if (bytesRead != null) {
				Debug.profileStart("ORDER_BYTES");
				protocolHandler.handle(bytesRead);
				Debug.profileFinis("ORDER_BYTES");
				bytesRead = null;
				//continue;
			    }
			}
			count = 0;
			while(BFT.Parameters.filtered 
			      && netQueue.hasNetWork(Role.FILTER)
			      && count < 1){//BFT.Parameters.getFilterCount()+BFT.Parameters.getExecutionCount() + BFT.Parameters.getOrderCount()) {
				bytesRead = netQueue.getWorkRR(Role.FILTER);
				count++;
				if (bytesRead != null) {
				    Debug.profileStart("FILTER_BYTES");
				    protocolHandler.handle(bytesRead);
				    Debug.profileFinis("FILTER_BYTES");
				    bytesRead = null;
				    //continue;
				}
			}
			count = 0;
			while (netQueue.hasNetWork(Role.CLIENT)
			       && count < 1){//(BFT.Parameters.getNumberOfClients()+1+BFT.Parameters.getFilterCount())/(1+BFT.Parameters.getFilterCount())){
			    bytesRead = netQueue.getWorkRR(Role.CLIENT);
			    count++;
			    if (bytesRead != null) {
				Debug.profileStart("CLIENT_BYTES");
				protocolHandler.handle(bytesRead);
				Debug.profileFinis("CLIENT_BYTES");
				bytesRead = null;
				//continue;
			    }
			}
	
		}
	}

}
