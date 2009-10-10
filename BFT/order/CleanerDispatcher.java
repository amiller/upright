/**
 * 
 */
package BFT.order;

import BFT.Debug;

import BFT.network.concurrentNet.*;
import BFT.util.Role;
import BFT.messages.*;
import BFT.order.messages.*;

/**
 * @author riche
 *
 */
public class CleanerDispatcher implements Runnable {

    private CleanerWorkQueue cwq = null;
	private Cleaner coreClean = null;
	private OrderBaseNode obn = null;
	private OrderWorkQueue owq = null;

    public CleanerDispatcher(Cleaner coreClean, CleanerWorkQueue cwq, OrderBaseNode obn) {
		this.coreClean = coreClean;
		this.obn = obn;
		this.cwq = cwq;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		byte[] bytes = null;
		SignedRequestCore rc = null;
		PrePrepare pp = null;
		FilteredRequestCore fr = null;
		long blocking = 0;
		long start = 0;
		long finish = 0;
		long getting = 0;
		long total = 0;
		long loopcount = 0;
		long startwork = 0;
		long finishwork=0;
		long totalwork = 0;
		startwork = System.nanoTime();
		boolean spin = false;
		long spincount = 0;
		while(true) {
			// First we look for a pre-prepare to clean
// 		    loopcount++;
// 		    if (loopcount %100000 == 0){
// 			finishwork = System.nanoTime();
// 			totalwork = finishwork - startwork;
// 			System.out.println("Cleaner work : "+totalwork);
// 			System.out.println("Cleaner block: "+blocking+" "+((float)blocking/(float)totalwork));
// 			System.out.println("Cleaner get  : "+getting+" "+((float)getting/(float)totalwork));
// 			System.out.println("spin: "+spincount+"/"+loopcount);
// 			loopcount = 0;
// 			spincount = 0;
// 		    }
// 		    spin = true;
// 		    start = System.nanoTime();
		    cwq.hasCleanerWork();
// 		    finish = System.nanoTime();
// 		    blocking += finish-start;
//		    start = System.nanoTime();
		    pp = cwq.getPrePrepareWork();
		    //		    finish = System.nanoTime();
		    //		    getting += finish - start;
		    if(pp != null) {
			Debug.profileStart("DIRTY_PP");
			coreClean.farm(pp);
			//cwq.addCleanWork(pp);
			pp = null;
			Debug.profileFinis("DIRTY_PP");
			continue;
		    }
		    // now we look for a request core to clean
		    //		    start = System.nanoTime();
		    rc = cwq.getRequestCoreWork();
		    //		    finish = System.nanoTime();
		    //		    getting += finish - start;
		    if(rc != null) {
			Debug.profileStart("DIRTY_RC");
			coreClean.farm(rc);
			//cwq.addCleanWork(rc);
			rc = null;
			Debug.profileFinis("DIRTY_RC");
			//continue;
		    }
		    //			pp = cwq.getDonePP();
		    //			if(pp != null) {
		    //			    Debug.profileStart("CLEANED_PP");
		    //				System.err.println("Got the cleaned PP from the farmer, Qing to worker");
		    //				cwq.addCleanWork(pp);
		    //				pp = null;
		    //				Debug.profileFinis("CLEANED_PP");
		    //				//continue;
		    //			}
		    
		    // look for client messages next if not using filters
		    if (!BFT.Parameters.filtered && cwq.hasNetWork(Role.CLIENT)){
			
			//			start = System.nanoTime();
			bytes = cwq.getWorkRR(Role.CLIENT);
			//			finish = System.nanoTime();
			//			getting += finish - start;
			if(bytes != null) {
			    Debug.profileStart("CLIENT_WORK");
			    obn.handle(bytes);
			    bytes = null;
			    
			    Debug.profileFinis("CLIENT_WORK");
			    //continue;
			}
		    }
		    // look for filtered messages if using filters
		    if (BFT.Parameters.filtered 
			&& (cwq.hasFilteredWork() ||cwq.hasNetWork(Role.FILTER))){
			// 			    FilteredRequest req = cwq.getFilteredWork();
			// 			    if (req != null){
			// 				Debug.profileStart("FILTER_WORK");
			// 				obn.handle(req);
			// 				Debug.profileFinis("FILTER_WORK");
			// 			    }
			
			//			start = System.nanoTime();
			fr = cwq.getFilteredWorkRR2();
			//			finish = System.nanoTime();
			//			getting += finish - start;
			if(fr != null) {
			    spin = false;
			    Debug.profileStart("FILTER_WORK");
			    obn.handle(fr);
			    bytes = null;
			    Debug.profileFinis("FILTER_WORK");
			    //continue;
			}
		    }
		    
		    //			rc = cwq.getDoneRC();
		    //			if(rc != null) {
		    //			    Debug.profileStart("CLEANED_RC");
		    //				cwq.addCleanWork(rc);
		    //				rc = null;
		    //				Debug.profileFinis("CLEANED_RC");
		    //				//continue;
		    //			}
		    //		    if (spin) spincount++;
		}
	}
    
}
