// $Id$

package BFT;


public class Parameters{

    public static long maxRequestSize = 60000;

    public static int toleratedOrderCrashes=0;
    public static int toleratedOrderLiars=0;
    public static int toleratedExecutionCrashes=0;
    public static int toleratedExecutionLiars = 0;
    public static int toleratedFilterCrashes = 0;
    public static int toleratedFilterLiars = 0;
    public static int numberOfClients=0;
    public static int concurrentRequests=1;


    public static boolean filtered = false;
    public static boolean fair = false;
    
    public static boolean debug=false;

    public static boolean filterCaching = true;
    public static boolean doLogging = true;
    
    public static boolean insecure=false;
    public static boolean cheapClients = false;
    
    public static boolean blockingSends = true;
    
    public static boolean linearizeReads = false;


    public static String provider="FlexiCore";
    //public static String provider="SunJCE";
    //public static String provider="";


    public static int getConcurrentRequestLimit(){
	return concurrentRequests;
    }
            
    public static int getExecutionCount(){
	return toleratedExecutionCrashes
	    + max(toleratedExecutionLiars, 
		  toleratedExecutionCrashes)
	    + 1;
    }

    public static int getExecutionLiars(){
	return toleratedExecutionLiars;
    }
    
    public static int getExecutionCrashes(){
	return toleratedExecutionCrashes;
    }

    public static int getOrderCrashes(){
	return toleratedOrderCrashes;
    }

    public static int getOrderLiars(){
	return toleratedOrderLiars;
    }
    
    public static int getOrderCount(){
	return 2 * toleratedOrderCrashes + toleratedOrderLiars + 1;
    }

    public static int getNumberOfClients(){
    return numberOfClients;
    }

    public static int getFilterLiars(){
	return toleratedFilterLiars;
    }

    public static int getFilterCrashes(){
	return toleratedFilterCrashes;
    }

    public static int getFilterCount(){
	//	int v1 = 3*getFilterLiars() + getFilterCrashes()+1;
	//      int v2 = 0; //2*getFilterLiars() + 2*getFilterCrashes()+1;
	return 
	    filtered?(fair?(1+getFilterCrashes() + 
			    2*max(getFilterCrashes(),getFilterLiars())+
			    getFilterLiars())
// 		      :(1+min(getFilterCrashes(), getFilterLiars()) +  
// 			2*max(getFilterCrashes(), getFilterLiars())))
		      :(1 + max(getFilterCrashes(), getFilterLiars())+
			getFilterLiars() + getFilterCrashes()))
	    :0;
	
// 	return filtered?(fair?(3*max(getFilterCrashes(), getFilterLiars())+
// 			       min(getFilterLiars(), getFilterCrashes()))
// 			 :2*max(getFilterCrashes(), getFilterLiars())+
// 			 getFilterLiars()):0;
	//	return filtered?((v1>v2)?v1:v2):0;
    }

    public static int max(int a, int b){
	if (a > b)
	    return a;
	else
	    return b;
    }

    public static int min(int a, int b){
	if (a < b)
	    return a;
	else
	    return b;
    }
    
    
   public final static int rightExecutionQuorumSize(){
       return getExecutionLiars() + 1;
    }

    public final static int upExecutionQuorumSize(){
	return getExecutionCrashes()+1;
    }

    public final static int smallExecutionQuorumSize(){
	return max(rightExecutionQuorumSize(), upExecutionQuorumSize());
    }

    public final static int linearizedExecutionQuorumSize(){
	//	BFT.Debug.kill("I DONT THINK THIS QUORUM SIZE WORKS PROPERLY");
	return getExecutionCount() - getExecutionLiars();
    }

    public final static int largeExecutionQuorumSize(){
	 return getExecutionCount() - getExecutionCrashes();
     }

    public final static int smallOrderQuorumSize(){
	//return (getOrderCount() - getOrderCrashes())/2+1;
	return getOrderLiars() + 1;
	//return (getOrderCount() + getOrderLiars())/2+1;
    }


    public final static int largeOrderQuorumSize(){
	return getOrderCount() - getOrderCrashes();
    }

    public final static int fastOrderQuorumSize(){
	return getOrderCount();
    }

    public final static int smallFilterQuorumSize(){
	if (getFilterLiars() > getFilterCrashes())
	    return getFilterLiars()+1;
	else
	    return getFilterCrashes()+1;
    }

    public final static int mediumFilterQuorumSize(){
	return smallFilterQuorumSize() + getFilterLiars();
    }

    public final static int largeFilterQuorumSize(){
	return mediumFilterQuorumSize() + getFilterCrashes();
    }

    public boolean checkId(String role, int id) {
        int maxId = 0;
        if (role.equalsIgnoreCase("exec")) {
            maxId = getExecutionCount() - 1;
        } else if (role.equalsIgnoreCase("order")) {
            maxId = getOrderCount() - 1;
        } else if (role.equalsIgnoreCase("order")) {
            maxId = getNumberOfClients() - 1;
        }
        return (id>=0 && id<=maxId);
    }

    public static void println(Object obj) {
        if(debug) {
            //System.out.println(obj);
        }
    }
    
    public static void print(String str) {
        if(debug) {
            System.out.print(str);
        }
    }

}
