// $Id$
package BFT.membership;

import BFT.Debug;
import BFT.Parameters;
import BFT.util.Role;

import java.util.Properties;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.security.*;
import java.security.interfaces.*;
import javax.crypto.*;
import javax.crypto.spec.*;

public class Membership {

    Role role;
    int id;  
    String myPrivateKeyString;      
    String myPublicKeyString;
    RSAPublicKey myPublicKey;
    RSAPrivateKey myPrivateKey;
    Principal[] filterNodes;
    Principal[] orderNodes;
    Principal[] execNodes;
    Principal[] clientNodes;  
    Principal[] myInterfaces;
    // TLR 2009.1.25: Support for different threads handling the network interaction with each role
    private Principal[] myFilterInterfaces;
    private Principal[] myOrderInterfaces;
    private Principal[] myExecInterfaces;
    private Principal[] myClientInterfaces;
    private Mac myMac;
    
    /**
     * This constructor receives 3 arguments:
     * configFilename : path to the config file
     * role : can be in the set {order, exec, client}
     * id : a number from 1 to k, where k is the maximum id possible for that role
     */
    public Membership(String configFilename, Role _role, int _id) {
	Security.addProvider(new de.flexiprovider.core.FlexiCoreProvider());

        role = _role;
        id = _id;

        Properties privKeyProp = new Properties();
        try {
	    privKeyProp.load(new FileInputStream("./keys/" + role.toString() + id + ".privk.properties"));
	} catch (FileNotFoundException e1) {
	    e1.printStackTrace();
	} catch (IOException e1) {
	    e1.printStackTrace();
	}
	myPrivateKeyString = privKeyProp.getProperty("PRIV");
	
        // Read properties file.
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFilename));
        } catch (IOException e) {
            e.printStackTrace();        
        }
        
	

        Parameters.toleratedOrderCrashes = 
	    Integer.parseInt(properties.getProperty("orderCrashFailures"));
        Parameters.toleratedOrderLiars = 
	    Integer.parseInt(properties.getProperty("orderLiarFailures"));
        Parameters.toleratedExecutionCrashes =
	    Integer.parseInt(properties.getProperty("execCrashFailures"));
        Parameters.toleratedExecutionLiars = 
	    Integer.parseInt(properties.getProperty("execLiarFailures"));


	if (properties.getProperty("filtered") != null)
	    Parameters.filtered = 
		Boolean.parseBoolean(properties.getProperty("filtered"));
	if (Parameters.filtered){
	    Parameters.toleratedFilterCrashes = 
		Integer.parseInt(properties.getProperty("filterCrashFailures"));
	    Parameters.toleratedFilterLiars = 
		Integer.parseInt(properties.getProperty("filterLiarFailures"));
	}
	if (properties.getProperty("fair") != null){
	    Parameters.fair =
		Boolean.parseBoolean(properties.getProperty("fair"));
	}
	if (properties.getProperty("insecure") != null)
	    Parameters.insecure = 
		Boolean.parseBoolean(properties.getProperty("insecure"));
	if (properties.getProperty("cheapClients") != null)
	    Parameters.cheapClients = 
		Boolean.parseBoolean(properties.getProperty("cheapClients"));

	if (properties.getProperty("doLogging") != null)
	    Parameters.doLogging = 
		Boolean.parseBoolean(properties.getProperty("doLogging"));
	if (properties.getProperty("filterCaching") != null)
	    Parameters.filterCaching = 
		Boolean.parseBoolean(properties.getProperty("filterCaching"));
	if (properties.getProperty("linearizeReads") != null)
	    Parameters.linearizeReads =
		Boolean.parseBoolean(properties.getProperty("linearizeReads"));


	if (Parameters.getFilterLiars() == 0 && Parameters.getOrderLiars()==0)
	    Parameters.insecure = true;
	

        Parameters.numberOfClients = 
	    Integer.parseInt(properties.getProperty("clientCount"));


	System.out.println("linearizeReads: "+Parameters.linearizeReads);
	System.out.println("cheapClients: "+Parameters.cheapClients);
	System.out.println("filterCaching: "+Parameters.filterCaching);
	System.out.println("doLogging: "+Parameters.doLogging);
	System.out.println("insecure: "+Parameters.insecure);
	System.out.println("filtered: "+Parameters.filtered);
	System.out.println("fair: "+ Parameters.fair);
	System.out.println("filter:"+Parameters.getFilterCrashes()+"+"+
			   Parameters.getFilterLiars()+"="+
			   Parameters.getFilterCount());
	System.out.println("order: "+Parameters.getOrderCount());
	System.out.println("exec:  "+Parameters.getExecutionCount());
	

        ////System.out.println("First point");
        int offset = 0; // tells you from which column to read

        if (role.equals(Role.CLIENT)) {
            CheckIdWithinRange(Parameters.getNumberOfClients());
            offset = 0;
        } else if (role.equals(Role.FILTER)){
	    CheckIdWithinRange(Parameters.getFilterCount());
	    offset = 1 + (id);
	}else if (role.equals(Role.ORDER)) {
            CheckIdWithinRange(Parameters.getOrderCount());
            offset = 1 // number of client entries
		+Parameters.getFilterCount() // number of filter entries
		+(id); // ofset into the order entries
        } else if (role.equals(Role.EXEC)) {
            CheckIdWithinRange(Parameters.getExecutionCount());
            offset = (1)  // number of client entries;
		+ Parameters.getFilterCount() // number of filter entries
		+ Parameters.getOrderCount() // number of order entries
		+(id); // offset into the exec entries
        } else {
            System.err.println("Unknown role while parsing properties file");
            System.exit(0);
        }

	int interfaceCount = 1 + 
	    Parameters.getFilterCount()+
	    Parameters.getOrderCount()+
	    Parameters.getExecutionCount();

        orderNodes = new Principal[Parameters.getOrderCount()];
        execNodes = new Principal[Parameters.getExecutionCount()];
        clientNodes = new Principal[Parameters.getNumberOfClients()];
        myInterfaces = new Principal[interfaceCount];
	if (Parameters.filtered)
	    filterNodes = new Principal[Parameters.getFilterCount()];
	else
	    filterNodes = new Principal[0];

	//System.out.println("number of filter nodes "+Parameters.getFilterCount());

	System.out.println("filter:  "+Parameters.getFilterCount());
	for(int i=0;i<Parameters.getFilterCount();i++) {
            String value = properties.getProperty(Role.FILTER+"."+i);
            String[] split = value.split(" ", 0);
            filterNodes[i] = new Principal(split[offset], 
					   split[interfaceCount]);
        }	

        ////System.out.println("Second point"); 
	System.out.println("order: "+Parameters.getOrderCount());
        for(int i=0;i<Parameters.getOrderCount();i++) {
            String value = properties.getProperty(Role.ORDER+"."+i);
            String[] split = value.split(" ", 0);
            orderNodes[i] = new Principal(split[offset], 
					  split[interfaceCount]);
        }



        for(int i=0;i<Parameters.getExecutionCount();i++) {
            String value = properties.getProperty(Role.EXEC+"."+i);
            String[] split = value.split(" ", 0);
            execNodes[i] = new Principal(split[offset], split[interfaceCount]);
        }

        for(int i=0;i<Parameters.getNumberOfClients();i++) {
            String value = properties.getProperty(Role.CLIENT+"."+i);
            String[] split = value.split(" ", 0);
            clientNodes[i] = new Principal(split[offset], split[interfaceCount]);
        }        
        
        ////System.out.println("Third point");        
        String value = properties.getProperty(role+"."+id);
        String[] split = value.split(" ", 0);
        for(int i=0;i<split.length-1;i++) {     // don't include the private key, just the IP:port pairs
            myInterfaces[i] = new Principal(split[i], split[split.length-1]);
        }
        ////System.out.println("First point");
        
        myPublicKeyString = split[split.length-1];
        myPublicKey = BFT.util.KeyGen.getPubKeyFromString(myPublicKeyString);
        myPrivateKey = BFT.util.KeyGen.getPrivKeyFromString(myPrivateKeyString);

        
        try {
	    privKeyProp = new Properties();
	    for (int i = 0; i < clientNodes.length; i++) {
		privKeyProp.load(new FileInputStream("./keys/" + Role.CLIENT.toString() + i + ".privk.properties"));
		clientNodes[i].setMacKey(BFT.util.KeyGen.getMacObjectfromString(privKeyProp.getProperty("SECRET")));
	    }
	    for(int i = 0; i < filterNodes.length; i++) {
		privKeyProp.load(new FileInputStream("./keys/" + Role.FILTER.toString() + i + ".privk.properties"));
		filterNodes[i].setMacKey(BFT.util.KeyGen.getMacObjectfromString(privKeyProp.getProperty("SECRET")));
	    }
	    for(int i = 0; i < orderNodes.length; i++) {
		privKeyProp.load(new FileInputStream("./keys/" + Role.ORDER.toString() + i + ".privk.properties"));
		orderNodes[i].setMacKey(BFT.util.KeyGen.getMacObjectfromString(privKeyProp.getProperty("SECRET")));
	    }
	    for(int i = 0; i < execNodes.length; i++) {
		privKeyProp.load(new FileInputStream("./keys/" + Role.EXEC.toString() + i + ".privk.properties"));
		execNodes[i].setMacKey(BFT.util.KeyGen.getMacObjectfromString(privKeyProp.getProperty("SECRET")));
	    }
	    privKeyProp.load(new FileInputStream("./keys/" + role.toString() + id + ".privk.properties"));
	    myMac = BFT.util.KeyGen.getMacObjectfromString(privKeyProp.getProperty("SECRET"));
	} catch (FileNotFoundException e1) {
	    e1.printStackTrace();
	} catch (IOException e1) {
	    e1.printStackTrace();
	}
        
        
        // TLR 2009.1.25: Support for different threads handling the network interaction with each role
        value = properties.getProperty(role + "." + id);
        split = value.split(" ", 0);
        myOrderInterfaces = new Principal[Parameters.getOrderCount()];
        myExecInterfaces = new Principal[Parameters.getExecutionCount()];

        myClientInterfaces = new Principal[1];
        myClientInterfaces[0] = new Principal(split[0], split[split.length-1]); 
        for(int i = 0; i < Parameters.getOrderCount(); i++) {
        	myOrderInterfaces[i] = 
		    new Principal(split[1 +
					Parameters.getFilterCount() +
					i ], 
				  split[split.length -1]); 
        }
        for(int i = 0; i < Parameters.getExecutionCount(); i++) {
        	myExecInterfaces[i] = 
		    new Principal(split[1 +
					Parameters.getFilterCount() +
					Parameters.getOrderCount() 
					+ i], 
				  split[split.length - 1]);
        }
	if (Parameters.filtered)
	    myFilterInterfaces = new Principal[Parameters.getFilterCount()];
	else 
	    myFilterInterfaces = new Principal[0];
	for (int i = 0; i < Parameters.getFilterCount(); i++)
	    myFilterInterfaces[i] = new Principal(split[1+i], 
						  split[split.length-1]);
	
        
    }

    public void CheckIdWithinRange(int range) {
        if(id<0 || id>=range) {
            //System.out.println("Id is: "+id+". Should be between >=0 and <"+range);
            System.exit(0);
        }
    }

    public Principal[] getFilterNodes(){
	return filterNodes;
    }
    
    public Principal[] getOrderNodes() {
        return orderNodes;
    }

    public Principal[] getExecNodes() {
        return execNodes;
    }

    public Principal[] getClientNodes() {
        return clientNodes;
    }
    
    public Principal[] getMyInterfaces() {
        return myInterfaces;
    }

    public RSAPublicKey getMyPublicKey() {
        return myPublicKey;
    }

    public RSAPrivateKey getMyPrivateKey() {
        return myPrivateKey;
    }
    
    public Mac getMyMac() {
    	return myMac;
    }

    public int getMyId(){
	return id;
    }

    public Role getMyRole(){
	return role;
    }

    // TLR 2009.1.25: Support for different threads handling the network interaction with each role

    public Principal[] getMyInterfaces(Role r) {
    	switch(r) {
	case FILTER: return myFilterInterfaces;
	case ORDER: return myOrderInterfaces; 
	case EXEC: return myExecInterfaces; 
	case CLIENT: return myClientInterfaces; 
	default: throw new RuntimeException("Unknown Role");
    	}
    }
}

