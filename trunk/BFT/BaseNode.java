// $Id$

package BFT;


import BFT.Parameters;
import BFT.util.Role;
import BFT.util.Multiplexor;
import BFT.util.UnsignedTypes;
import BFT.messages.VerifiedMessageBase;
import BFT.messages.Digest;
import BFT.membership.Membership;
import BFT.membership.Principal;
import BFT.messages.MacBytes;
import BFT.messages.MacArrayMessage;
import BFT.messages.MacMessage;
import BFT.messages.MacSignatureMessage;

import BFT.network.Network;
import BFT.network.concurrentNet.*;
import BFT.network.MessageHandler;

import java.security.interfaces.*;
import javax.crypto.*;

import java.util.Calendar;


abstract public class BaseNode extends Throwable implements MessageHandler {

	protected Membership members;
	public Network network;
    protected ConcurrentNetwork filterNet = null;
    protected ConcurrentNetwork orderNet = null;
    protected ConcurrentNetwork execNet = null;
    protected ConcurrentNetwork clientNet = null;
    //Multiplexor mux;



	public BaseNode(String membershipFile, String myId){
		throw new RuntimeException("DEPRECATED: Please use the"+
				" BaseNode(String membershipFile,"+
		" Role myRole, int myId) constructor");
	}

	public BaseNode(String membershipFile, Role myRole, int myId){
		// populate the membership list
		members = new Membership(membershipFile, myRole, myId);



		// TODO: fill in all the appropriate keys



		////System.out.println("registered ServerSocketChannels");

	}

	public Membership getMembership(){
		return members;
	}

	/**
       Return my identifier as a client/order/execution node
	 **/
	public int getMyClientIndex(){
		if (members.getMyRole() == BFT.util.Role.CLIENT)
			return members.getMyId();
		return -1;
	}
	public int getMyOrderIndex(){
		if (members.getMyRole() == BFT.util.Role.ORDER)
			return members.getMyId();
		return -1;
	}
	public int getMyExecutionIndex(){
		if (members.getMyRole() == BFT.util.Role.EXEC)
			return members.getMyId();
		return -1;
	}
    public int getMyIndex(){
	return members.getMyId();
    }


    public int getMyFilterIndex(){
	if (members.getMyRole() == BFT.util.Role.FILTER)
	    return members.getMyId();
	else
	    return -1;
    }

	public Mac getMyMac() {
		return members.getMyMac();
	}

	/** Functions to get the Mac keys for specific nodes **/
	public Mac getClientMac(int id){ 
		return members.getClientNodes()[id].getMacKey();
	}
	public Mac[] getClientMacs(){
		int arrayLength = members.getClientNodes().length;
		Mac[] clientMacs = new Mac[arrayLength];
		for(int i=0;i<arrayLength;i++) {
			clientMacs[i] = members.getClientNodes()[i].getMacKey();
		}
		return clientMacs;
	}

    public Mac getFilterReplicaMac(int id){
	return members.getFilterNodes()[id].getMacKey();
    }

    public Mac[] getFilterMacs(){
	int arrayLength = members.getFilterNodes().length;
	Mac[] filterMacs = new Mac[arrayLength];
	for(int i=0;i<arrayLength;i++) {
	    filterMacs[i] = members.getFilterNodes()[i].getMacKey();
	}
	return filterMacs;
    }


	public Mac getOrderReplicaMac(int id){
		return members.getOrderNodes()[id].getMacKey();
	}
	public Mac[] getOrderMacs(){
		int arrayLength = members.getOrderNodes().length;
		Mac[] orderMacs = new Mac[arrayLength];
		for(int i=0;i<arrayLength;i++) {
			orderMacs[i] = members.getOrderNodes()[i].getMacKey();
		}
		return orderMacs;
	}

	public Mac getExecutionReplicaMac(int id) { 
		return members.getExecNodes()[id].getMacKey();
	}
	public Mac[] getExecutionMacs() { 
		int arrayLength = members.getExecNodes().length;
		Mac[] execMacs = new Mac[arrayLength];
		for(int i=0;i<arrayLength;i++) {
			execMacs[i] = members.getExecNodes()[i].getMacKey();
		}
		return execMacs;
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

    public void sendToFilterReplica(byte m[], int id){
	if(filterNet != null) {
	    filterNet.send(m, id);
	}
	else if(network != null) {
	    network.send(m, Role.FILTER, id);
	}
	else {
	    throw new RuntimeException("Send to unitialized filter net "+id);
	}
    }

    public void sendToAllFilterReplicas(byte[] m){
	for (int i = 0; i < BFT.Parameters.getFilterCount(); i++)
	    sendToFilterReplica(m, i);
	    //	    sendToFilterReplica(m, (i+getMyIndex())%BFT.Parameters.getFilterCount());
    }



	/** Functions to communicate with the order replicas **/
	public void sendToOrderReplica(byte m[], int id){
		if(orderNet != null) {
			orderNet.send(m, id);
		}
		else if(network != null) {
			network.send(m, Role.ORDER, id);
		}
		else {
			throw new RuntimeException("Send to unitialized order net");
		}
	}
	public void sendToAllOrderReplicas(byte[] m){
		if(orderNet != null) {
			for (int i=0;i<BFT.Parameters.getOrderCount();i++) {
			    orderNet.send(m, i);
			}
		}
		else if(network != null) {
		    int index = members.getMyId();
		    for (int i=0;i<BFT.Parameters.getOrderCount();
			 i++){
			index = i;//(i+getMyIndex())%BFT.Parameters.getOrderCount();
			network.send(m, Role.ORDER, i);
			//			network.send(m, Role.ORDER, (i+getMyIndex())%BFT.Parameters.getOrderCount());
			}
		}
		else {
			throw new RuntimeException("Send to unitialized order net");
		}
	}

	public void sendToOtherOrderReplicas(byte[] m){
		if(orderNet != null) {
			for (int i = 0;i < BFT.Parameters.getOrderCount();i++) {
				if (i != getMyOrderIndex()) {
					orderNet.send(m, i);
				}
			}
		}
		else if(network != null) {
			for (int i = 0;i < BFT.Parameters.getOrderCount();i++) {
			    int index = i;
			    //			    int index = (i+getMyIndex())%BFT.Parameters.getOrderCount();
				if (index != getMyOrderIndex()) {
					network.send(m, Role.ORDER,index);
				}
			}
		}
		else {
			throw new RuntimeException("Send to unitialized order net");
		}
	}


	/** Functions to communicate with the execution replicas **/
	public void sendToExecutionReplica(byte m[], int id){
		if(execNet != null) {
			execNet.send(m, id);
		}
		else if(network != null) {
			network.send(m, Role.EXEC, id);
		}
		else {
			throw new RuntimeException("Send to unitialized exec net");
		}
	}
	public void sendToAllExecutionReplicas(byte m[]){ 
	    if(execNet != null) {
		for (int i=0;i<BFT.Parameters.getExecutionCount();i++) {
		    execNet.send(m, i);
		}
	    }
	    else if(network != null) {
		int index ;
		for (int i=0;i<BFT.Parameters.getExecutionCount();i++) {
		    index = i;//(i + getMyIndex()) % BFT.Parameters.getExecutionCount();
		    network.send(m, Role.EXEC, index);
		}
	    }
	    else {
		throw new RuntimeException("Send to unitialized exec net");
	    }
	}
	public void sendToOtherExecutionReplicas(byte[] m){
		if(execNet != null) {
			for (int i = 0;i < BFT.Parameters.getExecutionCount();i++) {
				if (i != getMyOrderIndex()) {
					execNet.send(m,i);
				}
			}
		}
		else if(network != null) {
		    for (int i = 0;i < BFT.Parameters.getExecutionCount();i++) {
			int index = i;
			//			int index = (i+getMyIndex())%BFT.Parameters.getExecutionCount();
			if (index != getMyOrderIndex()) {
			    network.send(m, Role.EXEC,index);
			}
		    }
		}
		else {
			throw new RuntimeException("Send to unitialized exec net");
		}
	}

	/** Functions to communciate with the client **/
	public void sendToClient(byte m[], int id){ 
		if(clientNet != null) {
			clientNet.send(m, id);
		}
		else if(network != null) {
			network.send(m, Role.CLIENT, id);
		}
		else {
			throw new RuntimeException("Send to an unitialized client net");
		}
	}




	/**
       Abstract method for handling incoming messages
	 **/
	abstract public void handle(byte[] bytes);


	public void setNetwork(Network net){
		if (network != null)
			throw new RuntimeException("Network has already been set");
		network = net;
	}

	public void setNetwork(ConcurrentNetwork net) {
		switch(net.getMyRole()) {
		case CLIENT: clientNet = net; break;
		case EXEC: execNet = net; break;
		case ORDER: orderNet = net; break;
		case FILTER: filterNet = net; break;
		default: throw new RuntimeException("Unknown Role");
		}
	}

	/**
       Listen to appropriate sockets and call handle on all
       appropriate incoming messages
	 **/
    public void start() {
	if (network == null && execNet == null && clientNet == null && orderNet == null) {
	    throw new RuntimeException("dont have a network");
	}
	else if (network != null) {
	    network.start();
	}else
	    System.out.println("wtf");

    }

	public void stop() {
        network.stop();
		//throw new RuntimeException("Not yet implemented");
	}


	public boolean validateClientMacMessage(MacMessage mm){
		Mac key = getClientMac((int)mm.getSender());
		return _validateMacMessage(mm, key);
	}

	public boolean validateOrderMacMessage(MacMessage mm){
		Mac key = getOrderReplicaMac((int)mm.getSender());
		return _validateMacMessage(mm, key);
	}

	public boolean validateExecMacMessage(MacMessage mm){
		Mac key = getExecutionReplicaMac((int)mm.getSender());
		return _validateMacMessage(mm, key);
	}
    	public boolean validateFilterMacMessage(MacMessage mm){
		Mac key = getFilterReplicaMac((int)mm.getSender());
		return _validateMacMessage(mm, key);
	}
    

    protected boolean _validateMacMessage(MacMessage mm, Mac key){
	return validateBytes(mm.getBytes(), mm.getAuthenticationStartIndex(), 
			     mm.getAuthenticationLength(),
			     key, mm.getMacBytes());
    }


    public boolean validateClientMacArrayMessage(MacArrayMessage mam){
	Mac[] keys = getClientMacs();
	return _validateMacArrayMessage(mam, keys);
    }
    
    
    public boolean validateFilterMacArrayMessage(MacArrayMessage mam){
	return _validateMacArrayMessage(mam, getFilterMacs());
    }
    
    public boolean validateOrderMacArrayMessage(MacArrayMessage mam){
	return _validateMacArrayMessage(mam, getOrderMacs());
    }
    
    public boolean validateExecMacArrayMessage(MacArrayMessage mam){
	return _validateMacArrayMessage(mam, getExecutionMacs());
    }

    protected boolean _validateMacArrayMessage(MacArrayMessage mam, 
					       Mac[] keys){

	Role role = members.getMyRole();
	int index = members.getMyId();
	int size = 0;
	int num = 0;
	switch(role) {
	case ORDER: size = MacArrayMessage.computeAuthenticationSize(BFT.Parameters.getOrderCount()); num = BFT.Parameters.getOrderCount();break;
	case EXEC: size = MacArrayMessage.computeAuthenticationSize(BFT.Parameters.getExecutionCount()); num = BFT.Parameters.getExecutionCount();break;
	case FILTER: size = MacArrayMessage.computeAuthenticationSize(BFT.Parameters.getFilterCount()); num = BFT.Parameters.getFilterCount(); break;
	default: BFT.Debug.kill(new RuntimeException("No macarray messages from clients for "+role));
	}
	Digest ad = mam.getAuthenticationDigest();
	boolean retVal = validateBytes(mam.getBytes(),
				       mam.getAuthenticationStartIndex(),
				       mam.getAuthenticationLength(),
				       keys[(int)mam.getSender()], 
				       mam.getMacBytes(index, num));
	return retVal;
    }


    /** Check that a threshold of folks authenticate this message for
     * you **/
    public boolean validateFullFilterMacSignatureMessage(MacSignatureMessage mam,
							 int thresh){
	Mac[] keys = getFilterMacs();
	int count = 0;
	int me = getMyOrderIndex();
	for (int i = 0; i < keys.length && count < thresh; i++){
	    MacBytes[] mb = mam.getMacArray(i, BFT.Parameters.getOrderCount());
	    if (validateBytes(mam.getBytes(),
			      mam.getAuthenticationStartIndex(),
			      mam.getAuthenticationLength(),
			      keys[i], mb[me]))
		count++;
		    
	}
	if (count < thresh)
	    BFT.Debug.kill("OH SHIT, bad things");
	return count >= thresh;
    }

    /** check that the specified sender authenticates this message for you **/
    public boolean validatePartialFilterMacSignatureMessage(MacSignatureMessage mam,
							    int signer){

	Mac[] keys = getFilterMacs();
	MacBytes[] mb = mam.getMacArray(signer, 
					BFT.Parameters.getOrderCount());
	boolean retVal = validateBytes(mam.getBytes(),
				       mam.getAuthenticationStartIndex(),
				       mam.getAuthenticationLength(),
				       keys[signer], 
				       mb[members.getMyId()]);
	return retVal;
	//	System.out.println("basenode.validatepartialfilter must be completed");
	//	return true;
    }


	public void authenticateClientMacMessage(MacMessage mm, int index){
		Mac key = getMyMac();
		_authenticateMacMessage(mm, key);
	}

	public void authenticateOrderMacMessage(MacMessage mm, int index){
		Mac key = getMyMac();
		_authenticateMacMessage(mm, key);
	}

	public void authenticateExecMacMessage(MacMessage mm, int index){
		Mac key = getMyMac();
		_authenticateMacMessage(mm, key);
// 		if(members.getMyRole() == Role.EXEC) {
// 			if (!validateExecMacMessage(mm))
// 				Debug.kill("foo");
// 		}
// 		else if(members.getMyRole() == Role.ORDER) {
// 			if (!validateOrderMacMessage(mm))
// 				Debug.kill("foo");
// 		}
// 		else {
// 			Debug.kill("BAD");
// 		}
	}

    public void _authenticateMacMessage(MacMessage mm, Mac key){
	MacBytes mb = authenticateBytes(mm.getBytes(), 0,
					mm.getTotalSize() - 
					mm.getAuthenticationSize(), key);
	mm.setMacBytes(mb);
    }

    protected void _authenticateMacArrayMessage(MacArrayMessage mam,
						Mac[] keys){
	Digest ad = mam.getAuthenticationDigest();
	for (int i = 0; i < keys.length; i++){
	    MacBytes mb = 
		authenticateBytes(ad.getBytes(), 0, 
				  ad.getSize(),
				  keys[i]);
	    mam.setMacBytes(i, mb, keys.length);
	}

    }

	public void authenticateOrderMacArrayMessage(MacArrayMessage mam){
	    Mac[] keys = new Mac[BFT.Parameters.getOrderCount()];
	    for (int i = 0;i < keys.length; i++)
		keys[i] = getMyMac();
	    _authenticateMacArrayMessage(mam, keys);
	}

    	public void authenticateFilterMacArrayMessage(MacArrayMessage mam){
	    Mac[] keys = new Mac[BFT.Parameters.getFilterCount()];
	    for (int i = 0;i < keys.length; i++)
		keys[i] = getMyMac();
	    _authenticateMacArrayMessage(mam, keys);
	}

	public void authenticateExecMacArrayMessage(MacArrayMessage mam){
	    Mac[] keys = new Mac[BFT.Parameters.getExecutionCount()];
	    for (int i = 0;i < keys.length; i++)
		keys[i] = getMyMac();
	    _authenticateMacArrayMessage(mam, keys);
	}

    public void authenticateOrderMacSignatureMessage(MacSignatureMessage msm){
	Mac key = getMyMac();
	MacBytes mb[] = new MacBytes[Parameters.getOrderCount()];
	// is the sender a filter or order node
	int columns = Parameters.getOrderCount();
	for (int i = 0; i < mb.length; i++){
	    mb[i] = authenticateBytes(msm.getBytes(),
				      msm.getAuthenticationStartIndex(),
				      msm.getAuthenticationLength(),
				      key);
	}
	int index = 
	    (members.getMyRole() == Role.FILTER)?getMyFilterIndex():
	    (members.getMyRole() == Role.ORDER)?getMyOrderIndex(): -1;
	msm.setMacArray(index, mb);
	MacBytes mb2[] = msm.getMacArray(index, BFT.Parameters.getOrderCount());
// 	for (int i = 0; i < mb2.length; i++){
// 	    if (!validateBytes(msm.getBytes(),
// 			       msm.getAuthenticationStartIndex(),
// 			       msm.getAuthenticationLength(),
// 			       key, mb2[i]))
// 		Debug.kill("broken autentication!");
// 	}
    }

	/**
	 * @param contents The bytes with which to generate a MAC
	 * @param key The initialized {@link javax.crypto.Mac} object used to generate a MAC
	 * @return The {@link MacBytes} object containing the freshly generated MAC
	 */
	public MacBytes authenticateBytes(byte[] contents, Mac key) {
		return authenticateBytes(contents, 0, contents.length, key);
		//     	MacBytes retBytes = null;
		//     	retBytes = new MacBytes(key.doFinal(contents));
		//     	return retBytes;
	}

	/**
	 * @param contents The bytes with which to generate a MAC
	 * @param offset The offset in bytes into contents where the data starts
	 * @param len The length of the data in bytes
	 * @param key The initialized {@link javax.crypto.Mac} object used to generate a MAC
	 * @return The {@link MacBytes} object containing the freshly generated MAC
	 */
	public MacBytes authenticateBytes(byte[] contents, int offset, int len, Mac key) {
		synchronized(key){
			MacBytes retBytes = null;
			if(BFT.Parameters.insecure) {
				retBytes = new MacBytes();
			}
			else {
				key.update(contents, offset, len);
				retBytes = new MacBytes(key.doFinal());
			}
			return retBytes;
		}
	}	

	/**
	 * @param contents The bytes that macBytes refers to
	 * @param key The initialized {@link javax.crypto.Mac} object used to generate a MAC
	 * @param macBytes The {@link MacBytes} containing the MAC to be verified
	 * @return True macBytes represents the MAC generated by key over contents
	 */
	public boolean validateBytes(byte[] contents, Mac key, MacBytes macBytes) {
		return validateBytes(contents, 0, contents.length, key, macBytes);
	}

	/**
	 * @param contents The bytes that macBytes refers to
	 * @param offset The offset in bytes into contents where the data starts
	 * @param len The length of the data in bytes
	 * @param key The initialized {@link javax.crypto.Mac} object used to generate a MAC
	 * @param macBytes The {@link MacBytes} containing the MAC to be verified
	 * @return True macBytes represents the MAC generated by key over contents
	 */
	protected boolean validateBytes(byte[] contents, int offset, int len, Mac key, MacBytes macBytes) {
		boolean retVal = false;
		if(BFT.Parameters.insecure) {
			retVal = true;
		}
		else {
		//	    key.update(contents, offset, len);
		//	    MacBytes answer = new MacBytes(key.doFinal());
			MacBytes answer = authenticateBytes(contents, offset, len, key);
			retVal = macBytes.equals(answer);
		}
		return retVal;
	}

	public void printTime(boolean isStart) {
		StackTraceElement[] elements = getStackTrace();
		Calendar now = Calendar.getInstance();
		String startEnd = isStart? "Starting ": "Ending   ";
		String toPrint = now.getTimeInMillis()+": "+
		startEnd+elements[0].getClassName()+"."+elements[0].getMethodName();
		System.err.println(toPrint);
	}


}
