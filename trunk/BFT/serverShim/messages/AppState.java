// $Id$

package BFT.serverShim.messages;

import BFT.util.UnsignedTypes;
import BFT.messages.MacMessage;

import BFT.Parameters;

public class AppState extends MacMessage{
    public AppState( byte[] tok, byte[] st, long sender){
	super(MessageTags.AppState, 
	      computeSize(tok, st), sender);

	token = tok;
	state = st;

	// now lets get the bytes
	byte[] bytes = getBytes();

	// copy the size of the token
	byte[] tmp = UnsignedTypes.intToBytes(token.length);
	int offset = getOffset();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// copy the token
	for (int i = 0; i < token.length; i++, offset++)
	    bytes[offset] = token[i];
	// copy the size of the token
	//System.out.println("size of state: "+state.length);
	tmp = UnsignedTypes.longToBytes(state.length);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	// copy the token
	for (int i = 0; i < state.length; i++, offset++)
	    bytes[offset] = state[i];
    }


    public AppState(byte[] bytes){
	super(bytes);
	if (getTag() != MessageTags.AppState)
	    throw new RuntimeException("invalid message Tag: "+getTag());
		
	// pull the length of the token
	byte[] tmp = new byte[BFT.messages.MessageTags.uint16Size];
	int offset = getOffset();
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	int size = UnsignedTypes.bytesToInt(tmp);
	token = new byte[size];
	// pull the token itself
	for (int i = 0; i < token.length; i++, offset++)
	    token[i] = bytes[offset];

			
	// pull the length of the state
	tmp = new byte[BFT.messages.MessageTags.uint32Size];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	size = (int)(UnsignedTypes.bytesToLong(tmp));
	//System.out.println("State has size: "+size);
	state = new byte[size];
	// pull the state itself
	for (int i = 0; i < state.length; i++, offset++)
	    state[i] = bytes[offset];

	if (offset != bytes.length - getAuthenticationSize()){
	    System.out.println("offset: "+offset);
	    System.out.println("bytes.length: "+bytes.length);
	    System.out.println("authenticationsize: "+getAuthenticationSize());
	    BFT.Debug.kill(new RuntimeException("Invalid byte input "+offset+ " != "+
						(bytes.length - getAuthenticationSize())));
	}
    }


    protected  byte[] token;
    protected byte[] state;

    public long getSendingReplica(){
	return getSender();
    }

    public byte[] getToken(){
	return token;
    }

    public byte[] getState(){
	return state;
    }

    private static int computeSize(byte[] tok, byte[] st){
	return  MessageTags.uint16Size + MessageTags.uint32Size + tok.length + st.length; 
    }

    
    public String toString(){
	return "<APP-sSTATE, "+super.toString()+", token: "+token+">";
    }

    public static void main(String args[]){
	byte[] tmp = new byte[8];
	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte)i;
	AppState vmb = 
	    new AppState( tmp,tmp, 1);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	AppState vmb2 = 
	    new AppState(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());

	for (int i = 0; i < 8; i++)
	    tmp[i] = (byte) (tmp[i] * tmp[i]);

	vmb = new AppState(tmp, tmp, 134);
	//System.out.println("initial: "+vmb.toString());
	UnsignedTypes.printBytes(vmb.getBytes());
	 vmb2 = new AppState(vmb.getBytes());
	//System.out.println("\nsecondary: "+vmb2.toString());
	UnsignedTypes.printBytes(vmb2.getBytes());
 
	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));

   }

}