// $Id$

package BFT.messages;
import BFT.messages.VerifiedMessageBase;

import BFT.Debug;

public class MacArrayMessage extends VerifiedMessageBase{

    public MacArrayMessage(int _tag, int _size, long send, int arraySize){
	super( _tag, _size, computeAuthenticationSize(arraySize));
	byte[] bytes = getBytes();
	int offset = getOffset() + getPayloadSize();
	if (offset != getTotalSize() - getAuthenticationSize())
	    BFT.Debug.kill("something horribly broken");
	sender = send;
	byte tmp[] = BFT.util.UnsignedTypes.longToBytes(send);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }
    
    public MacArrayMessage(byte[] bytes){
	super(bytes);
	int offset = getTotalSize() - getAuthenticationSize();
	byte tmp[] = new byte[MessageTags.uint32Size];
	for (int i = 0; i < tmp.length; i++, offset++)
	    tmp[i] = bytes[offset];
	sender = BFT.util.UnsignedTypes.bytesToLong(tmp);
	tmp = new byte[Digest.size()];
	for (int i = 0; i < Digest.size(); i++, offset++)
	    tmp[i] = bytes[offset];
	authenticationDigest = Digest.fromBytes(tmp);
    }

    protected Digest authenticationDigest;
    protected long sender;
    final public long getSender(){ return sender;}

    public static int computeAuthenticationSize(int i){
	return i * MacBytes.size() + Digest.size()+MessageTags.uint32Size;
    }

    protected int digestStart(){
	return getOffset() + getPayloadSize() + MessageTags.uint32Size;
    }
    
    protected int macStart(){
	return digestStart() + Digest.size();
    }

    public int getAuthenticationStartIndex(){
	return digestStart();
    }

    public int getAuthenticationEndIndex(){
	return macStart();
    }

    public Digest getAuthenticationDigest(){
	if (authenticationDigest == null){
		if(BFT.Parameters.insecure) {
			authenticationDigest = new Digest();
		}
		else {
			authenticationDigest = new Digest(getBytes(), 0, getTotalSize() - getAuthenticationSize());			
		}
	    // add to byte array
	    byte tmp[] = getBytes();
	    int offset = digestStart();
	    for (int i = 0; i < authenticationDigest.getBytes().length; i++, offset++)
		tmp[offset] = authenticationDigest.getBytes()[i];
	}
	return authenticationDigest;
    }

    public boolean isValid(){
	return checkAuthenticationDigest();
    }

    public boolean checkAuthenticationDigest(){
	if (authenticationDigest == null)
	    Debug.kill("authentication must be non-null to be checked");
	Digest tmp = authenticationDigest;
	authenticationDigest = null;
	return tmp.equals(getAuthenticationDigest());
    }

    /**
       return types of following two are subject to change
     **/
    public MacBytes getMacBytes(int index, int arraySize){
	byte[] dst = new byte[MacBytes.size()];
	int offset = macStart() + index*MacBytes.size();
	byte[] bytes = getBytes();
	for (int i=0; i < dst.length; i++, offset++)
	    dst[i] = bytes[offset];
	return new MacBytes(dst);
    }
    
    public void setMacBytes(int index, MacBytes mb, int arraySize) {
	
	byte[] bytes = getBytes();
	byte[] tmp = mb.getBytes();
	int offset = macStart()+ index*MacBytes.size();
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }


    public MacBytes[] getMacArray(){
	BFT.Debug.kill("NYI");
	return null;
    }


    public boolean equals(MacArrayMessage m){
	return super.equals(m);
    }

}