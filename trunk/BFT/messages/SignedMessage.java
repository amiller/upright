// $Id$

package BFT.messages;

import java.security.*;
import java.security.interfaces.*;

import BFT.Debug;

abstract public class SignedMessage extends VerifiedMessageBase{

	public SignedMessage(int _tag, int _size, long send){
	    super( _tag, _size, computeAuthenticationSize());
	    sender = send;
	    byte[] tmp = BFT.util.UnsignedTypes.longToBytes(send);
	    int offset = getOffset() + getPayloadSize();
	    byte[] bytes = getBytes();
	    for (int i = 0;i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	}

	public SignedMessage(byte[] bytes){
	    super(bytes);
	    int offset = getOffset() + getPayloadSize();
	    byte[] tmp = new byte[MessageTags.uint32Size];
	    for(int i = 0; i <  tmp.length; i++, offset++)
		tmp[i] = bytes[offset];
	    sender = BFT.util.UnsignedTypes.bytesToLong(tmp);
	}

    protected long sender;
    final public long getSender(){
	return sender;
    }

	public static int computeAuthenticationSize(){
		// should return size in bytes of a signatures
		return 128 + MessageTags.uint32Size;
	}

    protected int startSig(){
	return getOffset() + getPayloadSize() + MessageTags.uint32Size;
    }

    public void sign(PrivateKey key){
	Debug.profileStart("SIGN");
	if (BFT.Parameters.insecure)
	    return;
	try {
	    java.security.Signature sig = java.security.Signature.getInstance("MD5withRSA", BFT.Parameters.provider);
	    sig.initSign(key);
	    sig.update(getBytes(), 0, 
		       startSig());
	    sig.sign(getBytes(), startSig(), computeAuthenticationSize()-MessageTags.uint32Size);
	    //BFT.util.UnsignedTypes.printBytes(getBytes());
	}
	catch (Exception e) {
	    e.printStackTrace(System.err);
	}

	Debug.profileFinis("SIGN");
		
    }

    public boolean verifySignature(PublicKey key){
	Debug.profileStart("VERIFY");
	boolean retVal = false;
	if (BFT.Parameters.insecure)
	    return true;
	try {
	    //BFT.util.UnsignedTypes.printBytes(getBytes());
	    java.security.Signature sig = java.security.Signature.getInstance("MD5withRSA", BFT.Parameters.provider);
	    sig.initVerify(key);
	    sig.update(getBytes(), 0, startSig());
	    retVal = sig.verify(getBytes(), startSig(), computeAuthenticationSize() -MessageTags.uint32Size);


	}
	catch (Exception e) {
	    e.printStackTrace(System.err);
	}
	Debug.profileFinis("VERIFY");
	return retVal;
    }


	public boolean containsSignature(){
		throw new RuntimeException("Not Yet Implemented");
	}

}
