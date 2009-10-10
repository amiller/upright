// $Id$

package BFT.messages;

abstract public class MacMessage extends VerifiedMessageBase{

	public MacMessage(int _tag, int _size, long send){
	    super(_tag, _size, computeAuthenticationSize());
	    sender = send;
	    byte[] tmp = BFT.util.UnsignedTypes.longToBytes(send);
	    int offset = getTotalSize() - getAuthenticationSize();
	    byte[] bytes = getBytes();
	    for (int i = 0;i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	}

    long sender;
    final public long getSender(){return sender;}

	public MacMessage(byte[] bytes){
		super(bytes);
		int offset = getTotalSize() - getAuthenticationSize();
		byte[] tmp = new byte[MessageTags.uint32Size];
		for(int i = 0; i <  tmp.length; i++, offset++)
		    tmp[i] = bytes[offset];
		sender = BFT.util.UnsignedTypes.bytesToLong(tmp);
		if (offset != getTotalSize() - actualAuthenticationSize())
		    BFT.Debug.kill("BAD MACMESSAGE CONSTRUCTION");
	}


	public static int computeAuthenticationSize(){
	    return actualAuthenticationSize() + 
		BFT.messages.MessageTags.uint32Size;
	}
    
    private static int actualAuthenticationSize(){
	return MacBytes.size();
    }

//	public void generateMac(String key){ 
//		throw new RuntimeException("unimplemented");
//	}

	public MacBytes getMacBytes() {
		byte[] dst = new byte[actualAuthenticationSize()];
		System.arraycopy(getBytes(), 
				 getTotalSize() -actualAuthenticationSize(),
				 dst, 0, actualAuthenticationSize());
		return new MacBytes(dst);
	}
	
	public void setMacBytes(MacBytes mb) {
		System.arraycopy(mb.getBytes(), 0, getBytes(), getTotalSize() - actualAuthenticationSize(), actualAuthenticationSize());
	}

//	public boolean authenticateMac(String key){
//		throw new RuntimeException("unimplemented");
//	}
//
//	public boolean containsMac(){ 
//		throw new RuntimeException("unimplemented");
//	}
}