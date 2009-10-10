// $Id$


package BFT.messages;

import BFT.util.UnsignedTypes;

public abstract class VerifiedMessageBase{

    public VerifiedMessageBase(int _tag, int _size,  
			       int _auth_size){
	tag = _tag;
	size = _size;
	authenticationSize = _auth_size;
	bytes = new byte[getTotalSize()];
	byte[] tmp2 = UnsignedTypes.intToBytes(tag);
	byte[] tmp3 = UnsignedTypes.longToBytes(size);
	
	// write the tag
	bytes[0] = tmp2[0];
	bytes[1] = tmp2[1];
	// write the size of the payload
	bytes[2] = tmp3[0];
	bytes[3] = tmp3[1];
	bytes[4] = tmp3[2];
	bytes[5] = tmp3[3];
    }
    
    public VerifiedMessageBase(byte[] bits){
	bytes = bits;

	// set the authenticationSize
	authenticationSize = bits.length - getPayloadSize() -
	    getOffset();

	// decipher the tag
	byte[] tmp = new byte[2];
	int offset = 0;
	for (int i = 0; i < 2; i++, offset++)
	    tmp[i] = bytes[offset];
	tag = UnsignedTypes.bytesToInt(tmp);

	// decipher the payload size
	tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	size = (int)UnsignedTypes.bytesToLong(tmp);

	authenticationSize = bits.length - getPayloadSize() -
	    getOffset();

	bytes = bits;
    }

    private int tag;
    private int size;
    private int authenticationSize;
    private byte[] bytes;

    /** 
	returns the total size of the byte representation of the message 
    **/
    final public int getTotalSize(){
	return getPayloadSize() + getOffset() + getAuthenticationSize();
    }
    
    /** 
     * returns the offset that subclasses should use in order to start
     * modifying the underlying byte array
     **/
    final static public  int getOffset(){
	return verificationBaseSize;
    }

    final public int getAuthenticationSize(){
	return authenticationSize;
    }


    public boolean equals(VerifiedMessageBase m){
	boolean res = tag == m.tag && size == m.size &&
	    bytes.length == m.bytes.length;
	for (int i = 0; i < bytes.length && res; i++)
	    res = res && bytes[i] == m.bytes[i];
	return res;
    }

    public boolean matches(VerifiedMessageBase m){
	BFT.Debug.kill(new RuntimeException("Not Yet Implemented"));
	return false;
    }

    public boolean isValid(){
	return true;
    }

    final public byte[] getBytes() {
	return bytes;
    }

    abstract public long getSender();
    public int getAuthenticationStartIndex(){ 
	return 0;
    }
    public int getAuthenticationEndIndex(){ 
	return getOffset() + getPayloadSize(); 
    }
    public int getAuthenticationLength(){
	return getAuthenticationEndIndex() - getAuthenticationStartIndex();
    }

    public int getTag(){ return tag;}
    public int getPayloadSize(){ return size;}

    /**
     * I HATE THAT THIS FIELD IS PUBLIC 
    **/
    private final static int verificationBaseSize = 
	MessageTags.uint16Size + MessageTags.uint32Size;



    public String toString(){
	return "<VMB, tag:"+getTag()+", payloadSize:"+getPayloadSize()+", sender:"+getSender()+">";
    }

//     public static void main(String args[]){
// 	VerifiedMessageBase vmb = 
// 	    new VerifiedMessageBase(1,0, 3, 0);
// 	//System.out.println("initial: "+vmb.toString());
// 	UnsignedTypes.printBytes(vmb.getBytes());
// 	VerifiedMessageBase vmb2 = 
// 	    new VerifiedMessageBase(vmb.getBytes());
// 	//System.out.println("\nsecondary: "+vmb2.toString());
// 	UnsignedTypes.printBytes(vmb2.getBytes());

// 	vmb = new VerifiedMessageBase(134,8, 1234125, 0);
// 	//System.out.println("initial: "+vmb.toString());
// 	UnsignedTypes.printBytes(vmb.getBytes());
// 	 vmb2 = new VerifiedMessageBase(vmb.getBytes());
// 	//System.out.println("\nsecondary: "+vmb2.toString());
// 	UnsignedTypes.printBytes(vmb2.getBytes());

// 	//System.out.println("old = new: "+(vmb2.toString().equals(vmb.toString())));
// 	//System.out.println(vmb2.equals(vmb));
		
//     }
    

    
}



