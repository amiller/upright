// $Id$

package BFT.messages;

import BFT.messages.MessageTags;

import BFT.Debug;

abstract public class MacSignatureMessage extends VerifiedMessageBase{

    public MacSignatureMessage( int _tag, int _size,
				int rowcount, int rowsize){
	super(_tag, _size,  computeAuthenticationSize(rowcount, rowsize));
    }
    
    public MacSignatureMessage(byte[] bytes){
	super(bytes);
    }
    

    static public int computeAuthenticationSize(int rowcount, int rowsize){
	// should be something else
	return rowcount * rowsize * MacBytes.size();
    }
    protected int authenticationStartIndex(){
	return getOffset()+getPayloadSize();
    }

    protected int macStartIndex(){
	return authenticationStartIndex() ;
    }

    public void setMacArray(int index, MacBytes[] row){
	int offset = 
	    macStartIndex() + index * row.length * MacBytes.size();
	for (int i = 0; i < row.length; i++)
	    for (int j = 0; j < row[i].getBytes().length; j++, offset++)
		getBytes()[offset] = row[i].getBytes()[j];
	if (internalBytes != null)
	    internalBytes[index] = row;
    }

    protected MacBytes[][] internalBytes;
    public MacBytes[] getMacArray(int index, int rowlength){
	if (internalBytes != null)
	    return internalBytes[index];
	
	int rowcount = getAuthenticationSize() / (rowlength * MacBytes.size());
	MacBytes[][] internalBytes = new MacBytes[rowcount][rowlength];
	for (int i = 0; i < rowcount * rowlength; i++){
	    byte[] mb = new byte[MacBytes.size()];
	    System.arraycopy(getBytes(), 
			     macStartIndex()+i*mb.length,
			     mb,
			     0, MacBytes.size());
	    internalBytes[i/rowlength][i%rowlength] = new MacBytes(mb);
	}
	return internalBytes[index];
    }

    

}