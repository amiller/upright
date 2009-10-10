// $Id$

package BFT.messages;

import java.util.Vector;
import BFT.util.UnsignedTypes;

public class Entry {//implements Comparable<Entry>{
    
    public Entry(long client, long req, Digest d){
	clientId = client;
	requestId = req;
	command = null;
	digest = d;
	has_digest = true;
    }

    public Entry(long client, long req, byte[] com){
	clientId = client;
	requestId = req;
	command = com;
	bytes = null;
	has_digest = false;

    }

    public static Entry fromBytes(byte[] bytes, int offset){
	int orig = offset;
	// digest or command
	boolean has_digest = bytes[offset++] == 1;
	// client id
	byte[] tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	long clientId = UnsignedTypes.bytesToLong(tmp);
	// read the requestId
	//	tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	long requestId = UnsignedTypes.bytesToLong(tmp);
	// read the command size
	//	tmp = new byte[4];
	for (int i = 0; i < 4; i++, offset++)
	    tmp[i] = bytes[offset];
	int size = (int)(UnsignedTypes.bytesToLong(tmp));
	// read the command
	byte[] com = new byte[size];
	for (int i = 0; i < com.length; i++, offset++)
	    com[i] = bytes[offset];
	Entry entry;
	if (has_digest){
	    Digest d = Digest.fromBytes(com);
	    com = null;
	    entry = new Entry(clientId, requestId, d);
	}else{
	    entry = new Entry(clientId, requestId, com);
	}
	return entry;
    }


    public static Entry[] getList(byte[] bytes, int e_count){
	byte[] tmp = new byte[4];
	Entry[] v = new Entry[e_count];
	int count = 0;
	int offset = 0;
	while(offset < bytes.length){
	    v[count++] = Entry.fromBytes(bytes, offset);
	    offset += v[count-1].getSize();
	}
	if (count != e_count)
	    throw new RuntimeException("Ill formed entry list");
	return v;
    }

    protected long clientId;
    protected long requestId;
    protected byte[] command;
    protected byte[] bytes;
    protected Digest digest;
    protected boolean has_digest;
    

    public boolean has_digest(){
	return has_digest;
    }

    public long getClient(){
	return clientId;
    }

    public long getRequestId(){
	return requestId;
    }

    public byte[] getCommand(){
	return command;
    }

    public Digest getDigest(){
	return digest;
    }

    public byte[] getBytes(){
	if (bytes == null){
	    int offset = 0;
	    bytes = new byte[getSize()];
	    // digest or command
	    bytes[offset++] = has_digest?(byte)1:(byte)0;
	    // write the client id
	    byte[] tmp = UnsignedTypes.longToBytes(clientId);
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	    // write the request id
	    tmp = UnsignedTypes.longToBytes(requestId);
	    for (int i = 0; i < tmp.length; i++, offset++)
		bytes[offset] = tmp[i];
	    // write the command command or digest
	    if (has_digest){
		tmp = UnsignedTypes.longToBytes(Digest.size());
		// size first
	    	for (int i = 0; i < tmp.length; i++, offset++)
		    bytes[offset] = tmp[i];
		// write the digest
		tmp = digest.getBytes();
		for (int i = 0; i < tmp.length; i++, offset++)
		    bytes[offset] = tmp[i];
	    } else{
		// size first
		tmp = UnsignedTypes.longToBytes(command.length);
		for (int i = 0; i < tmp.length; i++, offset++)
		    bytes[offset] = tmp[i];
		// write the command
		for (int i = 0; i < command.length; i++, offset++)
		    bytes[offset] = command[i];
	    }
		if (offset != getSize())
	    BFT.Debug.kill("off by one in entry.getbytes()");
	}

	return bytes;
    }


    Digest myDig;
    public boolean matches(Digest d){
	return d.equals(getMyDigest());
    }

    public Digest getMyDigest(){
	if (digest != null)
	    BFT.Debug.kill("Should never get my digest if the digest already exists");
	if (myDig == null)
	    myDig = new Digest(getBytes());
	return myDig;
    }

    public int getSize(){
	return ((command!=null)?command.length:Digest.size())+ 3 * MessageTags.uint32Size +1;
    }


    public boolean equals(Entry e){
	boolean res = getClient() == e.getClient() && 
	    getRequestId() == e.getRequestId() && has_digest == e.has_digest;
	if (!has_digest && res)
	    for (int i = 0;  res && i < command.length; i++){
		res = res && getCommand()[i] == e.getCommand()[i];
	    }
	else if (res)
	    res = digest.equals(e.digest);
	return res;
    }

    public String toString(){
	return clientId +":"+ requestId;
    }

//     long offset;
//     public void setOffset(long off){
// 	if (offset != -1)
// 	    BFT.Debug.kill("Cannot set an offset if one already exists");
// 	else
// 	    offset = off;
//     }

//     public long getOffset(){
// 	return offset;
//     }

 
//     public int compareTo(Entry o){
// 	if (getOffset() == o.getOffset())
// 	    return 0;
// 	else if (getOffset() < o.getOffset())
// 	    return -1;
// 	else 
// 	    return 1;
//     }

}