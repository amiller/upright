// $Id$

package BFT.messages;

import BFT.util.UnsignedTypes;

public class NonDeterminism{

    private long time_offset = 1245540515840l;

    //    2^40 + 2^37
    
    //    2^40 + 2^36,...32

    // this value is not sustainable -- it is set to keep the time (as
    // of today) within a 32 bit unsigned integer of actual time

    public NonDeterminism(long _time, long nonDetSeeds){
	time = _time;
	seeds = nonDetSeeds;
	bytes = new byte[computeSize()];
	
	byte[] tmp;
	
	int offset = 0;
	tmp = UnsignedTypes.longlongToBytes(time);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
	tmp = UnsignedTypes.longlongToBytes(seeds);
	for (int i = 0; i < tmp.length; i++, offset++)
	    bytes[offset] = tmp[i];
    }
    
    

    public NonDeterminism(byte[] bites){
	int offset = 0;
	bytes = bites;
	byte[] tmp = new byte[MessageTags.uint64Size];
	for (int i = 0; i < tmp.length; i++, offset++){
	    tmp[i] = bytes[offset];
	}
	time = UnsignedTypes.bytesToLongLong(tmp);// + time_offset;
	for (int i = 0; i < tmp.length; i++, offset++){
	    tmp[i] = bytes[offset];
	}
	seeds = UnsignedTypes.bytesToLongLong(tmp);
    }


    protected byte[] bytes;
    protected long time;
    protected long seeds;

    public long getTime(){
	return time;
    }
    public long getSeed(){
	return seeds;
    }

    static public int size(){
		return MessageTags.uint64Size + MessageTags.uint64Size;

    }

    public int getSize(){
	return NonDeterminism.size();
    }


    /** 
     * returns byte represenation of the non-determinism. The byte
     * array does not include reference to size of the
     * non-determinism 
     **/
    public byte[] getBytes(){
	return bytes;
    }

    public String toString(){
	return UnsignedTypes.bytesToString(getBytes());
    }

    public boolean equals(NonDeterminism d){
	return time == d.time && seeds == d.seeds;
    }

    private final int computeSize(){
	return getSize();
    }


}