// $Id$

package BFT.util;

/**
   Utility functions for converting between unsigned integer and longs
   and java types.

   Maps unsigned 16 bit integers to int and unsigned 32 bit integers to long.

   Relies on network byte order for the byte representation.
 **/
public class UnsignedTypes{

    
    final public static long minusOne = 4294967295l;
    
    /**
       Iinput: 2 bytes
       
       Output: java int (4 byte) representation of that
               int. Effectively a 16 bit unsigned int
     **/
    public static int bytesToInt(byte[] buf){
	if (buf.length != 2)
	    throw new RuntimeException("Invalid buffer length");
	char anUnsignedInt = 0;

        int firstByte = 0;
        int secondByte = 0;

	int index = 0;
	
        firstByte = (0x000000FF & ((int)buf[index]));
        secondByte = (0x000000FF & ((int)buf[index+1]));
	index = index+2;
	anUnsignedInt  = (char) (firstByte << 8 | secondByte);
	
	return anUnsignedInt;
    }

  /**
       input:  java int
       outputs: byte represention of an unsigned 16 bit integer
    **/
    public static byte[] intToBytes(int val){
	byte[] buf = new byte[2];
	buf[0] = (byte) ((val & 0xFF00) >> 8);
	buf[1] = (byte) (val & 0x00FF);
	return buf;
    }
    

    
    /** Input: 4 bytes

	Output: a java long (8 bytes) representation of the unsigned
	        32 bit long
    **/
    public static long bytesToLong(byte[] buf){
	if (buf.length != 4) 
	    throw new RuntimeException("invalid byte array length");

	long anUnsignedInt = 0;

        int firstByte = 0;
        int secondByte = 0;
        int thirdByte = 0;
        int fourthByte = 0;

        firstByte = (0x000000FF & ((int)buf[0]));
        secondByte = (0x000000FF & ((int)buf[1]));
        thirdByte = (0x000000FF & ((int)buf[2]));
        fourthByte = (0x000000FF & ((int)buf[3]));
	anUnsignedInt  = ((long) (firstByte << 24
	                | secondByte << 16
                        | thirdByte << 8
                        | fourthByte))
                       & 0xFFFFFFFFL;

	return anUnsignedInt;
    }

 /** 
	input:  java long
	output:  byte representation of an unsigned 32 bit long
     **/
    public static byte[] longToBytes(long val){
	byte[] buf = new byte[4];
	buf[0] = (byte) ((val & 0xFF000000L) >> 24);
	buf[1] = (byte) ((val & 0x00FF0000L) >> 16);
	buf[2] = (byte) ((val & 0x0000FF00L) >> 8);
	buf[3] = (byte) (val & 0x000000FFL);
	return buf;
	
    }

 /** Input: 8 bytes

	Output: a java long (8 bytes) representation of the unsigned
	        64 bit long
    **/
    public static long bytesToLongLong(byte[] buf){
	if (buf.length != 8) 
	    throw new RuntimeException("invalid byte array length");

	long anUnsignedInt = 0;

        long firstByte = 0;
        long secondByte = 0;
        long thirdByte = 0;
        long fourthByte = 0;
	long fifthByte = 0;
	long sixthByte = 0;
	long seventhByte= 0;
	long eightByte = 0;

        firstByte = (0x00000000000000FFL & ((long)buf[0]));
        secondByte = (0x00000000000000FFL & ((long)buf[1]));
        thirdByte = (0x00000000000000FFL & ((long)buf[2]));
        fourthByte = (0x00000000000000FFL & ((long)buf[3]));
	fifthByte = (0x00000000000000FFL & ((long)buf[4]));
        sixthByte = (0x00000000000000FFL & ((long)buf[5]));
        seventhByte = (0x00000000000000FFL & ((long)buf[6]));
        eightByte = (0x00000000000000FFL & ((long)buf[7]));
	anUnsignedInt  = ((long) (firstByte << 56
				  | secondByte << 48
				  | thirdByte << 40
				  | fourthByte << 32
				  | fifthByte << 24 
				  | sixthByte << 16
				  | seventhByte << 8
				  | eightByte ))
	    & 0xFFFFFFFFFFFFFFFFL;

	return anUnsignedInt;
    }

 /** 
	input:  java long
	output:  byte representation of an unsigned 32 bit long
     **/
    public static byte[] longlongToBytes(long val){
	byte[] buf = new byte[8];
	buf[0] = (byte) ((val & 0xFF00000000000000L) >> 56);
	buf[1] = (byte) ((val & 0x00FF000000000000L) >> 48);
	buf[2] = (byte) ((val & 0x0000FF0000000000L) >> 40);
	buf[3] = (byte) ((val & 0x000000FF00000000L) >> 32);
	buf[4] = (byte) ((val & 0x00000000FF000000L) >> 24);
	buf[5] = (byte) ((val & 0x0000000000FF0000L) >> 16);
	buf[6] = (byte) ((val & 0x000000000000FF00L) >> 8);
	buf[7] = (byte)  (val & 0x00000000000000FFL);
	return buf;
	
    }



    public static String bytesToString(byte[] buf){
	String tmp = "";
	for (int i = 0; i < buf.length; i++){
	    tmp+=buf[i]+"\t";
	    if (i % 16 == 15)
		tmp += "\n";
	}
	return tmp;
    }

    public static void printBytes(byte[] buf){
	printBytes(buf, 0, buf.length);
    }
    public static void printBytes(byte[] buf, int start, int finish){
	int integer;
    for (int i = start; i < finish; i++){
        integer = buf[i] & 0xFF;
	    System.out.print(integer+"\t");
	    if ((i-start) % 8 ==7)
		System.out.println();
	}
    }


    public static void main(String args[]){
	
	long longval = 0;
	int intval = 0;

	//System.out.println("i\tint\t\tb1\tb2\t\tint(b(i))\tlong\t\tb1\tb2\tb3\tb4\t\tlong(b(l))");
	System.out.print(-1+"\t"+intval+"\t\t");
	printBytes(intToBytes(intval));
	System.out.print("\t"+bytesToInt(intToBytes(intval))+"\t\t");
	System.out.print(longval+"\t\t");
	printBytes(longToBytes(longval));
	//System.out.println("\t"+bytesToLong(longToBytes(longval)));
	intval++;
	longval++;

	for (int i = 0; i<= 32; i++){
	    System.out.print(i+"\t");
	    System.out.print(intval+"\t");
	    if (i < 24 || i == 32)
		System.out.print("\t");
	    printBytes(intToBytes(intval));
	    System.out.print("\t"+bytesToInt(intToBytes(intval))+"\t\t");
	    System.out.print(longval+"\t");
	    if (i < 24 )
		System.out.print("\t");
	    printBytes(longToBytes(longval));
	    //System.out.println("\t"+bytesToLong(longToBytes(longval)));
	    intval*=2;
	    longval*=2;
	}
	//System.out.println("i\tint\t\tb1\tb2\t\tint(b(i))\tlong\t\tb1\tb2\tb3\tb4\t\tlong(b(l))");
	
	
	System.out.println("testing long long");
	long i = 2;
	for (; i > 0; i = (i-1)*2+1)
	    System.out.println(i+" : " + bytesToString(longlongToBytes(i))+ " : "+ bytesToLongLong(longlongToBytes(i)));
	
	



    }

}