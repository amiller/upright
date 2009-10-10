// $Id$

package BFT.messages;

import BFT.messages.VerifiedMessageBase;

/**
    certificate.

   Unfortunately we cannot use Generics to implement a handful of
   generic certificates -- Generic types must be of the form <T
   extends Parent> and not <T implements Interface>.
 **/

public class Quorum<T extends VerifiedMessageBase>{

    T val;
    VerifiedMessageBase[] entries;

    int added;
    int quorumSize;
    int indexBase;

    public Quorum(int maxSize, int targetSize, int offsetBase){
	entries = new VerifiedMessageBase[maxSize];
	val = null;
	quorumSize = targetSize;
	indexBase = offsetBase;
	added = 0;
    }

    public boolean isComplete(){
	return added >= quorumSize;
// 	int count = 0;
// 	for (int i = 0; i < entries.length; i++)
// 	    if (entries[i]!=null){
// 		count++;
// 	    }
// 	return count >= quorumSize;
    }

    /**
       returns true if the entry is successfully added to the current
       set.  returns false if the quorum must be reset to accomodate
       the entry
     **/
    public boolean addEntry(T rep){
	if (val == null || val.matches(rep)){
	    if (val == null && rep.isValid()){
		val = rep;
	    }
	    else if (val == null)
		return false;
	    if (entries[(int)(rep.getSender()-indexBase)] == null)
		added++;
	    entries[(int)(rep.getSender()-indexBase)] = rep;
	    return true;
	}
	else if (rep.isValid()){ // gonna throw out all the old ones now!
	    clear();
	    val = rep;
	    entries[(int)(rep.getSender()-indexBase)] = rep;
	    added++;
	    return false;
	}else
	    return false;
    }

    public T[] getEntries(){
	return (T[])entries;
    }

    public boolean containsEntry(int i){
	return entries[i] != null;
    }
    
    public T getEntry(){
	return val;
    }

    public void clear(){
	//System.out.println("clearing quorum cert"); 
	for (int i = 0;  i < entries.length; i++)
	    entries[i] = null;
	val = null;
	added = 0;
    }

}