// $Id$
package Applications.hashtable;

import java.io.*;
import java.util.*;

import BFT.generalcp.*;
import BFT.serverShim.ShimBaseNode;

//The server class must implement the two interfaces
public class HTServer implements AppCPInterface{

	// The hashtable. We use LinkedHashMap instead of HashMap, since the iteration of HashMap is nondeterministic.
	LinkedHashMap<String, DataUnit> ht;
	int writeCount=0;
	CPAppInterface generalCP=null;
   

	// Directories to store log and sync files.
	private String syncDir=null;
	private int id=0;
    
	public HTServer(String syncDir, int id) throws IOException {
		this.syncDir=syncDir+File.separator;
		this.id=id;
		ht = new LinkedHashMap<String, DataUnit>();
		File syncDir2 = new File(this.syncDir);
                syncDir2.mkdirs();
	}
	
	public void setGenCP(CPAppInterface cp) {
		generalCP = cp;
	}
	
	// Execute the request
	@Override
	public synchronized void execAsync(byte[] request, RequestInfo info) {
		HTRequest req = (HTRequest)(Convert.bytesToObject(request));
		HTReply rep = null;
		String key = req.getKey();
		if(req.getType() == HTRequest.READ){
			if(ht.containsKey(key)){
				rep = new HTReply(false, ht.get(key));
			}else{
				rep = new HTReply(true, null);
			}
		} else {	// WRITE operation
			DataUnit data=new DataUnit(req.getValue(), info.getRandom(), info.getTime());
			ht.put(key, data);
			writeCount++;
			rep = new HTReply(false, null);
		}
		generalCP.execDone(Convert.objectToBytes(rep), info);
		
	}

	@Override
	public void execReadonly(byte[] request, int clientId, long requestId){
		throw new RuntimeException("Not implemented");
	}
    
	// Write all states into a snapshot file
	@Override
	public void sync() {
        	try {
			File syncFile=new File(this.syncDir+"ht_sync_"+writeCount);
            		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(syncFile));
            		oos.writeObject(ht);
			oos.writeInt(writeCount);
			oos.close();
        	}
		catch (Exception e) {
            		e.printStackTrace();
        	}
        	generalCP.syncDone(this.syncDir+"ht_sync_"+writeCount);
    	}

	// Load all states from a snapshot file 
	@Override
	public synchronized void loadSnapshot(String fileName) {
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
			ht = (LinkedHashMap<String, DataUnit>) ois.readObject();
			writeCount = ois.readInt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	
	
	public static void main(String args[]) throws Exception{
		if(args.length!=4){
			System.out.println("Usage: java Applications.hashtable <id> <config_file> <log_path> <snapshot_path>");
		}
        	GeneralCP generalCP = new GeneralCP(Integer.parseInt(args[0]), args[1], args[2], args[3]);
        	HTServer main = new HTServer(args[3],Integer.parseInt(args[0]));
        	main.setGenCP(generalCP);
        	generalCP.setupApplication(main);
	}

}
