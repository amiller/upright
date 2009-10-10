//$Id$

package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class BftSecondaryNameNode extends SecondaryNameNode {

	private static final int HASH_BUF_SIZE = 4096;
	private static final String BFT_SIG_FILENAME = "bftSignature";
	private static final String BFT_IMAGE_FILENAME = "fsImage";

	private static MessageDigest md;
	
	static {
		try {
			md = MessageDigest.getInstance("SHA");
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}
	}

	private Hashtable<BftCheckpointSignature,String> checkpoints;
	private String checkpointRootDir;
	private long checkpointID;
	
	
	class LogQueue {
		LinkedList<String> logs;
		
		public LogQueue(){
			logs = new LinkedList<String>();
		}
		
		synchronized public void enqueue(String s){
			logs.addLast(s);
			notifyAll();
		}
		
		synchronized public String dequeue(){
			String l = logs.removeFirst();
			notifyAll();
			/*
			while(logs.isEmpty()){
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			return l;//logs.removeFirst();
		}
		
		synchronized public String getFirst(){
			while(logs.isEmpty()){
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return logs.getFirst();
		}
		
		synchronized public void waitUntilEmpty(){
			while(!logs.isEmpty()){
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

	LogQueue logQ;
	NewBftCP lastCP;
	LogConsumer logConsumer;
	HashSet<NewBftCP> cpSet;
	Server server;

	public BftSecondaryNameNode(Configuration conf) throws IOException{
		super(conf);
		this.initialize(conf);

	}
	
	public void initialize(Configuration conf){
		//logs = new LinkedList<String>();
		logQ = new LogQueue();
		lastCP = null;
		cpSet = new HashSet<NewBftCP>();
		checkpointRootDir = "/tmp/bftgluecp/";
		File cpDirRoot = 	new File(checkpointRootDir);
		cpDirRoot.mkdirs();
		
		//server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
    //    handlerCount, false, conf);
		
		logConsumer = new LogConsumer();
		logConsumer.start();
		
	}

	/*
	private void initialize(Configuration conf){
		checkpoints = new Hashtable<BftCheckpointSignature, String>();
		checkpointRootDir = conf.get("bft.checkpoint.rootdir","/tmp/hadoop/bft/checkpoint/");

		File cpDirRoot = 	new File(checkpointRootDir);
		cpDirRoot.mkdirs();

		File[] cpDirs = cpDirRoot.listFiles();

		for(File dir : cpDirs){
			try {
				//read signature
				DataInput in = new DataInputStream(
						new BufferedInputStream(
								new FileInputStream(
										dir.getCanonicalPath() 
										+ File.separatorChar + BFT_SIG_FILENAME)));
				BftCheckpointSignature bftSig = new BftCheckpointSignature();
				bftSig.readFields(in);

				checkpoints.put(bftSig, dir.getCanonicalPath());

			} catch (IOException e) {
				try {
					System.err.println("Failed to read bft checkpoint from "
							+ dir.getCanonicalPath() + " : " + e.getMessage());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();

			}
		}

		checkpointID = 0;
	}
	*/
	
	public byte[] getStateInBytes(NewBftCP bftcp) throws IOException{
		int size = 0;
		File base = new File(bftcp.basefile);
		size += base.length();
		size += Long.SIZE /8 ;
		for(String s : bftcp.logs){
			size += Long.SIZE /8 ;
			size += (new File(s)).length();
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
		DataOutputStream dos = new DataOutputStream(baos);
		
		dos.writeLong(base.length());
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(base));
		byte[] buf = new byte[1024];
		int len = 0;
		while ((len = in.read(buf, 0, buf.length)) > 0){
			dos.write(buf, 0, len);
		}
		in.close();
		for(String s : bftcp.logs){
			dos.writeLong((new File(s)).length());
			in = new BufferedInputStream(new FileInputStream(s));
			len = 0;
			while ((len = in.read(buf, 0, buf.length)) > 0){
				dos.write(buf, 0, len);
			}
			in.close();
		}
		dos.flush();
		return baos.toByteArray();

	}
	
	public byte[] getStateInBytes(BftCheckpointSignature bftSig) 
	throws IOException{

		String cpDir = checkpoints.get(bftSig);
		if(cpDir == null){
			String msg = "Checkpoint not found : " + bftSig.toString();
			throw new NoSuchBftCheckpointFoundException(msg);
		}
		
		String img = cpDir + File.separator + BFT_IMAGE_FILENAME;
		
		File imgFile = new File(img);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream((int)imgFile.length());		
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(imgFile));
		byte[] buf = new byte[1024];
		int len = 0;
		while ((len = in.read(buf, 0, buf.length)) > 0){
			baos.write(buf, 0, len);
		}
		
		return baos.toByteArray();
		
	}
	
	public void removeCheckpoint(BftCheckpointSignature bftSig){
		if(checkpoints.containsKey(bftSig)){
			String cpDirStr = checkpoints.get(bftSig);
			File cpDir = new File(cpDirStr);
			for(File f : cpDir.listFiles()){
				f.delete();
			}
			cpDir.delete();
		}
		checkpoints.remove(bftSig);
	}
	
	public void addCheckpoint(BftCheckpointSignature bftSig, byte[] state){
		if(checkpoints.containsKey(bftSig)){
			return;
		}
		
		// create directory for this checkpoint
		String cpDir = null;
		File dir;
		try {
			do{
				cpDir = checkpointRootDir + (checkpointID++) + "/" ;
				dir = new File(cpDir);
			} while(checkpoints.containsValue(dir.getCanonicalPath()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		dir.mkdirs();
					
		// write signature in the directory
		try{
			DataOutput out = new DataOutputStream(
					new BufferedOutputStream(
						new FileOutputStream(cpDir + BFT_SIG_FILENAME)));
			bftSig.write(out);
		} catch (IOException e){
			dir.delete();
			return;
		}
				
		BufferedOutputStream out2 = null;
		try{
			File f = new File(cpDir + BFT_IMAGE_FILENAME);			
			out2 = new BufferedOutputStream(new FileOutputStream(f));			
			out2.write(state);
		} catch (IOException e) {
			e.printStackTrace();
			(new File(cpDir + BFT_SIG_FILENAME)).delete();
			dir.delete();
			return;
		} finally {
			if(out2 != null){
				try {
					out2.close();
				} catch (IOException e) {
					e.printStackTrace();
					(new File(cpDir + BFT_SIG_FILENAME)).delete();
					dir.delete();
					return;
				}
			}
		}
		
		checkpoints.put(bftSig, cpDir);
				
	}

	
	class LogConsumer extends Thread{
		//LinkedList<String> mylogs;
		boolean running;
		//public LogConsumer(LinkedList<String> logs){
		public LogConsumer(){
			//mylogs = logs;
		}
		
		public void run(){
			running = true;
			System.out.println("# LOG CONSUMER BEGIN");
			while(running){
				String log = logQ.getFirst();
				/*
				synchronized(mylogs){
					while(mylogs.isEmpty() && running){
						try {
							System.out.println("# LOG CONSUMER SLEEP");
							mylogs.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if(mylogs.isEmpty()) return;
					log = mylogs.getFirst();
				}*/
				
				File[] srcNames = checkpointImage.getEditsFiles();
				
				//String orgName = new String(log.getAbsolutePath());
				System.out.println("Org name : " + log);
				try {
					if ( srcNames[0].exists() ){
						srcNames[0].delete();
					}
					copyfile(log, srcNames[0].getAbsolutePath());
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				//log.renameTo(srcNames[0]);
				StorageDirectory sdEdits = null;
		    Iterator<StorageDirectory> it = null;
		    //try {
					//checkpointImage.getEditLog().open();
				//} catch (IOException e1) {
					// TODO Auto-generated catch block
				//	e1.printStackTrace();
				//}
		    it = checkpointImage.dirIterator(NameNodeDirType.EDITS);
		    if (it.hasNext())
		      sdEdits = it.next();
				try {
					System.out.println("##### Loading logs... " + log + " by " + this.getId());
					checkpointImage.loadFSEdits(sdEdits);
				} catch (IOException e) {					
					e.printStackTrace();
					System.err.println("LOADFSEDIT FAIL");
					System.exit(-1);
				}
				//System.out.println("Back from " + log.getAbsolutePath() + "to : " + orgName);
				//log.renameTo(new File(orgName));
				
				logQ.dequeue();
				/*
				synchronized(mylogs){
					mylogs.removeFirst();
					mylogs.notifyAll();
				}*/
				
			}
			
		}
		
		public void shutdown(){
			running = false;
			/*
			synchronized(mylogs){
				mylogs.notifyAll();
			}*/
		}
		
		
	}
	
	
	private void createInitialCP() throws IOException{
		doCheckpoint();
		
		File initialBase = new File(checkpointRootDir + "base_init"); 
		//System.out.println("cp from " + checkpointImage.getImageFiles()[0].getAbsolutePath() +
		//		" to "+initialBase.getAbsolutePath());
		if(checkpointImage.getImageFiles()[0].exists()){
			System.out.println(checkpointImage.getImageFiles()[0].getAbsolutePath()+" not exist");
		}
		copyfile(checkpointImage.getImageFiles()[0].getAbsolutePath(),
				initialBase.getAbsolutePath());
		
		lastCP = new NewBftCP(initialBase.getAbsolutePath(),
				generateHash(initialBase), new LinkedList<String>());
		
		cpSet.add(lastCP);
		
	}
	
	public NewBftCP myBigCheckpoint(long seq) throws IOException{
		
		CheckpointSignature sig;
		sig = namenode.rollEditLog();
		File editlog = new File(namenode.getEditLogName());
		//File dummylog = new File(editlog.getAbsolutePath()+"_dummy");
		String log = checkpointRootDir + "editlog_" + seq;
		copyfile(editlog.getAbsolutePath(), log);
		//System.out.println("BigCP : Getting log " + log);
		/*
		editlog.renameTo(log);
		dummylog.createNewFile();
		dummylog.renameTo(editlog);
		*/
		//namenode.fakeUpload(sig);
		
		File fakeNewCP = new File(namenode.getNewCPName());
		fakeNewCP.createNewFile();
		
		namenode.rollFsImage();
		
		/*
		LinkedList<File> oldlogs = logs;
		logs = new LinkedList<File>();

		synchronized(oldlogs){
			oldlogs.add(log);
			oldlogs.notifyAll();
		}
		synchronized(oldlogs){
			
			while(oldlogs.size() >0){
				try {
					oldlogs.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
			}
		}*/
		
		logQ.enqueue(log);
		/*
		synchronized(logs){
			logs.add(log);
			logs.notifyAll();
		}*/
		/*
		synchronized(logs){
			while(!logs.isEmpty()){
				try {
					logs.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}*/
		
		File newBaseFile = new File(checkpointRootDir + "base_"+seq);
		logQ.waitUntilEmpty();
		System.out.println("Save Base File");
		checkpointImage.saveFSImage(newBaseFile);
		//checkpointImage.endCheckpoint();
		//checkpointImage.getImageFiles()[0].renameTo(newBaseFile);
		//checkpointImage.startCheckpoint();
		//lastCP = new NewBftCP(newBaseFile.getAbsolutePath(), new byte[2],new LinkedList<String>());
		//lastCP = new NewBftCP(lastCP, log.getAbsolutePath());
		lastCP = new NewBftCP(newBaseFile.getAbsolutePath(), generateHash(newBaseFile),new LinkedList<String>());
		cpSet.add(lastCP);
		//logConsumer.shutdown();
		//logConsumer = new LogConsumer(logs);
		//logConsumer.start();
		
		return lastCP;
	}

	
	public NewBftCP mydoCheckpoint(long seqNo) throws IOException{		
		
		if(lastCP == null){
			createInitialCP();
		}
		
		//System.out.println("1 myDoCheckpoint seqNo "+ seqNo);
				
		CheckpointSignature sig;
		
		sig = namenode.rollEditLog();
		File editlog = new File(namenode.getEditLogName());
		//File dummylog = new File(editlog.getAbsolutePath()+"_dummy");
		String log = checkpointRootDir + "editlog_" + seqNo;
		//System.out.println("LittleCP : Getting log " + log);
		copyfile(editlog.getAbsolutePath(), log);
		/*
		editlog.renameTo(log);
		dummylog.createNewFile();
		dummylog.renameTo(editlog);
		*/
		//namenode.fakeUpload(sig);
		
		File fakeNewCP = new File(namenode.getNewCPName());
		fakeNewCP.createNewFile();
		//System.out.println("created :"+fakeNewCP.getAbsolutePath());
		namenode.rollFsImage();
		//System.out.println("10 myDoCheckpoint seqNo "+ seqNo);
		
		
		
		
		logQ.enqueue(log);
		/*
		synchronized(logs){
			logs.add(log);
			logs.notifyAll();
		}*/
		
		lastCP = new NewBftCP(lastCP, new String(log));
		cpSet.add(lastCP);
		return lastCP;
		
		/*
		md.reset();
		md.update(getPreviousHash());
		md.update(generateHash(srcNames[0]));
		byte[] hash = md.digest();
		
		getCPReady();
		
		BftCheckpointSignature bftSig = new BftCheckpointSignature(sig, hash);
		
		checkpointImage.checkpointUploadDone();				
		
		return bftSig;*/
	}

	public BftCheckpointSignature takeCheckpoint() throws IOException {
		//System.out.println("CALLING doCP()");
		CheckpointSignature sig = doCheckpoint();
		//System.out.println("Return from doCP()");
		
		byte[] hash = null;
		try{
			hash = generateHash(checkpointImage.getImageFiles()[0]);
		} catch(IOException e){
			System.err.println("Exception while generating hash: " + e.getMessage());
			return null;
		}

		//System.out.println("Hash generated : " + new String(hash));

		//BftCheckpointSignature bftSig = new BftCheckpointSignature(sig, hash);
		byte[] tmphash = new byte[20];
		for (byte b : tmphash){
			b = 'a';
		}
		BftCheckpointSignature bftSig = new BftCheckpointSignature(sig, tmphash);
		// create directory for this checkpoint
		String cpDir = null;
		File dir;
		do{
			cpDir = checkpointRootDir + (checkpointID++) + "/" ;
			dir = new File(cpDir);
		} while(checkpoints.containsValue(dir.getCanonicalPath()));
		dir.mkdirs();

		// write signature in the directory
		BufferedOutputStream bis = new BufferedOutputStream(
				new FileOutputStream(cpDir + BFT_SIG_FILENAME));
		DataOutput out = new DataOutputStream(bis);
		bftSig.write(out);
		bis.close();
		

		// and move the image file to the directory
		File cpSrc = checkpointImage.getImageFiles()[0];
		String cpDst = cpDir + BFT_IMAGE_FILENAME;
		//System.out.println("Copying from " + cpSrc.getCanonicalPath() 
		//		+ " to " + cpDst + "");

		boolean succeed = true; //cpSrc.renameTo(new File(cpDst));
		
		try{
			copyfile(cpSrc.getCanonicalPath(), cpDst);
		} catch (IOException e){
			succeed = false;
		}

		if(succeed){
			checkpoints.put(bftSig, cpDir);
			//System.out.println("Succeed in copying cp");
		}	else {
			(new File(cpDir + BFT_SIG_FILENAME)).delete();
			dir.delete();
			//System.out.println("Failed copying cp");
		}

		return bftSig;
	}

	public void uploadCheckpoint(BftCheckpointSignature bftSig) throws IOException {

		String cpDir = checkpoints.get(bftSig);
		if(cpDir == null){
			String msg = "Checkpoint not found : " + bftSig.toString();
			throw new NoSuchBftCheckpointFoundException(msg);
		}

		String img = cpDir + File.separator + BFT_IMAGE_FILENAME;
		
		
		/*

		namenode.rollEditLog();

		String img = cpDir + File.separator + BFT_IMAGE_FILENAME;

		File[] dsts = super.checkpointImage.getImageFiles();
		//File[] dsts = super.checkpointImage.getFileNames(NameNodeFile.IMAGE_NEW, NameNodeDirType.IMAGE);

		for(File dst : dsts){
			copyfile(img, dst.getCanonicalPath());
		}

		putFSImage(bftSig);
		namenode.rollFsImage();
		*/
	}

	protected static byte[] generateHash(File cp) throws IOException{

		md.reset();
		BufferedInputStream in = new BufferedInputStream(
				new FileInputStream(cp));

		byte[] buf = new byte[HASH_BUF_SIZE];
		int read = 0;

		try{
			while((read = in.read(buf, 0, buf.length)) > 0){
				md.update(buf, 0, read);
			}
		} finally {
			if(in!=null){
				in.close();
			}
		}

		return md.digest();    

	}

	private static void copyfile(String srFile, String dtFile) throws IOException{
		System.err.println("Copying file from : " + srFile + " to " + dtFile);
		BufferedInputStream in = null;
		BufferedOutputStream out = null;
		try{
			File f1 = new File(srFile);
			File f2 = new File(dtFile);
			in = new BufferedInputStream(new FileInputStream(f1));
			out = new BufferedOutputStream(new FileOutputStream(f2));

			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf, 0, buf.length)) > 0){
				out.write(buf, 0, len);
			}
		} finally {
			if(in != null){
				in.close();
			}
			if(out != null){
				out.close();
			}
		}
	}

	public void copyCheckpoint(BftCheckpointSignature bftSig,
			String namenodeImageLocation) throws IOException{
		
		String cpDir = checkpoints.get(bftSig);
		if(cpDir == null){
			String msg = "Checkpoint not found : " + bftSig.toString();
			throw new NoSuchBftCheckpointFoundException(msg);
		}

		String img = cpDir + File.separator + BFT_IMAGE_FILENAME;
		
		copyfile(img, namenodeImageLocation);
		
	}
	
	public void join(){
		
	}
	
	public static void main(String[] args) throws IOException{
		
		BftSecondaryNameNode bftSNN = new BftSecondaryNameNode(new Configuration());
		bftSNN.join();
	}

}
