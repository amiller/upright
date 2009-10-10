package org.apache.zookeeper.server;

import BFT.Debug;
import BFT.fork.Forker;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.zookeeper.server.NIOServerCnxn.Factory;

public class ZKForker extends Forker {
	
	protected ZooKeeperServer zks;
	
	public ZKForker(ZooKeeperServer zks, long seqNo) {
		super();
		this.seqNo = seqNo;
		this.zks = zks;
	}

	public void run() {
		if(sysfork() == 0) {
			System.out.println("I've been forked and all I got was this lousy T-Shirt");
            String fileName = zks.takeSnapshot();
            try {
				OutputStreamWriter os = new FileWriter("filename.txt");
				os.write(fileName);
				os.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			sysexit();
		}
		else {
			sayForked();
			System.out.println("\tPARENT about to wait");
			syswait();
			try {
				InputStream is = new FileInputStream("filename.txt");
				byte[] target = new byte[is.available()];
				is.read(target);
				String fileName = new String(target);
				//Debug.println("CP file is " + fileName); 
				/*String shortFileName = new File(fileName).getName();

				FileInputStream fis = new FileInputStream(fileName); 
				byte[] data = new byte[1048576]; 
				long offset = 0; 
				ArrayList<ZKStateToken> list = new ArrayList<ZKStateToken>(); 
				while (fis.available() > 0) { 
					if (fis.available() < 1048576) data = new byte[fis.available()]; 
					int len	= fis.read(data); 
					ZKStateToken stateToken = new ZKStateToken(ZKStateToken.SNAPSHOT, shortFileName, offset, len, data);
					//Debug.println("Generate stateToken=" + stateToken);
					list.add(stateToken); System.out.println(stateToken.toString());
					offset += len; 
				}*/
				Factory scxn = zks.getServerCnxnFactory();
				scxn.syncDone(fileName);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
}
