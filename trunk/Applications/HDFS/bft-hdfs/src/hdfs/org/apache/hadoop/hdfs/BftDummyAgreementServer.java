package org.apache.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

public class BftDummyAgreementServer {

	private static boolean running;
	//private static int numDest = 1;
	
	DatagramSocket socket; // UDP socket
	Listener listener;
	InetSocketAddress[] destAddrs;
	
	SequenceNumberGenerator sng;
	
	Configuration conf;
	
	public BftDummyAgreementServer(int portNum) throws IOException{
		conf = new Configuration();
		socket = new DatagramSocket(portNum);
		System.out.println("Launching a dummy agreement server at "
				+ DNS.getDefaultIP("default") + ":" + portNum);
		listener = new Listener();

		String[] strDestAddrs;
		strDestAddrs = readDestAddrs();
		for(String s : strDestAddrs){
			System.out.println(s);
		}
		
		destAddrs = new InetSocketAddress[strDestAddrs.length];
	
		for(int i=0; i < strDestAddrs.length; i++){
			destAddrs[i] = NetUtils.createSocketAddr(strDestAddrs[i]);
		}
		
		sng = new SequenceNumberGenerator();
		
		sendRequest(null);
		
		listener.run();
	}

	private void sendRequest(BFTRequest req) {
		BFTOrderedRequest or = new BFTOrderedRequest(req);
		
		long seqNum = sng.getNextSequenceNumber();
		or.setSequenceNumber(seqNum);
		or.runThreadFunctions = true;
		
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(baos);

    try {
			or.write(dataOutput);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	      
		
		for(int i=0; i<destAddrs.length; i++){
			try {
				DatagramPacket p = new DatagramPacket(baos.toByteArray(), baos.size(), destAddrs[i]);
				//DatagramPacket p = new DatagramPacket(data, data.length, destAddrs[i]);
				System.out.println("Forwarding the request to " 
						+ p.getSocketAddress() + "\n" + or);
				socket.send(p);
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private String[] readDestAddrs() throws IOException{
		String replicas = conf.get("dfs.bft.namenode.replicaAddresses");
		System.out.println(replicas);
		return replicas.split(",");
		/*
		File file = new File("/tmp/replicaAddresses");
	
		BufferedReader input = new BufferedReader(new FileReader(file));
		String line;
		
		int i = 0;
		while( (line = input.readLine()) != null ){
			if(line.startsWith("//") || !line.matches("*:*") ){
				continue;
			}
			addrs[i++] = line;
			if(i >= addrs.length){
				break;
			}
		}
		*/
	}
	
	public void join(){
		try {
			listener.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private class SequenceNumberGenerator {
		private long sequenceNum;
		
		public SequenceNumberGenerator(){
			sequenceNum = 0;
		}
		
		synchronized long getNextSequenceNumber(){
			return sequenceNum++;
		}
		
	}
	
	private class Listener extends Thread {               

		private byte[] buffer;
		private DatagramPacket packet;

		private UserGroupInformation ticket;

		public Listener() {
			buffer = new byte[2048];
			packet = new DatagramPacket(buffer, buffer.length);
		}

		public void run() {
			while(running){	
				try {
					socket.receive(packet);
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println("Received a request from "
						+ packet.getSocketAddress());
				byte[] data = packet.getData();
				
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				DataInputStream dis = new DataInputStream(bais);
				Writable req = (Writable)ReflectionUtils.newInstance(BFTRequest.class, conf);
				try {
					req.readFields(dis);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					System.exit(-1);
				}
				
				BFTRequest bftReq = (BFTRequest)req;
				
				sendRequest(bftReq);


			}
		}

	}
	
	
	private static BftDummyAgreementServer createServer(String portNum) throws IOException{
		BftDummyAgreementServer.running = true;
		return new BftDummyAgreementServer(Integer.parseInt(portNum));
	}
	
	private static void destroyServer(){
		BftDummyAgreementServer.running = false;
	}
	
	private static void printUsage(){
		System.err.println("USAGE :");
		System.err.println(BftDummyAgreementServer.class.getSimpleName()
				+ " start (<portNumber>) / stop");
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		if(args.length < 1){
			printUsage();
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		BftDummyAgreementServer server;
		
		if(args[0].equalsIgnoreCase("start")){
			if(BftDummyAgreementServer.running){
				System.err.println("DummyAgreementServer is already running");
				System.exit(-1);
			}else{
				try {
					if(args.length < 2){
						String sockAddr = conf.get("dfs.bft.agreementServer.sockAddr","localhost:59770");
						server = createServer(sockAddr.split(":")[1]);
					} else {
						server = createServer(args[1]);
					}
					server.join();
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println(e.getMessage());
					System.exit(-1);
				}			
			}
		}else if(args[0].equalsIgnoreCase("stop")){
			if(BftDummyAgreementServer.running == false){
				System.err.println("The server is not running.");
			}else{
				destroyServer();
			}
		}else {
			printUsage();
			System.exit(-1);
		}
		
	}

}
