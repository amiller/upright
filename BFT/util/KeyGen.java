/**
 * $Id$
 */
package BFT.util;

import java.security.*;
import java.security.spec.*;
import java.security.interfaces.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import java.io.*;
import java.util.*;
import java.math.*;
import BFT.Parameters;

/**
 * @author riche
 *
 */
public class KeyGen {

	private PrivateKey privKey;
	private PublicKey pubKey;
	private byte[] sigBytes;
	private Signature sigObj = null;
	private final String testString = "TAYLORasfasdfkja;sldkfja;sldfj saf;lkasdjf;lkasf;laskjhk;asjhf;asdlkjhf;aslkfh;aslkf;lakjdfha;lksfj";

	public KeyGen() {

	}

	public KeyGen(boolean BC) {
		if(!BC) {
			try {
				this.sigObj= Signature.getInstance("MD5withRSA");
				KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
				keyGen.initialize(1024);
				KeyPair keyPair = keyGen.generateKeyPair();
				this.privKey = keyPair.getPrivate();
				this.pubKey = keyPair.getPublic();

				sigBytes = null;

			}
			catch (Exception e) {
				System.err.println(e.getLocalizedMessage());
				e.printStackTrace();
				System.exit(1);			
			}
		}
		else {
			try {
				String dir = "./lib/security"; 
				//System.out.println(dir);
				//new File(dir).mkdirs(); 
				this.sigObj= Signature.getInstance("MD5withRSA", BFT.Parameters.provider);
				KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", BFT.Parameters.provider);
				keyGen.initialize(1024);
				KeyPair keyPair = keyGen.generateKeyPair();
				this.privKey = keyPair.getPrivate();
				this.pubKey = keyPair.getPublic();

				sigBytes = null;

			}
			catch (Exception e) {
				System.err.println(e.getLocalizedMessage());
				e.printStackTrace();
				System.exit(1);			
			}
		}
	}

	public int getSigSize() {
		return sigBytes.length;
	}

	/**
	 * @param keyString A {@link String} object containing the hex representation of the base and exponent separated by a ":".
	 * @return A {@link RSAPublicKey} object that can be used to verify signatures.
	 */
	public static RSAPublicKey getPubKeyFromString(String keyString) {
		RSAPublicKey key = null;
		try {
			String[] splits = keyString.split(":");
			BigInteger mod = new BigInteger(splits[0], 16);
			BigInteger exp = new BigInteger(splits[1], 16);
			RSAPublicKeySpec pubKeySpec = new RSAPublicKeySpec(mod, exp);
			KeyFactory kf = KeyFactory.getInstance("RSA", BFT.Parameters.provider);
			key = (RSAPublicKey)kf.generatePublic(pubKeySpec);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
		return key;
	}

	/**
	 * @param keyString A {@link String} object containing the hex representation of the base and exponent separated by a ":".
	 * @return A {@link RSAPrivateKey} object that can be used to verify signatures.
	 */
	public static RSAPrivateKey getPrivKeyFromString(String keyString) {
		RSAPrivateKey key = null;
		try {
			String[] splits = keyString.split(":");
			BigInteger mod = new BigInteger(splits[0], 16);
			BigInteger exp = new BigInteger(splits[1], 16);
			RSAPrivateKeySpec privKeySpec = new RSAPrivateKeySpec(mod, exp);
			KeyFactory kf = KeyFactory.getInstance("RSA", BFT.Parameters.provider);
			key = (RSAPrivateKey)kf.generatePrivate(privKeySpec);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
		return key;
	}

	public static Mac getMacObjectfromString(String keyString) {
		try {
			BigInteger biKey = new BigInteger(keyString, 16);
			SecretKeySpec sks = new SecretKeySpec(biKey.toByteArray(), mactype);
			Mac retMac = Mac.getInstance(mactype);
			retMac.init(sks);
			return retMac;
		} catch (InvalidKeyException e) {
			BFT.Debug.kill(e);
		} catch (NoSuchAlgorithmException e) {
			BFT.Debug.kill(e);
		} 
		return null;
	}

	//	public void fileStuff() {
	//		try {
	//			KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", "BC");
	//			KeyPair keyPair = keyGen.generateKeyPair();
	//			RSAPrivateKey privKey = (RSAPrivateKey)keyPair.getPrivate();
	//			String privKeyModString = privKey.getModulus().toString(16).toUpperCase();
	//			String privKeyExpString = privKey.getPrivateExponent().toString(16).toUpperCase();
	//			BufferedWriter outputStream = new BufferedWriter(new FileWriter("test.txt"));
	//			outputStream.write(privKeyModString + "" + privKeyExpString);
	//			outputStream.close();
	//			Scanner inputStream = new Scanner(new BufferedReader(new FileReader("test.txt")));
	//			String readModString = inputStream.next();
	//			String readExpString = inputStream.next();
	//			BigInteger readMod = new BigInteger(readModString, 16);
	//			BigInteger readExp = new BigInteger(readExpString, 16);
	//			RSAPrivateKeySpec privKeySpec = new RSAPrivateKeySpec(readMod, readExp);
	//			KeyFactory kf = KeyFactory.getInstance("RSA", "BC");
	//			RSAPrivateKey newPK = (RSAPrivateKey)kf.generatePrivate(privKeySpec);
	//		} catch (Exception e) {
	//			System.err.println(e.getLocalizedMessage());
	//			e.printStackTrace();
	//			System.exit(1);
	//		}
	//		
	//	}

	public void sign() {
		try {
			sigObj.initSign(privKey);
			sigObj.update(testString.getBytes());
			sigBytes = sigObj.sign();
		}
		catch (Exception e) {
			System.err.println(e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void verify() {
		try{
			sigObj.initVerify(pubKey);
			sigObj.update(testString.getBytes());
			if(!sigObj.verify(sigBytes)) throw new RuntimeException("BAD VERIFY");
		}
		catch (Exception e) {
			System.err.println(e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	public KeyStringPair keysToString(KeyPair kp) {
		if(kp.getPrivate().getAlgorithm().equals("RSA")) {
			return new KeyStringPair(keyToString((RSAPublicKey)kp.getPublic()), keyToString((RSAPrivateKey)kp.getPrivate()));
		}
		else {
			throw new RuntimeException("Not Implemented");
		}
	}

	public String keyToString(RSAPublicKey rk) {
		String retString = null;
		String pubKeyModString = rk.getModulus().toString(16).toUpperCase();
		String pubKeyExpString = rk.getPublicExponent().toString(16).toUpperCase();
		retString = pubKeyModString + ":" + pubKeyExpString;
		return retString;
	}

	public String keyToString(RSAPrivateKey rk) {
		String retString = null;
		String privKeyModString = rk.getModulus().toString(16).toUpperCase();
		String privKeyExpString = rk.getPrivateExponent().toString(16).toUpperCase();
		retString = privKeyModString + ":" + privKeyExpString;
		return retString;
	}

	public String keyToString(PublicKey pk) {
		throw new RuntimeException("Not Implemented");
	}

	public String keyToString(PrivateKey pk) {
		throw new RuntimeException("Not Implemented");
	}

	public String keyToString(SecretKey sk) {
		return null;
	}

	static final int TF = 5000;

	public static final int SHA1size = 20;
	public static final int MD5size = 16;
	public static final int macsize = SHA1size;
	public static final String SHA1 = "HmacSHA1";
	public static final String MD5 = "HmacMD5";
	public static final String mactype = SHA1;


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length == 0) {
				System.err.println("Please Specify Mode");
			}
			else if (args[0].equals("--generate")) {
				if(args.length == 2) {
					KeyGen kg = new KeyGen();
					int numMachines = Integer.parseInt(args[1]);
					KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
					for(int i = 0; i < numMachines; i++) {
						KeyPair keyPair = keyGen.generateKeyPair();
						KeyStringPair kpStrings = kg.keysToString(keyPair);
						System.out.print("PUB = ");
						System.out.println(kpStrings.getPubString());
						System.out.print("PRIV = ");
						System.out.println(kpStrings.getPrivString());

						KeyGenerator keyGen2 = KeyGenerator.getInstance(mactype);
						SecretKey sk = keyGen2.generateKey();
						BigInteger keyInt = new BigInteger(sk.getEncoded());
						String keyString = keyInt.toString(16).toUpperCase();
						System.out.print("SECRET = ");
						System.out.println(keyString);
					}
				}
				else {
					System.err.println("Missing number of keys to generate");
				}
			}
			else if (args[0].equals("--generateMac")) { 
				if(args.length == 2) {
					KeyGenerator keyGen = KeyGenerator.getInstance("HmacSHA1");
					int num = Integer.parseInt(args[1]);
					for (int i = 0; i < num; i++) {
						SecretKey sk = keyGen.generateKey();
						BigInteger keyInt = new BigInteger(sk.getEncoded());
						String keyString = keyInt.toString(16).toUpperCase();
						System.out.print("SECRET = ");
						//System.out.println(keyString);
					}
				}
				else {
					System.err.println("Missing number of keys to generate");
				}
			}
			else if (args[0].equals("--test")) {
				Date now = new Date();
				KeyGen kg = new KeyGen(false);
				long start = now.getTime();
				for (int i = 0; i < KeyGen.TF; i++) {
					kg.sign();
				}
				now = new Date();
				long end = now.getTime();
				System.out.println("Avg. Sign time for Sun: "
						+ new Double((new Double(end - start) / KeyGen.TF)));
				System.out.flush();
				now = new Date();
				start = now.getTime();
				for (int i = 0; i < KeyGen.TF; i++) {
					kg.verify();
				}
				now = new Date();
				end = now.getTime();
				System.out.println("Avg. Verify time for Sun: "
						+ new Double((new Double(end - start) / KeyGen.TF)));
				Security.addProvider(new de.flexiprovider.core.FlexiCoreProvider());
				now = new Date();
				kg = new KeyGen(true);
				start = now.getTime();
				for (int i = 0; i < KeyGen.TF; i++) {
					kg.sign();
				}
				now = new Date();
				end = now.getTime();
				System.out.println("Avg. Sign time for FC: "
						+ new Double((new Double(end - start) / KeyGen.TF)));
				System.out.flush();
				now = new Date();
				start = now.getTime();
				for (int i = 0; i < KeyGen.TF; i++) {
					kg.verify();
				}
				now = new Date();
				end = now.getTime();
				System.out.println("Avg. Verify time for FC: "
						+ new Double((new Double(end - start) / KeyGen.TF)));

			} 
			else if (args[0].equals("--size")) {
				KeyGen kg = new KeyGen(false);
				kg.sign();
				//System.out.println("Sig Size: " + kg.getSigSize());
			}
			else {
				System.err.println("Unknown Mode");
			}
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}

}
