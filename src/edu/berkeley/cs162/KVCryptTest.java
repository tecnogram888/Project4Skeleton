package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

import org.junit.Test;

public class KVCryptTest {

	@Test
	public void testEncryptDecrypt() {
		KVCrypt cryptor = new KVCrypt();

		DESedeKeySpec keySpec = null;
		SecretKey dougKey = null;
		try {
			keySpec = new DESedeKeySpec("douglasJamesDaviesUCBerkeley".getBytes());//In the real version, use the system name?
			SecretKeyFactory kf = SecretKeyFactory.getInstance("DESede");
			dougKey = kf.generateSecret(keySpec);
			} catch (InvalidKeyException e) {
				fail();
			} catch (NoSuchAlgorithmException e) {
				fail();
			} catch (InvalidKeySpecException e) {
				fail();
			}
		String test1 = "Douglas James Davies";
	    try {
			 cryptor.setKey(dougKey);
			 cryptor.setCipher();
			 cryptor.setUp();
			 
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	    
	    try {
	    	String test11 = KVMessage.encodeObject(test1);
	    	byte[] test12 = cryptor.encrypt(test11);
	    	String test13 = KVMessage.encodeObject(test12);
	    	System.out.println("1: " + test1 + " 2:" + test11 + " 3:" + test12 + " 4:" + test13);
	    	byte[] test21 = (byte[])KVMessage.decodeObject(test13);
	    	String test22 = cryptor.decrypt(test21);
	    	System.out.println("1: " + test21 + " 2:" + test22);
	    	
	    	String d1 = "Doug";
	    	String d2 = KVMessage.encodeObject(d1);
	    	String d3 = (String)KVMessage.decodeObject(d2);
	    	System.out.println(d1 + " " + d2 + " " + d3);
	    	System.out.println(test12 + " " + test13 + " " + test21);
	    	
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	    	
	   
	    
	}

}
