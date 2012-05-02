package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

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
	    
	    try {
			 cryptor.setKey(dougKey);
			 cryptor.setCipher();
			 cryptor.setUp();
			 String test1 = "Douglas James Davies";
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	   
	    
	}

}
