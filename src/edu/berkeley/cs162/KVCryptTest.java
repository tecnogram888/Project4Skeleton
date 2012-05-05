package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

import junit.framework.Assert;

import org.junit.Test;

public class KVCryptTest {
	@Test
	public void testAllAtOnce() {
		try {
		KVCrypt cryptor = new KVCrypt();
		KeyGenerator keygen = KeyGenerator.getInstance("DESede");
		SecretKey dougKey = keygen.generateKey();

		cryptor.setKey(dougKey);
		cryptor.setCipher();
		cryptor.setUp();
		assertEquals("Doug",(String) KVMessage.decodeObject(cryptor.decrypt(
				(byte[]) KVMessage.decodeObject(KVMessage.encodeObject(
						cryptor.encrypt(KVMessage.encodeObject("Doug")))))));
		} catch (Exception e){
			fail();
		}
	}

	@Test
	public void testEncryptDecrypt() {
		try{
		KVCrypt cryptor = new KVCrypt();
		KeyGenerator keygen = KeyGenerator.getInstance("DESede");
		SecretKey dougKey = keygen.generateKey();

		cryptor.setKey(dougKey);
		cryptor.setCipher();
		cryptor.setUp();
		String test1 = "Douglas James Davies";
		System.out.println(test1);
		String test2 = KVMessage.encodeObject(test1);
		System.out.println(test2);
		byte[] test3 = cryptor.encrypt(test2);
		System.out.println(test3);
		String test4 = KVMessage.encodeObject(test3);//Full encrypted/encoded
		System.out.println(test4);
		
		byte[] test5 = (byte[]) KVMessage.decodeObject(test4);//Should give back String test3
		System.out.println(test5);
		assertArrayEquals(test3, test5);
		String test6 = cryptor.decrypt(test5);//Encoded version of of the original encoded string?
		System.out.println(test6);
		assertEquals(test2, test6);
		String test7 = (String) KVMessage.decodeObject(test6);//Should be back now
		System.out.println(test7);
		assertEquals(test1, test7);
		
		
		
		
		

		
		
		
	
		} catch (Exception e){
			e.printStackTrace();
			fail();
		}
	    
	}

}
