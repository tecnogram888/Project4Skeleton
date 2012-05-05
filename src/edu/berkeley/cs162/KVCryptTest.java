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
	public void testEncryptDecrypt() {
		try{
		KVCrypt cryptor = new KVCrypt();

		DESedeKeySpec keySpec = null;
		SecretKey dougKey = null;
		KeyGenerator keygen = KeyGenerator.getInstance("DESede");
		dougKey = keygen.generateKey();

		cryptor.setKey(dougKey);
		cryptor.setCipher();
		cryptor.setUp();
		String test1 = "Douglas James Davies";
		System.out.println(test1);
		String test2 = null;
			test2 = KVMessage.encodeObject(test1);
		System.out.println(test2);
		byte[] test3 = null;
		test3 = cryptor.encrypt(test2);
		System.out.println(test3);
		String test4 = KVMessage.encodeObject(test3);//Full encrypted/encoded
		System.out.println(test4);
		byte[] test5 = (byte[]) KVMessage.decodeObject(test4);//Should give back String test3
		System.out.println(test5);
		//assertTrue(test5.equals(test3));
		
		byte[] a = {1, 2, 3, 4, 5};
		System.out.println(a);
		byte[] b = (byte[])KVMessage.decodeObject(KVMessage.encodeObject(a));
		System.out.println(b);
		Assert.
		
		
	
		} catch (Exception e){
			e.printStackTrace();
			fail();
		}
	    
	}

}
