/**
 * Client-side encryption using triple DES encryption (DES-EDE)
 * 
 * @author Karthik Reddy Vadde
 *
 * Copyright (c) 2011, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import javax.crypto.Cipher;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.InvalidKeyException;

public class KVCrypt {
    private static String algorithm = "DESede";
    private static SecretKey key = null;
    public static String keyStr = null;
    private static Cipher cipher = null;
    private static Cipher decipher = null;
    private static PKCS5Padding padder = null;

    public void setUp() throws Exception {
    	// implement me
    	cipher.init(Cipher.ENCRYPT_MODE, key);
    	cipher.init(Cipher.DECRYPT_MODE, key);
    	padder = new PKCS5Padding(8);//Block byte length = 8
    }
    
  

    public void setKey(SecretKey keyPar) throws Exception {
    	key = keyPar;
    }

    public void setCipher() throws Exception {
    	cipher = Cipher.getInstance(algorithm);
    	decipher = Cipher.getInstance(algorithm);
    }
    
    private byte[] dealWithPadding(byte[] input){
    	int blockSize = 8//Block byte length = 8
    	int totalLength = input.length + padder.padLength(input.length);
    	byte[] output = new byte[totalLength];
    	int len = blockSize - (len % blockSize);
    	byte paddingOctet = (byte) (len & 0xff);
        for (int i = 0; i < len; i++) {
            in[i + off] = paddingOctet;
        }
    }

    public byte[] encrypt(String input)
        throws InvalidKeyException, 
               BadPaddingException,
               IllegalBlockSizeException {
    	
    	return cipher.doFinal(padder.padWithLen(input.getBytes(), 0, padder.padLength(input.getBytes().length)));
    
    }

    public String decrypt(byte[] encryptionBytes)
        throws InvalidKeyException, 
               BadPaddingException,
               IllegalBlockSizeException {
    	
    	return new String(decipher.doFinal(encryptionBytes));
      }
}
