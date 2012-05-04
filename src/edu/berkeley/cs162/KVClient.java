/**
 * Client component for generating load for the KeyValue store. 
 * This is also used by the Master server to reach the slave nodes.
 * 
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;


/**
 * This class is used to communicate with (appropriately marshalling and unmarshalling) 
 * objects implementing the {@link KeyValueInterface}.
 *
 * @param <K> Java Generic type for the Key
 * @param <V> Java Generic type for the Value
 */
public class KVClient<K extends Serializable, V extends Serializable> implements KeyValueInterface<K, V> {

	private String server = null;
	private int port = 0;
	private SecretKey masterKey;
	private KVCrypt crypt;
	
	/**
	 * @param server is the DNS reference to the Key-Value server
	 * @param port is the port on which the Key-Value server is listening
	 */
	public KVClient(String server, int port) {
		this.server = server;
		this.port = port;
		this.crypt = new KVCrypt();
		try {
			setUpKey();
			crypt.setKey(masterKey);
			crypt.setCipher();
			crypt.setUp();
		} catch (KVException e) {
			// TODO How to handle this? The system is broken at this point... Maybe retry?
			e.printStackTrace();
		} catch (Exception e) {
			// TODO How to handle this? The system is broken at this point... Maybe retry?
			e.printStackTrace();
		}
		
	}
	
	private void setUpKey() throws KVException {
		KVMessage keyMess = sendRecieve(new KVMessage("getEnKey", null, null));
		if (!"resp".equals(keyMess.getMsgType())){
			throw new KVException(new KVMessage("Incorrect server response to encryption key request"));
		}
		String s = keyMess.getMessage();
		masterKey = (SecretKey)KVMessage.decodeObject(s);
		
	}
	
	private KVMessage sendRecieve(KVMessage message) throws KVException {
		String xmlFile = message.toXML();
		Socket connection;
		PrintWriter out = null;
		InputStream in = null;
		try {
			connection = new Socket(server, port);
		} catch (UnknownHostException e) {
			throw new KVException(new KVMessage("Network Error: Could not connect"));
		} catch (IOException e) {
			throw new KVException(new KVMessage("Network Error: Could not create socket"));
		}
		try {
			//TODO uncomment line below, assume Master is immorta
			//connection.setSoTimeout(0);
			connection.setSoTimeout(15000);
		} catch (SocketException e1) {
			throw new KVException(new KVMessage("Unknown Error: Could net set Socket timeout"));
		}
		try {
			out = new PrintWriter(connection.getOutputStream(),true);
			out.println(xmlFile);
			connection.shutdownOutput();
		} catch (IOException e) {
			throw new KVException(new KVMessage("Network Error: Could not send data"));
		}
		try {
			in = connection.getInputStream();
			message = new KVMessage(in);
			in.close();
		} catch (IOException e) {
			throw new KVException(new KVMessage("Network Error: Could not receive data"));
		}
		out.close();
		try {
			connection.close();
		} catch (IOException e) {
			throw new KVException(new KVMessage("Unknown Error: Could not close socket"));
		}
		return message;
	}
	
	@Override
	public boolean put(K key, V value) throws KVException {
		if (key == null) throw new KVException(new KVMessage("Empty key"));
		if (value == null) throw new KVException(new KVMessage("Empty value"));
		if (key instanceof String) {
			if( ((String) key).isEmpty()) throw new KVException(new KVMessage("Empty key"));
		}
		if (value instanceof String) {
			if( ((String) value).isEmpty()) throw new KVException(new KVMessage("Empty value"));
		}
		String keyString = KVMessage.encodeObject(key);
		String valueString = KVMessage.encodeObject(value);
		try {
			valueString = KVMessage.encodeObject(crypt.encrypt(valueString));
		} catch (InvalidKeyException e) {
			// TODO What should we do if encryption breaks?
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO What should we do if encryption breaks?
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO What should we do if encryption breaks?
			e.printStackTrace();
		}
		if (keyString.isEmpty()) throw new KVException(new KVMessage("Empty key"));
		if (valueString.isEmpty()) throw new KVException(new KVMessage("Empty value"));

		KVMessage message = new KVMessage("putreq", keyString, valueString);

		message = sendRecieve(message);

		// message.msgType should be "resp." If not, throw KVException
		if (!message.getMsgType().equals("resp")) throw new KVException(new KVMessage("Unknown Error: response xml not a response!!"));

		// If the message is not "Success," it'll have an error message inside the return xml
		if (!"Success".equals(message.getMessage())) throw new KVException(new KVMessage(message.getMessage()));

		// return the boolean status
		return message.getStatus();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(K key) throws KVException {
		if (key == null) throw new KVException(new KVMessage("Unknonw Error: get was called with null argument"));
		String keyString = KVMessage.encodeObject(key);
		KVMessage message = new KVMessage("getreq", keyString);

		message = sendRecieve(message);			

		if (!message.getMsgType().equals("resp")) throw new KVException(new KVMessage("Unknown Error: response xml not a response!!"));

		if ("Does not exist".equals(message.getMessage())){
			throw new KVException(new KVMessage(message.getMessage()));
		} else {
			if (message.getValue() == null) throw new KVException(new KVMessage("Unknown Error: Get received \"null\" in value in the response"));
			byte[] encryptedBytes = (byte[])KVMessage.decodeObject(message.getValue());
			String valueString = null;
			try {
				valueString = crypt.decrypt(encryptedBytes);
			} catch (InvalidKeyException e) {
				// TODO What should we do if encryption breaks?
				e.printStackTrace();
			} catch (BadPaddingException e) {
				// TODO What should we do if encryption breaks?
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) {
				// TODO What should we do if encryption breaks?
				e.printStackTrace();
			}
			
			return (V)KVMessage.decodeObject(valueString);
		}
	}

	@Override
	public void del(K key) throws KVException {
		if (key == null) throw new KVException(new KVMessage("Unknonw Error: del was called with null argument"));
		String keyString = KVMessage.encodeObject(key);
		KVMessage message = new KVMessage("delreq", keyString);
		message = sendRecieve(message);
		if (!message.getMsgType().equals("resp")) throw new KVException(new KVMessage("Unknown Error: response xml not a response!!"));
		if (!"Success".equals(message.getMessage())) throw new KVException(new KVMessage(message.getMessage()));
	}
}
