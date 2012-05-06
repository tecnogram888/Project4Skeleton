/**
 * Handle TPC connections over a socket interface
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 *
 * Copyright (c) 2012, University of California at Berkeley
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
import java.io.Serializable;
import java.net.Socket;
import java.util.Hashtable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.bind.DatatypeConverter;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler<K extends Serializable, V extends Serializable> implements NetworkHandler {
	private KeyServer<K, V> keyserver = null;
	private ThreadPool threadpool = null;
	private TPCLog<K, V> tpcLog = null;

	private boolean ignoreNext = false;
	private long SlaveID = -1;

	private Hashtable<K, ReentrantReadWriteLock> accessLocks = 
			new Hashtable<K, ReentrantReadWriteLock>();
	private ReentrantLock transactionLock = new ReentrantLock();
	private enum EState {
		NOSTATE, PUT_WAIT, DEL_WAIT, ABORT, COMMIT
	}
	private EState TPCState = EState.NOSTATE;
	private ReentrantLock TPCStateLock = new ReentrantLock();
	private TPCMessage putDelMessage;


	public TPCMasterHandler(KeyServer<K, V> keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KeyServer<K, V> keyserver, int connections) {
		this.keyserver = keyserver;
		threadpool = new ThreadPool(connections);	
	}

	@Override
	public void handle(Socket master) throws IOException {

		//TODO SOLOMON: ARE THERE ANY LOCKS IN HANDLE? I THINK THERE AREN'T, BUT CAN YOU DOUBLE-CHECK?
		
		// assume master is immortal
		// TODO uncomment line below
		// master.setSoTimeout(0);

		TPCMessage inputMessage = TPCMessage.receiveMessage(master);
		System.out.println("Received from Master at Slave " + SlaveID + ":");
		try {
			System.out.println(inputMessage.toXML()+"\n");
		} catch (KVException e) {
			e.printStackTrace();
			TPCMaster.exit();
		}

		//don't think we need this anymore
//		if(!inputMessage.getMsgType().equals("getreq") && 
//				!inputMessage.getMsgType().equals("ignoreNext"))
//			tpcLog.appendAndFlush(inputMessage);
		
		if (inputMessage.getMsgType().equals("putreq")) {
			tpcLog.appendAndFlush(new TPCMessage("ready", inputMessage.getKey(), inputMessage.getValue(), "putreq", inputMessage.getTpcOpId()));
		}
		if (inputMessage.getMsgType().equals("delreq")) {
			tpcLog.appendAndFlush(new TPCMessage("ready", inputMessage.getKey(), "delreq", inputMessage.getTpcOpId()));
		}
		if (inputMessage.getMsgType().equals("commit")) {
			tpcLog.appendAndFlush(new TPCMessage("commit", inputMessage.getTpcOpId()));
		}
		if (inputMessage.getMsgType().equals("abort")) {
			tpcLog.appendAndFlush(new TPCMessage("abort", inputMessage.getTpcOpId()));
		}
		
		
		switch (TPCState) {

		case NOSTATE:
			// we have to save the message or else we won't know what to put or delete
			// if we get a commit!
			putDelMessage = inputMessage;

			if (inputMessage.getMsgType().equals("getreq")){
				try {
					threadpool.addToQueue(new getRunnable((K)TPCMessage.decodeObject(inputMessage.getKey()), keyserver, master, inputMessage.getTpcOpId()));
				} catch (InterruptedException e) {
					sendMessage(master, new TPCMessage(new KVMessage("Unknown Error: Get Request failed -- InterruptedException from the threadpool"), "-1"));
					break;
				} catch (KVException e){
					sendMessage(master, new TPCMessage(e.getMsg(), "-1"));
					break;
				}
			} else if (inputMessage.getMsgType().equals("putreq")){
				if (ignoreNext == true){
					TPCMessage abortMsg = new TPCMessage("abort", "IgnoreNext Error: SlaveServer "+SlaveID+" has ignored this 2PC request during the first phase", inputMessage.getTpcOpId(), false);
					sendMessage(master, abortMsg);
					ignoreNext = false;
					TPCStateLock.lock();
					TPCState = EState.PUT_WAIT;
					TPCStateLock.unlock();
					break;
				}
				try {
					threadpool.addToQueue(new putRunnable<K,V>(
							(K)TPCMessage.decodeObject(inputMessage.getKey()), 
							(V)TPCMessage.decodeObject(inputMessage.getValue()), 
							keyserver, master, inputMessage.getTpcOpId(), inputMessage));
					break;
				} catch (InterruptedException e) {
					// send Abort response
					TPCMessage abortMsg = new TPCMessage("abort", "Unknown Error: InterruptedException from the threadpool", inputMessage.getTpcOpId(), false);
					sendMessage(master, abortMsg);
					TPCStateLock.lock();
					TPCState = EState.PUT_WAIT;
					TPCStateLock.unlock();
					break;
				} catch (KVException e){
					// send Abort response
					TPCMessage abortMsg = new TPCMessage("abort", e.getMsg().getMessage(), inputMessage.getTpcOpId(), false);
					sendMessage(master, abortMsg);
					TPCStateLock.lock();
					TPCState = EState.PUT_WAIT;
					TPCStateLock.unlock();
					break;
				}
			} else if (inputMessage.getMsgType().equals("delreq")){
				if (ignoreNext == true){
					TPCMessage abortMsg = new TPCMessage("abort", "IgnoreNext Error: SlaveServer "+SlaveID+" has ignored this 2PC request during the first phase", inputMessage.getTpcOpId(), false);
					sendMessage(master, abortMsg);
					ignoreNext = false;
					TPCStateLock.lock();
					TPCState = EState.DEL_WAIT;
					TPCStateLock.unlock();
					break;
				}
				try {
					threadpool.addToQueue(new delRunnable<K,V>(
							(K)TPCMessage.decodeObject(inputMessage.getKey()), 
							keyserver, master, inputMessage.getTpcOpId(), inputMessage));
					break;
				} catch (InterruptedException e) {
					// send Abort response
					TPCMessage abortMsg = new TPCMessage("abort", "Unknown Error: InterruptedException from the threadpool", inputMessage.getTpcOpId(), false);
					sendMessage(master, abortMsg);
					TPCStateLock.lock();
					TPCState = EState.DEL_WAIT;
					TPCStateLock.unlock();
					break;
				} catch (KVException e){
					// send Abort response
					TPCMessage abortMsg = new TPCMessage("abort", e.getMsg().getMessage(), inputMessage.getTpcOpId(), false);
					sendMessage(master, abortMsg);
					TPCStateLock.lock();
					TPCState = EState.DEL_WAIT;
					TPCStateLock.unlock();
					break;
				}
			} else if (inputMessage.getMsgType().equals("ignoreNext")){
				ignoreNext = true;
				TPCMessage response = new TPCMessage(new KVMessage("Success"), "-1");
				sendMessage(master, response);
				return;
			} else {
				// this should not happen.
				// TODO DOUBLE-CHECK
				System.err.println("TPCMasterHandler in NOSTATE, but didn't get a getreq, putreq, or delreq");
				TPCMaster.exit();
				break;
			}
			break;

		case PUT_WAIT: 
			// Sanity Check... make sure the message is a commit or abort message
			if (!inputMessage.getMsgType().equals("commit") && !inputMessage.getMsgType().equals("abort")){
				System.err.println("TPCMasterHandler in WAIT, but didn't get a getreq, putreq, or delreq");
				TPCMaster.exit();
			}

			if (inputMessage.getMsgType().equals("commit")){
				TPCStateLock.lock();
				TPCState = EState.COMMIT;
				TPCStateLock.unlock();
			} else if (inputMessage.getMsgType().equals("abort")){
				TPCStateLock.lock();
				TPCState = EState.ABORT;
				TPCStateLock.unlock();
			}
			
			// we have to reload the original message or else getKey() and getValue()
			// will throw NullPointers
			inputMessage = putDelMessage;
			
			//Sanity Check
			if (!inputMessage.getMsgType().equals("putreq")){
				// this should not happen
				System.err.println("failed Sanity Check 1 in TPCMasterHandler");
				TPCMaster.exit();
			}
			
			try {
				threadpool.addToQueue(new putRunnable<K,V>(
						(K)TPCMessage.decodeObject(inputMessage.getKey()), 
						(V)TPCMessage.decodeObject(inputMessage.getValue()), 
						keyserver, master, inputMessage.getTpcOpId(), inputMessage));
				break;
			} catch (InterruptedException e) {
				// TODO figure out what to do here
				System.err.println("PUT_WAIT had an InterruptedException");
				TPCMaster.exit();
				return;
			} catch (KVException e){
				// TODO figure out what to do here
				System.err.println("PUT_WAIT had a KVException: " + e.getMsg().getMessage());
				TPCMaster.exit();
				return;
			}
		case DEL_WAIT: 
			// Sanity Check... make sure the message is a commit or abort message
			if (!inputMessage.getMsgType().equals("commit") && !inputMessage.getMsgType().equals("abort")){
				System.err.println("TPCMasterHandler in WAIT, but didn't get a getreq, putreq, or delreq");
			}
			
			if (inputMessage.getMsgType().equals("commit")){
				TPCStateLock.lock();
				TPCState = EState.COMMIT;
				TPCStateLock.unlock();
			} else if (inputMessage.getMsgType().equals("abort")){
				TPCStateLock.lock();
				TPCState = EState.ABORT;
				TPCStateLock.unlock();
			}
			
			// we have to reload the original message or else getKey()
			// will throw NullPointer
			inputMessage = putDelMessage;
			
			//Sanity Check
			if (!inputMessage.getMsgType().equals("delreq")){
				// this should not happen
				System.err.println("failed Sanity Check 2 in TPCMasterHandler");
				TPCMaster.exit();
			}
			
			try {
				threadpool.addToQueue(new delRunnable<K,V>(
						(K)TPCMessage.decodeObject(inputMessage.getKey()), 
						keyserver, master, inputMessage.getTpcOpId(), inputMessage));
				break;
			} catch (InterruptedException e) {
				// TODO figure out what to do here
				System.err.println("DEL_WAIT had an InterruptedException");
				break;
			} catch (KVException e){
				// TODO figure out what to do here
				System.err.println("DEL_WAIT had a KVException: " + e.getMsg().getMessage());
				break;
			}		
		default:
			// this should pretty much never happen
			System.err.println("TPCMasterHandler -- handle somehow got to the default case");
			break;
		}
	}

	class getRunnable implements Runnable {
		K key;
		KeyServer<K, V> keyserver;
		Socket master;
		String TpcOpID;

		public getRunnable(K key, KeyServer<K,V> keyserver, Socket master, String TpcOpID){
			this.key = key;
			this.keyserver = keyserver;
			this.master = master;
			this.TpcOpID = TpcOpID;
		}

		@Override
		public void run() {
			ReentrantReadWriteLock accessLock = accessLocks.get(key);
			if (accessLock == null) {
				accessLocks.put(key, new ReentrantReadWriteLock());
				accessLock = accessLocks.get(key);
			}
			accessLock.readLock().lock();

			// call get function and send answer to master
			V value = null;
			try {
				value = keyserver.get(key);
			} catch (KVException e) {
				sendMessage(master, new TPCMessage(e.getMsg(), "-1"));
				accessLock.readLock().unlock();
				return;
			}

			// create the response TPCMessage
			KVMessage KVresponse = null;
			TPCMessage TPCresponse = null;
			try {
				// create get response
				KVresponse = new KVMessage("resp", KVMessage.encodeObject(key), KVMessage.encodeObject(value));
				TPCresponse = new TPCMessage(KVresponse, TpcOpID);
			} catch (KVException e){
				sendMessage(master, new TPCMessage(e.getMsg(), "-1"));
				accessLock.readLock().unlock();
				return;
			}

			// send the TPCMessage to the master
			// send the get request to slave
			System.out.println("Sending to Master from Slave " + SlaveID + ":");
			try {
				System.out.println(TPCresponse.toXML()+"\n");
			} catch (KVException e) {
				e.printStackTrace();
				TPCMaster.exit();
			}
			sendMessage(master, TPCresponse);
			// TODO should I still close it here, I close it in handle()
			try {
				master.close();
			} catch (IOException e) {
				// These ones don't send errors, this is a server error
				e.printStackTrace();
			}
			accessLock.readLock().unlock();
		}
	}

	class putRunnable<K extends Serializable, V extends Serializable>implements Runnable {
		K key;
		V value;
		KeyServer<K,V> keyserver;
		Socket master;
		String TpcOpID;
		// this is more or less redundant, but is still used for efficiency purposes
		TPCMessage message;

		public putRunnable(K key, V value, KeyServer<K,V> keyserver, Socket master, String TpcOpID, TPCMessage message){
			this.key = key;
			this.value = value;
			this.keyserver = keyserver;
			this.master = master;
			this.TpcOpID = TpcOpID;
			this.message = message;
		}
		@Override
		public void run() {
			transactionLock.lock();

			switch (TPCState) {
			case NOSTATE: 
				// check to see if putreq will send back an Abort response or a Ready response
				if (!checkKey(message.getKey())){
					// send Abort response
					TPCMessage abortMessage = new TPCMessage("abort", "Over sized key", message.getTpcOpId(), false);
					sendMessage(master, abortMessage);
					TPCStateLock.lock();
					TPCState = EState.PUT_WAIT;
					TPCStateLock.unlock();
					break;
				} else if (!checkValue(message.getValue())){
					// send Abort response
					TPCMessage abortMessage = new TPCMessage("abort", "Over sized value", message.getTpcOpId(), false);
					sendMessage(master, abortMessage);
					TPCStateLock.lock();
					TPCState = EState.PUT_WAIT;
					TPCStateLock.unlock();
					break;
				} else{
					TPCMessage readyMessage = new TPCMessage("ready", TpcOpID);
					sendMessage(master, readyMessage);		
					TPCStateLock.lock();
					TPCState = EState.PUT_WAIT;
					TPCStateLock.unlock();
					break;
				}
			case COMMIT:
				try {
					keyserver.put(key, value);
					System.out.println("Put " + key + ", " + value + " into keyServer!\n");
					System.out.println(keyserver.get(key)+"\n");
				} catch (KVException e) {
					System.err.println("put COMMIT failed");
					TPCMaster.exit();
				}
				// send acknowledgment
				TPCMessage ackMessage = new TPCMessage("ack", TpcOpID);
				sendMessage(master, ackMessage);
				TPCStateLock.lock();
				TPCState = EState.NOSTATE;
				TPCStateLock.unlock();
				break;	
			case ABORT:
				// send acknowledgment
				ackMessage = new TPCMessage("ack", TpcOpID);
				sendMessage(master, ackMessage);
				TPCStateLock.lock();
				TPCState = EState.NOSTATE;
				TPCStateLock.unlock();
				break;
			default:
				// this should pretty much should NEVER happen
				System.err.println("TPCMasterHandler -- putRunnable somehow got to the default case");
				break;
			}
			// TODO should I still close it here, I close it in handle()
			try {
				master.close();
			} catch (IOException e) {
				// These ones don't send errors, this is a server error
				e.printStackTrace();
			}
			transactionLock.unlock();
		}
	}

	class delRunnable<K extends Serializable, V extends Serializable>implements Runnable {
		K key;
		KeyServer<K,V> keyserver;
		Socket master;
		String TpcOpID;
		// this is more or less redundant, but is still used for efficiency purposes
		TPCMessage message;

		public delRunnable(K key, KeyServer<K,V> keyserver, Socket master, String TpcOpID, TPCMessage message){
			this.key = key;
			this.keyserver = keyserver;
			this.master = master;
			this.TpcOpID = TpcOpID;
			this.message = message;
		}

		@Override
		public void run() {
			transactionLock.lock();

			switch (TPCState) {

			case NOSTATE: 
				// check to see if delreq will send back an Abort response or a Ready response
				if (!checkKey(message.getKey())){
					TPCMessage abortMessage = new TPCMessage("abort", "Over sized value", message.getTpcOpId(), false);
					sendMessage(master, abortMessage);
					TPCStateLock.lock();
					TPCState = EState.DEL_WAIT;
					TPCStateLock.unlock();
					break;
				} else {
					// test to see if key is actually inside the server
					try {
						keyserver.get(key);
					} catch (KVException e) {
						// this means that key is not inside of keyserver
						TPCMessage abortMessage = new TPCMessage("abort", "Key doesn't exist", message.getTpcOpId(), false);
						sendMessage(master, abortMessage);
						TPCStateLock.lock();
						TPCState = EState.DEL_WAIT;
						TPCStateLock.unlock();
						break;
					}

					TPCMessage readyMessage = new TPCMessage("ready", TpcOpID);
					sendMessage(master, readyMessage);
					TPCStateLock.lock();
					TPCState = EState.DEL_WAIT;
					TPCStateLock.unlock();
					break;
				}
			case COMMIT:
				/* Correctness Constraints:
				 * key is definitely in keyserver
				 * 
				 */
				try {
					keyserver.del(key);
					System.out.println("Deleted " + key + " from keyServer!\n");
				} catch (KVException e) {
					// this breaks the correctness constraint
					System.err.println("Delete COMMIT failed when it wasn't supposed to");
					e.printStackTrace();
					TPCMaster.exit();
				}
				// send acknowledgment
				TPCMessage ackMessage = new TPCMessage("ack", TpcOpID);
				sendMessage(master, ackMessage);
				TPCStateLock.lock();
				TPCState = EState.NOSTATE;
				TPCStateLock.unlock();
				break;
			case ABORT:
				// send acknowledgment
				ackMessage = new TPCMessage("ack", TpcOpID);
				sendMessage(master, ackMessage);
				TPCStateLock.lock();
				TPCState = EState.NOSTATE;
				TPCStateLock.unlock();
				break;
			default:
				// this should pretty much should NEVER happen
				System.err.println("TPCMasterHandler -- delRunnable somehow got to the default case");
				break;
			}

			try {
				// TODO should I still close it here, I close it in handle()
				master.close();
			} catch (IOException e) {
				// These ones don't send errors, this is a server error
				e.printStackTrace();
			}
			transactionLock.unlock();
		}
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog<K, V> tpcLog) {
		this.tpcLog  = tpcLog;
	}

	private boolean checkKey(String key){	
		byte[] decoded = DatatypeConverter.parseBase64Binary(key);
		// make sure the length of the byte array is less than 128KB
		if (decoded.length > 256) {
			return false;
		} else{
			return true;
		}
	}

	private boolean checkValue(String value){	
		byte[] decoded = DatatypeConverter.parseBase64Binary(value);
		// make sure the length of the byte array is less than 128KB
		if (decoded.length > 128 * java.lang.Math.pow(2,10)) {
			return false;
		} else{
			return true;
		}
	}
	
	private void sendMessage(Socket master, TPCMessage message){
		System.out.println("Sending message to Master from Slave " + SlaveID + ":");
		try {
			System.out.println(message.toXML()+"\n");
		} catch (KVException e) {
			e.printStackTrace();
			TPCMaster.exit();
		}
		TPCMessage.sendMessage(master, message);
	}


}
