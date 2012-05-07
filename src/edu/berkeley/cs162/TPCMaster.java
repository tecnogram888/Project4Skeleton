/**
 * Master for Two-Phase Commits
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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.DESedeKeySpec;

public class TPCMaster<K extends Serializable, V extends Serializable>  {

	/**
	 * Implements NetworkHandler to handle registration requests from 
	 * SlaveServers.
	 * 
	 */
	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			// Call the other constructor
			this(1);	
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);	
		}

		@Override
		public void handle(Socket client) throws IOException {
			try {
				threadpool.addToQueue(new registrationRunnable(client));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private class registrationRunnable implements Runnable{
			private Socket client;

			public registrationRunnable (Socket _client){
				this.client = _client;
			}

			public void run(){
				SlaveInfo newSlave = null;
				TPCMessage registration = null;

				// read registration message from SlaveServer
				try {
					registration = new TPCMessage(client.getInputStream());
					newSlave = new SlaveInfo(registration.getMessage());
				} catch (KVException e) {
					System.err.println("error reading registration message");
				} catch (IOException e) {
					System.err.println("error reading input stream");
				}

				addToConsistentHash(newSlave);
				if (consistentHash.size() >= listOfSlaves.length)
					synchronized(TPCMaster.this){
						TPCMaster.this.notifyAll();
					}

				TPCMessage msg = new TPCMessage("Successfully registered"+newSlave.slaveID+"@"+newSlave.hostName+":"+newSlave.port);

				TPCMessage.sendMessage(client, msg);
			}
		}
	}

	/**
	 *  Data structure to maintain information about SlaveServers
	 *
	 */
	private class SlaveInfo {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;

		// Variables to be used to maintain connection with this SlaveServer
		private KVClient<K, V> kvClient = null;
		private Socket kvSocket = null;

		/**
		 * 
		 * @param slaveInfo as "SlaveServerID@HostName:Port"
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {
			// added by luke
			int indexAT = slaveInfo.indexOf("@");
			int indexCOLON= slaveInfo.indexOf(":");
			// indexAT is not included
			String slaveIDString = slaveInfo.substring(0,indexAT);
			String hostName = slaveInfo.substring(indexAT+1, indexCOLON);
			String portString = slaveInfo.substring(indexCOLON + 1);
			long slaveID;
			int port;
			try{
				slaveID = Long.decode(slaveIDString);
				port = Integer.parseInt(portString);
			} catch (IllegalArgumentException e){
				e.printStackTrace();
				System.err.println("IllegalArgumentException in SlaveInfo constructor");
				throw new KVException(new KVMessage("Registration Error: Received unparseable slave information"));
			}
			this.slaveID = slaveID;
			this.port = port;
			this.hostName = hostName;

		}

		public long getSlaveID() {
			return slaveID;
		}

		public KVClient<K, V> getKvClient() {
			return kvClient;
		}

		public Socket getKvSocket() {
			return kvSocket;
		}

		public void setKvSocket(Socket kvSocket) {
			this.kvSocket = kvSocket;
		}

		public String getHostName() {
			return hostName;
		}

		public int getPort() {
			return port;
		}

	}

	//
	DESedeKeySpec keySpec = null;
	SecretKey masterKey = null;

	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 0;

	// Cache stored in the Master/Coordinator Server
	private KVCache<K, V> masterCache = new KVCache<K, V>(1000);

	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;

	// ID of the next 2PC operation
	private Long tpcOpId = 0L;

	private SortedMap<Long, SlaveInfo> consistentHash = new TreeMap<Long, SlaveInfo>();

	private SocketServer clientServer = null;

	private ThreadPool threadpool = null;

	private Hashtable<String, ReentrantReadWriteLock> accessLocks = 
			new Hashtable<String, ReentrantReadWriteLock>();

	String abortMessage = "";
	String getReturnValue;
	boolean getFinished = false;

	private Long currentTpcOpId = -1L;
	private Integer finishedCount = 0;
	private Integer finishedCount2 = 0;
	private Integer allDone = 0;
	
	private ReentrantLock transactionLock = new ReentrantLock();
	private boolean canCommit = false;
	private ReentrantLock canCommitLock = new ReentrantLock();
	private enum EState {
		NOSTATE, INIT, ABORT, COMMIT
	}
	private EState TPCState = EState.NOSTATE;
	private ReentrantLock TPCStateLock = new ReentrantLock();
	//added by Doug
	private ReentrantReadWriteLock consistantHashLock = new ReentrantReadWriteLock();

	public String[] listOfSlaves;


	/**
	 * Creates TPCMaster using SlaveInfo provided as arguments and SlaveServers 
	 * actually register to let TPCMaster know their presence
	 * 
	 * @param listOfSlaves list of SlaveServers in "SlaveServerID@HostName:Port" format
	 * @throws Exception
	 */
	public TPCMaster(String[] slaves) throws Exception {
		listOfSlaves = slaves;
		// Create registration server
		regServer = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 9090);
		regServer.addHandler(new TPCRegistrationHandler(1));

		// delayed start ThreadPool
		threadpool = new ThreadPool(10); //TODO: how many threads?
		KeyGenerator keygen = KeyGenerator.getInstance("DESede");
		masterKey = keygen.generateKey();
	}

	public SecretKey getMasterKey() {
		return masterKey;
	}

	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation
	 * it is a long variable that increases by one for each 2PC operation. 
	 * 
	 * @return 
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();		
	}

	/**
	 * Start registration server in a separate thread
	 */
	public void run() {
		// create a runnable and thread for regServer
		class regServerRunnable implements Runnable {

			@Override 
			public void run() {
				try {
					regServer.connect();
					regServer.run();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		Thread regServerThread = new Thread(new regServerRunnable());
		regServerThread.start();

	}

	/**
	 * Converts Strings to 64-bit longs
	 * Borrowed from http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * @param string String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L; 
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}

	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned data types except for char)
	 * Borrowed from http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * @param n1 First long
	 * @param n2 Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}

	private boolean isLessThanEqualUnsigned(long n1, long n2) {
		return isLessThanUnsigned(n1, n2) || n1 == n2;
	}	

	/** 
	 * Add the SlaveInfo to the consistent hash table
	 * @param newSlave
	 */
	public synchronized void addToConsistentHash(SlaveInfo newSlave) {

		Long x = newSlave.getSlaveID();
		consistantHashLock.writeLock().lock();
		consistentHash.put(x, newSlave);
		consistantHashLock.writeLock().unlock();
	}

	/**
	 * Find first/primary replica location
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(K key) {
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());

		consistantHashLock.readLock().lock();
		if (consistentHash.isEmpty()) { return null; }
		Long x = ((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(hashedKey);
		if (x == null) {

			SlaveInfo temp = consistentHash.get(
					((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(consistentHash.firstKey()) );
			consistantHashLock.readLock().unlock();
			return temp;
		}
		SlaveInfo temp = consistentHash.get(x);
		consistantHashLock.readLock().unlock();
		return temp;
	}

	/**
	 * Find the successor of firstReplica to put the second replica
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		consistantHashLock.readLock().lock();
		if (consistentHash.isEmpty()) return null;
		SlaveInfo temp = consistentHash.get(
				((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(firstReplica.getSlaveID() + 1) );
		if (temp == null) {
			return consistentHash.get(
					((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(consistentHash.firstKey()) );
		}
		consistantHashLock.readLock().unlock();
		return temp;
	}

	/**
	 * Synchronized method to perform 2PC operations one after another
	 * 
	 * @param msg
	 * @param isPutReq
	 * @return True if the TPC operation has succeeded
	 * @throws KVException
	 */
	public synchronized boolean performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		transactionLock.lock();

		// get the next TPC Op ID
		String TPCOpId = getNextTpcOpId();

		// create TPCMessage from msg
		TPCMessage TPCmess = new TPCMessage(msg, TPCOpId);		

		// get the accessLock
		ReentrantReadWriteLock accessLock = accessLocks.get(TPCmess.getKey());
		if (accessLock == null) {
			accessLocks.put(TPCmess.getKey(), new ReentrantReadWriteLock());
			accessLock = accessLocks.get(TPCmess.getKey());
		}
		accessLock.writeLock().lock();

		// Set TPCState to INIT
		TPCStateLock.lock();
		TPCState = EState.INIT;
		TPCStateLock.unlock();

		// add two processTPCOpRunnables to threadpool, one for each slaveServer that’s storing the key
		SlaveInfo firstReplica = findFirstReplica((K)KVMessage.decodeObject(TPCmess.getKey()));
		SlaveInfo successor = findSuccessor(firstReplica);
		

		Runnable firstReplicaRunnable = new processTPCOpRunnable<K,V>(TPCmess, firstReplica);
		Runnable successorRunnable = new processTPCOpRunnable<K,V>(TPCmess, successor);

		try {
			threadpool.addToQueue(firstReplicaRunnable);
			threadpool.addToQueue(successorRunnable);
		} catch (InterruptedException e) {
			// should not happen
			e.printStackTrace();
			TPCMaster.exit();
		}
				
		synchronized(currentTpcOpId){
			while (allDone < 2){
				try {
					currentTpcOpId.wait();
				} catch (InterruptedException e) {
					// this should not happen
					e.printStackTrace();
					TPCMaster.exit();
				}
			}
			finishedCount = 0;
			finishedCount2 = 0;
			allDone = 0;
		}

		boolean success = (TPCState == EState.COMMIT);
//		TPCStateLock.lock();
//		TPCState = EState.NOSTATE;
//		TPCStateLock.unlock();
		if (success) {
			if (TPCmess.getMsgType().equals("putreq")){
				masterCache.put((K) TPCMessage.decodeObject(TPCmess.getKey()), 
						(V)TPCmess.getValue());
			} else {
				masterCache.del((K) TPCMessage.decodeObject(TPCmess.getKey()));
			}
		} else {
			// find the replica that aborted and return its error message
			String temp = abortMessage;
			abortMessage = "";
			accessLock.writeLock().unlock();
			transactionLock.unlock();
			throw new KVException(new KVMessage(temp));
		}
		accessLock.writeLock().unlock();
		transactionLock.unlock();
		return success;
	}

	/**
	 * Perform GET operation in the following manner:
	 * - Try to GET from first/primary replica
	 * - If primary succeeded, return Value
	 * - If primary failed, try to GET from the other replica
	 * - If secondary succeeded, return Value
	 * - If secondary failed, return KVExceptions from both replicas
	 * 
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public V handleGet(KVMessage msg) throws KVException {
		// Sanity Check
		if (!"getreq".equals(msg.getMsgType())){
			System.err.println("handleGet called without a getRequest");
			throw new KVException(new KVMessage("handleGet called without a getRequest"));
		}

		// get the accessLock
		ReentrantReadWriteLock accessLock = accessLocks.get(msg.getKey());
		if (accessLock == null) {
			accessLocks.put(msg.getKey(), new ReentrantReadWriteLock());
			accessLock = accessLocks.get(msg.getKey());
		}
		accessLock.readLock().lock();

		// try the cache first
		V value = masterCache.get((K) KVMessage.decodeObject(msg.getKey()));

		if (value == null) {
			SlaveInfo firstReplica = findFirstReplica((K)KVMessage.decodeObject(msg.getKey()));
			SlaveInfo successor = findSuccessor(firstReplica);
			Runnable tempGetRunnable = new getRunnable<K,V>(msg, firstReplica, successor, value);
			try {
				threadpool.addToQueue(tempGetRunnable);
			} catch (InterruptedException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}

			// sleep until threads are finished
			synchronized(currentTpcOpId){
				while (getFinished == false){
					try {
						currentTpcOpId.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
						TPCMaster.exit();
					}
				}
				getFinished = false;
			}

			if (!abortMessage.equals("")) {
				// abortMessage is not the empty string, meaning get did not return a correct value
				String temp = abortMessage;
				abortMessage = "";
				accessLock.readLock().unlock();
				throw new KVException(new KVMessage(temp));
			} else { // get should have returned an good value
				value = (V) getReturnValue;

				// put into cache
				accessLock.writeLock().lock();
				masterCache.put((K) KVMessage.decodeObject(msg.getKey()), value);
				accessLock.writeLock().unlock();
				accessLock.readLock().unlock();
				return value;
			}
		}
		accessLock.readLock().unlock();
		return value;
	}

	/**
	 * Perform GET operation in the following manner:
	 * - Try to GET from first/primary replica
	 * - If primary succeeded, return Value
	 * - If primary failed, try to GET from the other replica
	 * - If secondary succeeded, return Value
	 * - If secondary failed, return KVExceptions from both replicas
	 * 
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	class getRunnable<K extends Serializable, V extends Serializable> implements Runnable {
		KVMessage message;
		SlaveInfo slaveServer;
		SlaveInfo successor;

		public getRunnable (KVMessage msg, SlaveInfo firstReplica, SlaveInfo successor, V value){
			this.message = msg;
			this.slaveServer = firstReplica;
			this.successor = successor;
		}

		@Override
		public void run(){
			TPCMessage slaveAnswer = null;

			// convert message to a TPCMessage
			TPCMessage tpcMessage = new TPCMessage(message, tpcOpId.toString());
			// make sure I don't accidentally use the KVMessage again;
			message = null;

			slaveAnswer = sendReceiveSlaveGET(slaveServer, tpcMessage);

			if (slaveAnswer.getValue() != null){ // first slave sent back a good put response
				getReturnValue = slaveAnswer.getValue();
				// do housekeeping before returning
				synchronized(currentTpcOpId){
					// do housekeeping before returning
					getFinished = true;
					currentTpcOpId.notifyAll();
				}
				return;
			} else if (slaveAnswer.getMessage() != null){ // slave sent back an error message
				message = slaveAnswer;

				// contact Successor
				slaveAnswer = sendReceiveSlaveGET(successor, tpcMessage);

				if (slaveAnswer.getValue() != null){ // successor slave sent back a good put response
					getReturnValue = slaveAnswer.getValue();
					// do housekeeping before returning
					synchronized(currentTpcOpId){
						// do housekeeping before returning
						getFinished = true;
						currentTpcOpId.notifyAll();
					}
					return;
				} else if (slaveAnswer.getMessage() != null){ // successor slave sent back an error message
					// set message to incorporate BOTH error messages
					abortMessage = "@"+slaveServer.getSlaveID()+"=>"+message.getMessage()+"\n@"+successor.getSlaveID()+"=>"+slaveAnswer.getMessage();
					synchronized(currentTpcOpId){
						// do housekeeping before returning
						getFinished = true;
						currentTpcOpId.notifyAll();
					}
					return;
				} else {
					// this should not happen
					System.err.println("getreq: successor slave didn't have a value or a message");
					TPCMaster.exit();
				}
			} else{
				// this should not happen
				System.err.println("getreq: first slave didn't have a value or a message");
				TPCMaster.exit();
			}

		}

		public TPCMessage sendReceiveSlaveGET(SlaveInfo slave, TPCMessage getRequest){
			TPCMessage slaveAnswer = null;

			// create new slave Socket
			Socket firstSlave = null;
			try {
				firstSlave = new Socket(slaveServer.hostName, slaveServer.port);
			} catch (UnknownHostException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			} catch (IOException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}

			// set timeout
			try {
				// as specified by Piazza post 876, GETS don't timeout
				firstSlave.setSoTimeout(0);
			} catch (SocketException e) {
				// could not set timeout, should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}

			// send the get request to slave
//			System.out.println("Sending to Slave " + slave.getSlaveID() + ":");
//			try {
//				System.out.println(getRequest.toXML()+"\n");
//			} catch (KVException e) {
//				e.printStackTrace();
//				TPCMaster.exit();
//			}
			TPCMessage.sendMessage(firstSlave, getRequest);

			// receive a response from slave
			// Correctness Constraint: slaveAnswer is either an error message or a get response
			try {
				slaveAnswer = TPCMessage.receiveMessage(firstSlave);
//				System.out.println("Received from Slave"+ slave.getSlaveID() + ":");
//				try {
//					System.out.println(slaveAnswer.toXML()+"\n");
//				} catch (KVException e) {
//					e.printStackTrace();
//					TPCMaster.exit();
//				}
			} catch (SocketTimeoutException e) {
				// as specified by Piazza post 876, GETS don't timeout, so this should never happen
				System.err.println("Get request should not have timed out");
				e.printStackTrace();
				TPCMaster.exit();
			}

			// Sanity Check
			if (!"resp".equals(slaveAnswer.getMsgType())){
				System.err.println("getRunnable got a bad response:");
//				try {
//					System.out.println(slaveAnswer.toXML());
//				} catch (KVException e) {
//					e.printStackTrace();
//				}
				TPCMaster.exit();
			}	
			return slaveAnswer;
		}
	}

	class processTPCOpRunnable<K extends Serializable, V extends Serializable>implements Runnable {
		TPCMessage message;
		TPCMessage response;
		SlaveInfo slaveServerInfo;

		public processTPCOpRunnable(TPCMessage msg, SlaveInfo slaveServerInfo){
			this.message = msg;
			this.slaveServerInfo = slaveServerInfo;
		}
		
		@Override
		public void run() {
			while (true) {
				switch (TPCState) {

				// send the appropriate message to client 
				case INIT:
					//synch
					synchronized(currentTpcOpId){
						if (finishedCount == 0){
							finishedCount++;
							while(finishedCount < 2){
								try {
									currentTpcOpId.wait();
								} catch (InterruptedException e) {
									e.printStackTrace();
									TPCMaster.exit();
								}
							}
						} else if (finishedCount == 1){
							finishedCount++;
							currentTpcOpId.notifyAll();
						} else {
							// this case should not happen
							System.err.println("synching got to else case");
							TPCMaster.exit();
						}
					}

					// Sanity Check
					if (!"putreq".equals(message.getMsgType()) && !"delreq".equals(message.getMsgType())){
						System.err.println("INIT did not get a putreq or delreq");
						TPCMaster.exit();
					}

					TPCMessage slaveResponse = null;
					try {
						slaveResponse = sendReceiveSlaveTPC(slaveServerInfo, message);
					} catch (SocketTimeoutException e1) {
						// if timeout, this is equivalent to receiving an abort message
						
						if (abortMessage.equals("")){
							abortMessage += "Timeout Error: SlaveServer "+slaveServerInfo.getSlaveID()+"has timed out during the first phase of 2PC";
						} else {
							abortMessage += "\nTimeout Error: SlaveServer "+slaveServerInfo.getSlaveID()+"has timed out during the first phase of 2PC";
						}
						
						TPCStateLock.lock();
						TPCState = EState.ABORT;
						TPCStateLock.unlock();
						break;
					}

					// Sanity Check
					if (!slaveResponse.getMsgType().equals("ready") && !slaveResponse.getMsgType().equals("abort")){
						System.err.println("getRunnable got a bad response");
						TPCMaster.exit();
					}

					if (slaveResponse.getMsgType().equals("abort")){
						if (abortMessage.equals("")){
							abortMessage += "@"+slaveServerInfo.getSlaveID()+"=>"+slaveResponse.getMessage();
						} else {
							abortMessage += "\n@"+slaveServerInfo.getSlaveID()+"=>"+slaveResponse.getMessage();
						}
						TPCStateLock.lock();
						TPCState = EState.ABORT;
						TPCStateLock.unlock();
					} else if ("ready".equals(slaveResponse.getMsgType())){
						if (TPCState != EState.ABORT){
							TPCStateLock.lock();
							TPCState = EState.COMMIT;
							TPCStateLock.unlock();
						}
					} else {
						// this should not happen
						System.err.println("Coordinator did not get a ready or abort response");
						TPCMaster.exit();
					}

					//synch
					synchronized(currentTpcOpId){
						if (finishedCount2 == 0){
							finishedCount2++;
							while(finishedCount2 < 2){
								try {
									currentTpcOpId.wait();
								} catch (InterruptedException e) {
									e.printStackTrace();
									TPCMaster.exit();
								}
							}
						} else if (finishedCount2 == 1){
							finishedCount2++;
							currentTpcOpId.notifyAll();
						} else {
							// this case should not happen
							System.err.println("synching got to else case");
							TPCMaster.exit();
						}
					}

					//Neither lock should be held now
					break;

				case ABORT:
					TPCMessage abortMessage = new TPCMessage("abort", message.getTpcOpId());
					TPCMessage abortAck;
					try {
						abortAck = sendReceiveSlaveTPC(slaveServerInfo, abortMessage);
						// check to see if response is ready or abort
						if (!"ack".equals(abortAck.getMsgType())){
							// should not happen
							System.err.println("ABORT did not get a correct ack");
							TPCMaster.exit();
						}
						// it is an ack, so we're done.
					} catch (SocketTimeoutException e) {
						// if timeout, then resend
						// this ONLY breaks out of the switch, not the while loop
						break;
					}
					
					synchronized(currentTpcOpId){
						allDone++;
						currentTpcOpId.notifyAll();
					}
					return;
				case COMMIT:
					TPCMessage commitMessage = new TPCMessage("commit", message.getTpcOpId());
					TPCMessage commitAck;
					try {
						commitAck = sendReceiveSlaveTPC(slaveServerInfo, commitMessage);
					} catch (SocketTimeoutException e) {
						// if timeout, then resend
						// this ONLY breaks out of the switch, not the while loop
						break;
					}
					if (!"ack".equals(commitAck.getMsgType())){
						System.err.println("COMMIT did not get a correct ack");
						System.exit(1);
					}
					// it is an ack, so we do housekeeping and finish
					synchronized(currentTpcOpId){
						allDone++;
						currentTpcOpId.notifyAll();
					}
					return;
				}
			}
		}
		public TPCMessage sendReceiveSlaveTPC(SlaveInfo slave, TPCMessage opRequest) throws SocketTimeoutException{
			TPCMessage slaveResponse = null;
			// send the request to the slaveServer

			// create new slave Socket
			Socket slaveSocket = null;
			try {
				slaveSocket = new Socket(slaveServerInfo.hostName, slaveServerInfo.port);
			} catch (UnknownHostException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			} catch (IOException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}

			// set timeout
			try {
				slaveSocket.setSoTimeout(TIMEOUT_MILLISECONDS);
			} catch (SocketException e) {
				// could not set timeout, should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}
//			try {
//				System.out.println("Sending to Slave " + slave.getSlaveID() + ":\n" + opRequest.toXML()+"\n");
//			} catch (KVException e) {
//				e.printStackTrace();
//				TPCMaster.exit();
//			}
			slaveResponse = TPCMessage.sendReceive(slaveSocket, opRequest);
//			try {
//				System.out.println("Received from Slave " + slave.getSlaveID() + ":\n" + slaveResponse.toXML() + "\n");
//			} catch (KVException e) {
//				e.printStackTrace();
//				TPCMaster.exit();
//			}
			return slaveResponse;
		}
	}

	// Either exits or doesn't exit
	public static void exit(){
		System.exit(1);
	}

	public void handleIgnore (KVMessage inputMessage, Socket client) throws KVException{

		// Sanity Check
		if (!"ignoreNext".equals(inputMessage.getMsgType())){
			System.err.println("handleIgnore called without a ignoreNext");
			TPCMaster.exit();
		}

		// get SlaveID
		Long slaveServerID = Long.decode(inputMessage.getMessage());
		SlaveInfo slave = consistentHash.get(slaveServerID);
		if (slave == null){
			throw new KVException(new KVMessage("IgnoreNext Error: Invalid SlaveServerID"));
		}

		// convert to a TPCMessage
		TPCMessage tpcMessage = new TPCMessage (inputMessage, "-1");

		// add ignoreRunnable to threadpool
		try {
			threadpool.addToQueue(new ignoreRunnable<K,V>(tpcMessage, slave, client));
		} catch (InterruptedException e) {
			// should not happen
			e.printStackTrace();
			TPCMaster.exit();
		}
		return;
	}

	class ignoreRunnable<K extends Serializable, V extends Serializable> implements Runnable {
		TPCMessage message;
		SlaveInfo slaveServerInfo;
		Socket client;

		public ignoreRunnable (TPCMessage msg, SlaveInfo slave, Socket client){
			this.message = msg;
			this.slaveServerInfo = slave;
			this.client = client;
		}

		@Override
		public void run(){

			// create connection to slaveServer
			Socket slaveServer = null;
			try {
				slaveServer = new Socket(slaveServerInfo.hostName, slaveServerInfo.port);
			} catch (UnknownHostException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			} catch (IOException e) {
				// should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}

			// send/receive from slaveServer
			TPCMessage slaveResponse = null;
			try {
				slaveResponse = TPCMessage.sendReceive(slaveServer, message);
			} catch (SocketTimeoutException e) {
				// should not timeout on ignoreNext call
				e.printStackTrace();
				TPCMaster.exit();
			}

			// send back to client
			KVMessage clientResponse = new KVMessage(slaveResponse.getMessage());
			KVMessage.sendMessage(client, clientResponse);

			// close ports
			try {
				client.close();
				slaveServer.close();
			} catch (IOException e) {
				// this should not happen
				e.printStackTrace();
				TPCMaster.exit();
			}

		}
	}

}
