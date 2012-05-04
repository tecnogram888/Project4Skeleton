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
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
		private class registrationRunnable implements Runnable{
			private Socket client;
			public registrationRunnable (Socket _client){
				this.client = _client;
			}
			public void run(){
				PrintWriter out = null;
				// TODO in is not used...
				InputStream in = null;
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
				}//TODO How to handle these errors?

				addToConsistentHash(newSlave);
				synchronized(consistentHash){
				if (consistentHash.size() >= listOfSlaves.length)//TODO Changed this to >= from ==, this is slightly safer, no?
					consistentHash.notify();
				}

				try {
					out = new PrintWriter(client.getOutputStream(), true);
				} catch (IOException e) {
					System.err.println("could not get slave's outputstream");
				}
				TPCMessage msg = new TPCMessage("Successfully registered"+newSlave.slaveID+"@"+newSlave.hostName+":"+newSlave.port);
				String xmlFile = null;
				try {
					xmlFile = msg.toXML();
				} catch (KVException e) {
					System.err.println("could not convert TPCMessage to XML");
				}
				out.println(xmlFile);
				try {
					client.shutdownOutput();
					// added by luke
					client.close();
					out.close();
				} catch (IOException e) {
					System.err.println("could not shutdown client ouptut");
				}
			}
		}
		@Override
		public void handle(Socket client) throws IOException {
			try {
				threadpool.addToQueue(new registrationRunnable(client));
			} catch (InterruptedException e) {
				// TODO How to handle this error?
				e.printStackTrace();
			}
//			PrintWriter out = null;
//			// TODO in is not used...
//			InputStream in = null;
//			SlaveInfo newSlave = null;
//			TPCMessage registration = null;
//
//			// read registration message from SlaveServer
//			try {
//				registration = new TPCMessage(client.getInputStream());
//				newSlave = new SlaveInfo(registration.getMessage());
//			} catch (KVException e) {
//				System.err.println("error reading registration message");
//			}
//
//			addToConsistentHash(newSlave);
//			synchronized(consistentHash){
//			if (consistentHash.size() >= listOfSlaves.length)//TODO Changed this to >= from ==, this is slightly safer, no?
//				consistentHash.notify();
//			}
//
//			try {
//				out = new PrintWriter(client.getOutputStream(), true);
//			} catch (IOException e) {
//				System.err.println("could not get slave's outputstream");
//			}
//			TPCMessage msg = new TPCMessage("Successfully registered"+newSlave.slaveID+"@"+newSlave.hostName+":"+newSlave.port);
//			String xmlFile = null;
//			try {
//				xmlFile = msg.toXML();
//			} catch (KVException e) {
//				System.err.println("could not convert TPCMessage to XML");
//			}
//			out.println(xmlFile);
//			try {
//				client.shutdownOutput();
//				// added by luke
//				client.close();
//				out.close();
//			} catch (IOException e) {
//				System.err.println("could not shutdown client ouptut");
//			}
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

		public String getHostName() {
			return hostName;
		}

		public int getPort() {
			return port;
		}
	}

	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;

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

	private Long currentTpcOpId = -1L;
	private ReentrantLock transactionLock = new ReentrantLock();
	private boolean canCommit = false;
	private ReentrantLock canCommitLock = new ReentrantLock();
	private enum EState {
		NOSTATE, INIT, ABORT, COMMIT
	}
	private EState TPCState = EState.NOSTATE;
	private ReentrantLock TPCStateLock = new ReentrantLock();
	private Condition otherThreadDone = TPCStateLock.newCondition();
	//added by Doug
	private ReentrantReadWriteLock consistantHashLock = new ReentrantReadWriteLock();

	public String[] listOfSlaves;


	/**
	 * Creates TPCMaster using SlaveInfo provided as arguments and SlaveServers 
	 * actually register to let TPCMaster know their spresence
	 * 
	 * @param listOfSlaves list of SlaveServers in "SlaveServerID@HostName:Port" format
	 * @throws Exception
	 */
	public TPCMaster(String[] slaves) throws Exception {
		// implement me
		listOfSlaves = slaves;
		// Create registration server
		regServer = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 9090);
		regServer.addHandler(new TPCRegistrationHandler()); //TODO: how many connections to instantiate with?
		clientServer = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 8080);
		//TODO: clientServer needs a NetworkHandler --> new TPCClientHandler
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
			class clientServerRunnable implements Runnable {
				
				@Override 
				public void run() {
					try {
						clientServer.connect();
						clientServer.run();
						} catch (IOException e) {
							e.printStackTrace();
							}
					}
				}
			Thread regServerThread = new Thread(new regServerRunnable());
			regServerThread.start();
			while (consistentHash.size() != listOfSlaves.length) {
				// TODO sleep clientServer
				synchronized(consistentHash){
				try {
					consistentHash.wait();
				} catch (InterruptedException e) {
					// TODO Doug how to handle this issue? In this, just die I think
					e.printStackTrace();
				}
				}
			}
			Thread clientServerThread = new Thread(new clientServerRunnable());
			clientServerThread.start();
			
			

		
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

		// implement me
		consistantHashLock.readLock().lock();
		if (consistentHash.isEmpty()) { return null; }
		SlaveInfo temp = consistentHash.get(
				((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(hashedKey) );
		if (temp == null) {
			return consistentHash.get(
					((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(consistentHash.firstKey()) );
		}
		consistantHashLock.readLock().unlock();
		return temp;
	}

	/**
	 * Find the successor of firstReplica to put the second replica
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		// implement me
		consistantHashLock.readLock().lock();
		if (consistentHash.isEmpty()) { return null; }
		SlaveInfo temp = consistentHash.get(
				((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(firstReplica.getSlaveID() + 1) );
		if (temp == null) {
			return consistentHash.get(
					((TreeMap<Long, SlaveInfo>) consistentHash).ceilingKey(consistentHash.firstKey()) );
		}
		consistantHashLock.readLock().unlock();
		return temp;
	}

	class processTPCOpRunnable<K extends Serializable, V extends Serializable>implements Runnable {
		TPCMessage message;
		Socket client;
		SlaveInfo slaveServerInfo;
		
		Boolean b1, b2;

		public processTPCOpRunnable(TPCMessage msg, SlaveInfo slaveServerInfo, Boolean _b1, Boolean _b2){
			this.message = msg;
			this.slaveServerInfo = slaveServerInfo;
			this.b1 = _b1;
			this.b2 = _b2;
		}
		@Override
		public void run() {
			//TODO Luke,Soloman,Doug need to change 'message' after the loop so we send different messages, reset the field at the end of each conditional block
			while (true) {
				switch (TPCState) {
				// send the appropriate message to client
				case INIT: 
					if (!"putreq".equals(message.getMsgType()) && !"delreq".equals(message.getMsgType())){
						//TODO Doug how to handle this error?
						System.err.println("INIT did not get a putreq or delreq");
						System.exit(1);
					}
					b1 = false;//Set boolean to false when starting a section, true when finished.
					try {
						// send the request to the slaveServer
						TPCMessage response = sendRecieveTPC(message, slaveServerInfo.hostName, slaveServerInfo.port);
						
						// check to see if response is ready or abort
						if ("ready".equals(response.getMsgType())){
							TPCStateLock.lock();
							if (TPCState == EState.COMMIT || TPCState == EState.ABORT){
								b1 = true;
								otherThreadDone.notifyAll();
							} else {
								TPCState = EState.COMMIT;
								try {
									b1 = true;
									while (!(b1 && b2)) otherThreadDone.await();
									TPCStateLock.unlock();//Unlock after waking up, reacquires lock after signal is called
									//otherThreadDone.wait();
								} catch (InterruptedException e) {
									//TODO Doug how to handle this error?
									System.err.println("INIT messed up when trying to wait");
									e.printStackTrace();
									System.exit(1);
								}
							}
							TPCStateLock.unlock();
						} else if ("abort".equals(response.getMsgType())){
							TPCStateLock.lock();
							if (TPCState == EState.INIT){
								// other thread is has not finished yet
								TPCState = EState.ABORT;
								TPCStateLock.unlock();
								try {
									b1 = true;
									while (!(b1 && b2)) otherThreadDone.await();
									TPCStateLock.unlock();//Unlock after waking up, reacquires lock after signal is called
								} catch (InterruptedException e) {
									//TODO Doug how to handle this error?
									System.err.println("INIT messed up when trying to wait");
									e.printStackTrace();
									System.exit(1);
								}
							} else{
								// if the other thread already finished and is waiting
								b1 = true;
								otherThreadDone.notifyAll();
							}		
							// check if other guy is sleeping, if so wake him up, if not go to sleep
							TPCStateLock.unlock();
						} else{
							//TODO Doug How to handle this error?
							System.err.println("Coordinator did not get a ready or abort response");
							System.exit(1);
						}
					} catch (KVException e) {
						//TODO Doug is this how we should handle errors?
						if("Unknown Error: Could net set Socket timeout".equals(e.getMsg().getMessage())){
							// Connection timed out
							TPCStateLock.lock();
							TPCState = EState.ABORT;
							// check if other guy is sleeping, if so wake him up, if not go to sleep
							TPCStateLock.unlock();
						} else {
							e.printStackTrace();
						}
					}
					break;
				case ABORT:
					b1 = false;//False at start of section
					try {
						TPCMessage response = sendRecieveTPC(message, slaveServerInfo.hostName, slaveServerInfo.port);
						// check to see if response is ready or abort
						if (!"ack".equals(response.getMsgType())){
							//TODO Doug how to handle this error?
							System.err.println("ABORT did not get a correct ack");
							System.exit(1);
						}
					} catch (KVException e) {
						if("Unknown Error: Could net set Socket timeout".equals(e.getMsg().getMessage())){
							// Connection timed out
							// resend
							continue;
						} else {
							e.printStackTrace();
						}
					}
					b1 = true;//Finished Abort section
					otherThreadDone.notifyAll();
					TPCState = EState.NOSTATE;
					return;
					
				case COMMIT:
					b1 = false;//Start of section
					try {
						TPCMessage response = sendRecieveTPC(message, slaveServerInfo.hostName, slaveServerInfo.port);
						// check to see if response is ready or abort
						if (!"ack".equals(response.getMsgType())){
							//TODO Doug how to handle this error?
							System.err.println("COMMIT did not get a correct ack");
							System.exit(1);
						}
					} catch (KVException e) {
						if("Unknown Error: Could net set Socket timeout".equals(e.getMsg().getMessage())){
							// Connection timed out
							// resend
							continue;
						} else {
							e.printStackTrace();
						}
					}
					b1 = true;
					otherThreadDone.notifyAll();
					TPCState = EState.NOSTATE;
					return;
					
				default: 
					return;
				}
			}

		}

	}

	/**
	 * Synchronized method to perform 2PC operations one after another
	 * 
	 * @param msg
	 * @param isPutReq
	 * @return True if the TPC operation has succeeded
	 * @throws KVException
	 */
	public synchronized boolean performTPCOperation(KVMessage msg) throws KVException {
		// implement me
		transactionLock.lock();

		// get the next TPC Op ID
		String TPCOpId = getNextTpcOpId();
		TPCMessage TPCmess = new TPCMessage(msg, TPCOpId);		
		ReentrantReadWriteLock temp = accessLocks.get(TPCmess.getKey());
		if (temp == null) {
			accessLocks.put(TPCmess.getKey(), new ReentrantReadWriteLock());
		}
		temp.writeLock().lock();
		TPCStateLock.lock();
		TPCState = EState.INIT;
		TPCStateLock.unlock();
		try {
			SlaveInfo firstReplica = findFirstReplica((K)KVMessage.decodeObject(TPCmess.getKey()));
			SlaveInfo successor = findSuccessor(firstReplica);
			Boolean b1 = new Boolean(false);
			Boolean b2 = new Boolean(false);
			threadpool.addToQueue(
					new processTPCOpRunnable<K,V>(TPCmess, firstReplica, b1, b2));
			threadpool.addToQueue(
					new processTPCOpRunnable<K,V>(TPCmess, successor, b2, b1));
			while (!(b1 && b2)){
				b1.wait();
			}
		} catch (InterruptedException e) {
			//sendMessage(client, new KVMessage("Unknown Error: InterruptedException from the threadpool"));
			temp.writeLock().unlock();
			transactionLock.unlock();
			return false;
		} catch (KVException e){
			//sendMessage(client, e.getMsg());
			temp.writeLock().unlock();
			transactionLock.unlock();
			return false;
		}

		// TODO SOLOMON Update cache
		// if (put) { cache.put() }
		// if (del) {cache.del() }
		temp.writeLock().unlock();
		transactionLock.unlock();
		return true;
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
		
		public getRunnable (KVMessage msg, SlaveInfo firstReplica, SlaveInfo successor){
			this.message = msg;
			this.slaveServer = firstReplica;
			this.successor = successor;
		}
		
		@Override
		public void run(){
			// send/receive request to first slave
			KVMessage slaveAnswer = null;
			try {
				// TODO LUKE CHANGE THE message from KVMEssage to TPCMessage
				slaveAnswer = sendRecieveKV(message, slaveServer.hostName, slaveServer.port);
			} catch (KVException e) {
				if("Unknown Error: Could net set Socket timeout".equals(e.getMsg().getMessage())){
					// Connection timed out
					// Move on to next slaveServer
				} else {
					e.printStackTrace();
					KVClientHandler.sendMessage(slaveServer.getKvSocket(), e.getMsg());
					return;
				}
			}
			if (!"resp".equals(slaveAnswer.getMessage())){
				// TODO this should not happen, so crash the server if it does
				System.exit(1);
				// temp.readLock().unlock();
				// throw new KVException(new KVMessage("handleGet called without a getRequest"));
			} else {
				slaveAnswer.getValue();
				// TODO return the value to parent thread somehow
				// wake up the parent
			}
			// if it gets here, response was wrong so try second replica

			try {
				slaveAnswer = sendRecieveKV(message, slaveServer.hostName, slaveServer.port);
			} catch (KVException e) {
				if("Unknown Error: Could net set Socket timeout".equals(e.getMsg().getMessage())){
					// Connection timed out
				} else {
					e.printStackTrace();
					KVClientHandler.sendMessage(slaveServer.getKvSocket(), e.getMsg());
					return;
				}
			}
			if (!"resp".equals(slaveAnswer.getMessage())){
				// TODO this should not happen, so crash the server if it does
				System.exit(1);
				// temp.readLock().unlock();
				// throw new KVException(new KVMessage("handleGet called without a getRequest"));
			} else {
				slaveAnswer.getValue();
				// TODO return the value to parent thread somehow
				// wake up the parent
			}
			// if it gets here both didn't work
			// TODO
		}
	}
	
	public V handleGet(KVMessage msg) throws KVException {
		if (!"getreq".equals(msg.getMsgType())){
			// TODO this should not happen, so crash the server if it does
			System.exit(1);
			// throw new KVException(new KVMessage("handleGet called without a getRequest"));
		}

		// implement me
		ReentrantReadWriteLock accessLock = accessLocks.get(msg.getKey());
		if (accessLock == null) {
			accessLocks.put(msg.getKey(), new ReentrantReadWriteLock());
		}
		accessLock.readLock().lock();
		// TODO Try cache
		V value = masterCache.get((K) KVMessage.decodeObject(msg.getKey()));
		if (value == null) {
			try {
				SlaveInfo firstReplica = findFirstReplica((K)KVMessage.decodeObject(msg.getKey()));
				SlaveInfo successor = findSuccessor(firstReplica);
				threadpool.addToQueue(
						new getRunnable<K,V>(msg, firstReplica, successor));
				// TODO DOUG sleeping on threads
				// value somehow gets set to the proper value
				if (value != null) {
					masterCache.put((K) KVMessage.decodeObject(msg.getKey()), value);
				} else {
					// ERROR? Or OK to return null when neither slaveServer has it
				}
			} catch (InterruptedException e) {
				//sendMessage(client, new KVMessage("Unknown Error: InterruptedException from the threadpool"));
				accessLock.writeLock().unlock();
				return null;
			} catch (KVException e){
				//sendMessage(client, e.getMsg());
				accessLock.writeLock().unlock();
				return null;
			}
		}

		accessLock.writeLock().unlock();
		return value;

	}
	
	private KVMessage sendRecieveKV(KVMessage InputMessage, String server, int port) throws KVException {
		String xmlFile = InputMessage.toXML();
		KVMessage returnMessage;
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
			returnMessage = new KVMessage(in);
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
		return returnMessage;
	}	

	public TPCMessage sendRecieveTPC(TPCMessage InputMessage, String server, int port) throws KVException {
		String xmlFile = InputMessage.toXML();
		TPCMessage returnMessage;
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
			returnMessage = new TPCMessage(in);
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
		return returnMessage;
	}	
}
