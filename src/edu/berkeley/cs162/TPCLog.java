/**
 * Log for Two-Phase Commit
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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class TPCLog<K extends Serializable, V extends Serializable> {

	//originally: private String logPath = null;
	String logPath = null;
	private KeyServer<K, V> keyServer = null;

	// Log entries
	private ArrayList<KVMessage> entries = null;

	// Keeps track of the interrupted 2PC operation (There can be at most one, 
	// i.e., when the last 2PC operation before crashing was in READY state)
	private KVMessage interruptedTpcOperation = null;

	public TPCLog(String logPath, KeyServer<K, V> keyServer) {
		this.logPath = logPath;
		entries = new ArrayList<KVMessage>();
		this.keyServer = keyServer;
	}

	public ArrayList<KVMessage> getEntries() {
		return entries;
	}

	public boolean empty() {
		return (entries.size() == 0);
	}

	public void appendAndFlush(KVMessage entry) {
		// implement me
		//		System.out.println("Entries currently has " + entries.size());
		//		System.out.println(entry);
		entries.add(entry);
		this.flushToDisk();
	}

	/**
	 * Load log from persistent storage
	 */
	@SuppressWarnings("unchecked")
	public void loadFromDisk() {
		ObjectInputStream inputStream = null;

		try {
			inputStream = new ObjectInputStream(new FileInputStream(logPath));			
			entries = (ArrayList<KVMessage>) inputStream.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// If log never existed, there are no entries
			if (entries == null) {
				entries = new ArrayList<KVMessage>();
			}

			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Writes log to persistent storage
	 */
	public void flushToDisk() {
		ObjectOutputStream outputStream = null;

		try {
			outputStream = new ObjectOutputStream(new FileOutputStream(logPath));
			outputStream.writeObject(entries);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (outputStream != null) {
					outputStream.flush();
					outputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Load log and rebuild by iterating over log entries 
	 * @throws KVException
	 */
	public void rebuildKeyServer() throws KVException {
		// implement me
		this.loadFromDisk();
		entries = this.getEntries();
		int x = 0; //just a counter
		while (x< entries.size()) { //execute each operation in entries
			TPCMessage msg = (TPCMessage) entries.get(x);

			//check if there is no following abort / commit tpc message (interrupted) 
			if (entries.size()-1 == x) {
				interruptedTpcOperation = entries.get(x); 
				x++;
			} else {
				interruptedTpcOperation = null;

				//execute message in log
				TPCMessage nextMsg = (TPCMessage) entries.get(x+1);
				if (msg.getMessage().equals("putreq")) {
					if (msg.getTpcOpId().equals(nextMsg.getTpcOpId())) {
						if (nextMsg.getMsgType().equals("commit")) {
							try {
								keyServer.put((K)msg.getKey(), (V)msg.getValue());
								x+=2;
							} catch (KVException e) {
								throw new KVException (new KVMessage ("Error with KV Message put" + e));
							}
						} else if (nextMsg.getMsgType().equals("abort")){
							x+=2;//do nothing
						}
					} else {//interrupted (msg's tpcopid is not same as nextmsg's tpcopid)
						x++;
						throw new KVException (new KVMessage ("Error-- Don't know if commit / abort"));
					}
				}
				else if (msg.getMessage().equals("delreq")) {
					if (msg.getTpcOpId().equals(nextMsg.getTpcOpId())) {
						if (nextMsg.getMsgType().equals("commit")) {
							try {
								keyServer.del((K)msg.getKey());
								x+=2;
							} catch (KVException e) {
								throw new KVException (new KVMessage ("Error with KV Message del" + e));
							}
						} else if (nextMsg.getMsgType().equals("abort")) {
							x+=2;//do nothing
						}
					} else {
						x++;
						throw new KVException (new KVMessage ("Error-- Don't know if commit / abort"));
					}
				}
			}
		}

		/* Notes-- Cases:
		 * ready for put --> abort
		 * ready for put --> commit
		 * ready for put --> interrupted (call get interruptedtpcop)
		 * ready for del --> abort
		 * ready for del --> commit
		 * ready for del --> interrupted (call get intrruptedtpcop)
		 */

		/* Old, Simplified stuff:
		for (int x = 0; x < entries.size(); x++) {
			try {
				TPCMessage msg = entries.get(x);
				if ((msg.getMsgType().equals("get"))) {
					keyServer.get((K) msg.getKey());
				}
				if ((msg.getMsgType().equals("put"))) { //if message is abort, don't do anything
					keyServer.put((K)msg.getKey(), (V)msg.getValue());
				}
				if ((msg.getMsgType().equals("delete"))) {
					keyServer.del((K)msg.getKey());
				}

				if ((msg.getMsgType().equals("abort"))) {
					// don't do anything
				}
			} catch (Exception e) {
				throw new KVException (new KVMessage ("Error with KVMessage " + e));
			}
		}*/
	}

	/**
	 * 
	 * @return Interrupted 2PC operation, if any 
	 */
	public KVMessage getInterruptedTpcOperation() { 
		KVMessage logEntry = interruptedTpcOperation; 
		interruptedTpcOperation = null; 
		return logEntry; 
	}

	/**
	 * 
	 * @return True if TPCLog contains an interrupted 2PC operation
	 */
	public boolean hasInterruptedTpcOperation() {
		return interruptedTpcOperation != null;
	}
}
