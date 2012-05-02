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
import java.io.InputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler<K extends Serializable, V extends Serializable> implements NetworkHandler {
	private KeyServer<K, V> keyserver = null;
	private ThreadPool threadpool = null;
	private TPCLog<K, V> tpcLog = null;
	
	private boolean ignoreNext = false;
	

	private enum EState {
		NOSTATE, WAIT, ABORT, COMMIT
	}
	private EState TPCState = EState.NOSTATE;
	private ReentrantLock TPCStateLock = new ReentrantLock();

	public TPCMasterHandler(KeyServer<K, V> keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KeyServer<K, V> keyserver, int connections) {
		this.keyserver = keyserver;
		threadpool = new ThreadPool(connections);	
	}

	class getRunnable<K extends Serializable, V extends Serializable>implements Runnable {
		TPCMessage message;
		Socket client;

		public getRunnable(TPCMessage msg){
			this.message = msg;		
		}
		@Override
		public void run() {
			keyserver.get()
			// send response to master
		}
	}
	
	class processTPCOpRunnable<K extends Serializable, V extends Serializable>implements Runnable {
		TPCMessage message;
		Socket client;

		public processTPCOpRunnable(TPCMessage msg){
			this.message = msg;
		}
		@Override
		public void run() {
			// try to do op, if can't, send abort, if can, send ready; we assume it always works
			// write message to TPCLog
			// send ready 
			// TPCState == WAIT
			while (true) {
				switch (TPCState) {
				case WAIT: 
					// listen for abort or commit
					// write message to TPC Log
					// go into proper state
				case ABORT:
					// abort and send ack and 
				case COMMIT:
					if () {
						keyserver.put()
					} else if () {
						keyserver.del(key)
					}
					// commit and send ack and 
					// commit by updating cache and KVStore according to original message
				default:
					// fail/error
				}
			}
			
		}
	}
	
	@Override
	public void handle(Socket client) throws IOException {
		// implement me

		InputStream in = client.getInputStream();
		TPCMessage message = null;
		
		try {
			message = new TPCMessage(in);
		} catch (KVException e) {
			
			TPCClientHandler.sendMessage(client, e.getMsg());
			return;
		}

		if (message.getMsgType().equals("getreq")) {
			try {
				threadpool.addToQueue(new getRunnable<K,V>(message));
			} catch (InterruptedException e) {
				//sendMessage(client, new KVMessage("Unknown Error: InterruptedException from the threadpool"));
			}
		} else if (message.getMsgType().equals("putreq") || message.getMsgType().equals("delreq")) {
			try {
				threadpool.addToQueue(new processTPCOpRunnable<K,V>(message));
			} catch (InterruptedException e) {
				//sendMessage(client, new KVMessage("Unknown Error: InterruptedException from the threadpool"));
			}
		} else {
			// error
		}
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog<K, V> tpcLog) {
		this.tpcLog  = tpcLog;
	}

}
