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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.bind.DatatypeConverter;

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

	class getRunnable<K extends Serializable, V extends Serializable> implements Runnable {
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
			// call get function and send answer to master
			V value = null;
			try {
				value = keyserver.get(key);
			} catch (KVException e) {
				TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(), "-1"));
				return;
			}
			KVMessage KVresponse = null;
			TPCMessage TPCresponse = null;
			try {
				// create get response
				KVresponse = new KVMessage("resp", KVMessage.encodeObject(key), KVMessage.encodeObject(value));
				TPCresponse = new TPCMessage(KVresponse, TpcOpID);
			} catch (KVException e){
				TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(), "-1"));
				return;
			}
			TPCMasterHandler.sendTPCMessage(master, TPCresponse);
			try {
				master.close();
			} catch (IOException e) {
				// These ones don't send errors, this is a server error
				e.printStackTrace();
			}
			// send response to master
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
			// try to do op, if can't, send abort, if can, send ready; we assume it always works
			// write message to TPCLog
			// send ready message to master
			// TPCState == WAIT

			// check to see if putreq will send back an ABORT
			TPCMessage reply = null;
			try{
				if (!checkKey(message.getKey())){
					TPCMessage abortMessage = new TPCMessage("abort", "Over sized key", message.getTpcOpId(), false);
					TPCMasterHandler.sendTPCMessage(master, abortMessage);
					reply = TPCMasterHandler.sendRecieveTPCMessage(master, abortMessage);
				} else if (!checkValue(message.getValue())){
					TPCMessage abortMessage = new TPCMessage("abort", "Over sized value", message.getTpcOpId(), false);
					reply = TPCMasterHandler.sendRecieveTPCMessage(master, abortMessage);
				} else{
					TPCMessage readyMessage = new TPCMessage("ready", TpcOpID);
					reply = TPCMasterHandler.sendRecieveTPCMessage(master, readyMessage);
				}
			} catch (KVException e){
				e.printStackTrace();
				sendTPCMessage(master, new TPCMessage("abort", e.getMsg().getMessage(), TpcOpID, false));
			}
			if ("commit".equals(reply.getMsgType())){
				//TODO SOLOMON DO LOCKS
				TPCState = EState.COMMIT;
			} else if ("abort".equals(reply.getMsgType())){
				//TODO SOLOMON DO LOCKS
				TPCState = EState.ABORT;
			} else{
				//  TODO throw exception
			}
			while (true) {
				switch (TPCState) {
				/*	DON'T NEED WAIT ANYMORE SINCE WE'RE DIONG SENDRECEIVE
				  	case WAIT: 
					// listen for abort or commit
					// write message to TPC Log
					// go into proper state*/
				case ABORT:
					// abort
					TPCMessage ackMessage = new TPCMessage("ack", TpcOpID);
					TPCMasterHandler.sendTPCMessage(master, ackMessage);
				case COMMIT:
					//TODO Solomon Do cache and KVStore stuff
					try {
						keyserver.put(key, value);
					} catch (KVException e) {
						TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(),"-1"));
						return;
					}
					TPCMessage ackmessage = new TPCMessage("ack", TpcOpID);
					TPCMasterHandler.sendTPCMessage(master, ackmessage);					
				default:
					// TODO fail/error
				}
				TPCState = EState.NOSTATE;
				try {
					master.close();
				} catch (IOException e) {
					// These ones don't send errors, this is a server error
					e.printStackTrace();
				}
			}
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
			// try to do op, if can't, send abort, if can, send ready; we assume it always works
			// write message to TPCLog
			// send ready message to master
			// TPCState == WAIT

			// check to see if putreq will send back an ABORT
			TPCMessage reply = null;
			try{
				if (!checkValue(message.getValue())){
					TPCMessage abortMessage = new TPCMessage("abort", "Over sized value", message.getTpcOpId(), false);
					reply = TPCMasterHandler.sendRecieveTPCMessage(master, abortMessage);
				} else{
					TPCMessage readyMessage = new TPCMessage("ready", TpcOpID);
					reply = TPCMasterHandler.sendRecieveTPCMessage(master, readyMessage);
				}
			} catch (KVException e){
				e.printStackTrace();
				sendTPCMessage(master, new TPCMessage("abort", e.getMsg().getMessage(), TpcOpID, false));
			}
			if ("commit".equals(reply.getMsgType())){
				//TODO SOLOMON DO LOCKS
				TPCState = EState.COMMIT;
			} else if ("abort".equals(reply.getMsgType())){
				//TODO SOLOMON DO LOCKS
				TPCState = EState.ABORT;
			} else{
				//  TODO throw exception
			}
			while (true) {
				switch (TPCState) {
				/*	DON'T NEED WAIT ANYMORE SINCE WE'RE DIONG SENDRECEIVE
				  	case WAIT: 
					// listen for abort or commit
					// write message to TPC Log
					// go into proper state*/
				case ABORT:
					// abort
					TPCMessage ackMessage = new TPCMessage("ack", TpcOpID);
					TPCMasterHandler.sendTPCMessage(master, ackMessage);
				case COMMIT:
					//TODO Solomon Do cache and KVStore stuff
					try {
						keyserver.del(key);
					} catch (KVException e) {
						TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(),"-1"));
						return;
					}
					TPCMessage ackmessage = new TPCMessage("ack", TpcOpID);
					TPCMasterHandler.sendTPCMessage(master, ackmessage);					
				default:
					// TODO fail/error
				}
				TPCState = EState.NOSTATE;
				try {
					master.close();
				} catch (IOException e) {
					// These ones don't send errors, this is a server error
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void handle(Socket master) throws IOException {
		// implement me

		//TODO Solomon: DO ALL LOCKS

		//TODO turn GET into TPCMessage


		InputStream in = master.getInputStream();
		TPCMessage message = null;

		try {
			message = new TPCMessage(in);
		} catch (KVException e) {
			TPCClientHandler.sendMessage(master, e.getMsg());
			return;
		}

		if (message.getMsgType().equals("getreq")) {
			//TODO Solomon lock accessLock
			if(!checkValue(message.getValue())){
				// throw exception or send error message or DO SOMETHING
			}
			try {
				threadpool.addToQueue(new getRunnable<K,V>((K)TPCMessage.decodeObject(message.getKey()), keyserver, master, message.getTpcOpId()));
			} catch (InterruptedException e) {
				TPCMasterHandler.sendTPCMessage(master, new TPCMessage("resp", "Unknown Error: InterruptedException from the threadpool"));
			} catch (KVException e){
				TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(), "-1"));
				return;
			}
		} else if (message.getMsgType().equals("putreq")){
			//TODO Solomon lock transaction lock
			//TODO Solomon lock accessLock
			//TODO Solomon lock estateLock
			//TODO Solomon change estatelock
			try {
				threadpool.addToQueue(new putRunnable<K,V>(
						(K)KVMessage.decodeObject(message.getKey()), 
						(V)KVMessage.decodeObject(message.getValue()), 
						keyserver, master,message.getTpcOpId(), message));
			} catch (InterruptedException e) {
				//TODO check if this is right
				TPCMasterHandler.sendTPCMessage(master,new TPCMessage(new KVMessage("Unknown Error: InterruptedException from the threadpool"), "-1"));
			} catch (KVException e){
				TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(), "-1"));
				return;
			}
		} else if (message.getMsgType().equals("delreq")){
			//TODO Solomon lock transaction lock
			//TODO Solomon lock accessLock
			//TODO Solomon lock estateLock
			//TODO Solomon change estatelock
			try {
				threadpool.addToQueue(new delRunnable<K,V>(
						(K)KVMessage.decodeObject(message.getKey()), 
						keyserver, master,message.getTpcOpId(), message));
			} catch (InterruptedException e) {
				//TODO check if this is right
				TPCMasterHandler.sendTPCMessage(master,new TPCMessage(new KVMessage("Unknown Error: InterruptedException from the threadpool"), "-1"));
			} catch (KVException e){
				TPCMasterHandler.sendTPCMessage(master, new TPCMessage(e.getMsg(), "-1"));
				return;
			}
		} else{
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

	//Utility method, sends the KVMessage to the client Socket and closes output on the socket
	public static void sendTPCMessage(Socket master, TPCMessage message){
		PrintWriter out = null;
		try {
			out = new PrintWriter(master.getOutputStream(), true);
		} catch (IOException e) {
			// Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out.println(message.toXML());
		} catch (KVException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		}
		try {
			master.shutdownOutput();
		} catch (IOException e) {
			e.printStackTrace();
		}
		out.close();
	}

	private static TPCMessage sendRecieveTPCMessage(Socket connection, TPCMessage message) throws KVException {
		String xmlFile = message.toXML();
		PrintWriter out = null;
		InputStream in = null;
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
			message = new TPCMessage(in);
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
}
