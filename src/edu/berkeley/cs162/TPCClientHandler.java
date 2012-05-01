/**
 * Handle client connections over a socket interface
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections. 
 * It uses a threadpool to ensure that none of it's methods are blocking.
 *
 * @param <K> Java Generic type for the Key
 * @param <V> Java Generic type for the Value
 */
public class TPCClientHandler<K extends Serializable, V extends Serializable> implements NetworkHandler {
	// private KeyServer<K, V> keyserver = null;
	private ThreadPool threadpool = null;	
	private TPCMaster<K, V> tpcMaster = null;

	public TPCClientHandler(TPCMaster master) {
		initialize(1, master);
	}

	public TPCClientHandler(int connections, TPCMaster master) {
		initialize(connections, master);
	}

	private void initialize(int connections, TPCMaster master) {
		//this.keyserver = keyserver;
		this.threadpool = new ThreadPool(connections);	
		this.tpcMaster = master;
	}

	//Utility method, sends the KVMessage to the client Socket and closes output on the socket
	public static void sendMessage(Socket client, KVMessage message){
		PrintWriter out = null;
		try {
			out = new PrintWriter(client.getOutputStream(), true);
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
			client.shutdownOutput();
		} catch (IOException e) {
			e.printStackTrace();
		}
		out.close();
	}

	/* (non-Javadoc)
	 * @see edu.berkeley.cs162.NetworkHandler#handle(java.net.Socket)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void handle(Socket client) throws IOException {

		InputStream in = client.getInputStream();
		KVMessage mess = null;

		try {
			mess = new KVMessage(in);
		} catch (KVException e) {
			TPCClientHandler.sendMessage(client, e.getMsg());
			return;
		}

		try {
			threadpool.addToQueue(new processMessageRunnable<K,V>(mess, client, tpcMaster));
		} catch (InterruptedException e) {
			// TODO ERROR MESSAGE: sendMessage(client, new KVMessage("Unknown Error: InterruptedException from the threadpool"));
			return;
		}
	}
}


class processMessageRunnable<K extends Serializable, V extends Serializable> implements Runnable {
	KVMessage mess;
	Socket client;
	TPCMaster tpcMaster;

	public processMessageRunnable(KVMessage mess, Socket client, TPCMaster master){
		this.mess = mess;
		this.client = client;
		this.tpcMaster = master;
	}
	@Override
	public void run() {
		if ("getreq".equals(mess.getMsgType())) {	
			V value = null;
			try {
				value = (V) tpcMaster.handleGet(mess);
			} catch (KVException e) {
				TPCClientHandler.sendMessage(client, e.getMsg());
				return;
			}
			KVMessage message = null;
			try {
				message = new KVMessage("resp", mess.getKey(), KVMessage.encodeObject(value));
			} catch (KVException e){
				TPCClientHandler.sendMessage(client, e.getMsg());
				return;
			}
			TPCClientHandler.sendMessage(client, message);
			try {
				client.close();
			} catch (IOException e) {
				// These ones don't send errors, this is a server error
				e.printStackTrace();
			}
		} else if ("putreq".equals(mess.getMsgType())) {
			
			boolean status = false;
			try {
				//need seperate operations for put and delete
				status = performPUTOperation(mess, true);
			} catch (KVException e) {
				KVClientHandler.sendMessage(client, e.getMsg());
				return;
			}
			KVMessage message = new KVMessage(status, "Success");
			KVClientHandler.sendMessage(client, message);
			try {
				client.close();
			} catch (IOException e) {
				// These ones don't send errors, this is a server error
				e.printStackTrace();
			}
		} else if ("delreq".equals(mess.getMsgType())) {
			
			try {
				performDelOperation(mess, false);
			} catch (KVException e) {
				KVClientHandler.sendMessage(client, e.getMsg());
				try {
					client.close();
				} catch (IOException e2) {
					e2.printStackTrace();
				}
				return;
			}
			KVMessage message = new KVMessage("Success");
			KVClientHandler.sendMessage(client, message);
			try {
				client.close();
			} catch (IOException e) {
				// These ones don't  errors, this is a server error
				e.printStackTrace();
			}
		} else {
			//TODO throw Exception
		}
		/*
		KVMessage message = null;
		try {
			message = new KVMessage("resp", KVMessage.encodeObject(key), KVMessage.encodeObject(value));
		} catch (KVException e){
			TPCClientHandler.sendMessage(client, e.getMsg());
			return;
		}
		TPCClientHandler.sendMessage(client, message);
		try {
			client.close();
		} catch (IOException e) {
			// These ones don't send errors, this is a server error
			e.printStackTrace();
		}*/
	}
}
