package edu.berkeley.cs162;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.naming.directory.BasicAttribute;

import org.junit.Test;
import static org.junit.Assert.*;

public class TPCMessageTest {

	@Test // tests constructor for converting KV error messages and successful delete messages to a TPC message
	public void convertKVMessage() {
		TPCMessage msg = new TPCMessage(new KVMessage("string"), "0");
		assertTrue(msg.getMsgType() == "resp");
		assertTrue(msg.getMessage() == "string");
		assertTrue(msg.getTpcOpId() == "0");
	}

	@Test // tests constructor for converting a KV successful put message to a TPC message
	public void convertKVMessage2() {
		TPCMessage msg = new TPCMessage(new KVMessage(false, "string"), "0");
		assertTrue(msg.getMsgType() == "resp");
		assertTrue(msg.getMessage() == "string");
		assertTrue(msg.getTpcOpId() == "0");
	}

	@Test // tests constructor for converting a KV message to a TPC message
	public void convertKVMessage3() {
		TPCMessage msg = new TPCMessage(new KVMessage("string1", "string2"), "0");
		assertTrue(msg.getMsgType() == "string1");
		assertTrue(msg.getKey() == "string2");
		assertTrue(msg.getValue() == null);
		assertTrue(msg.getTpcOpId() == "0");
	}

	@Test // tests constructor for converting a KV message to a tpc message
	public void convertKVMessage4() {
		TPCMessage msg = new TPCMessage(new KVMessage("string1", "string2", "string3"), "0");
		assertTrue(msg.getMsgType() == "string1");
		assertTrue(msg.getKey() == "string2");
		assertTrue(msg.getValue() == "string3");
		assertTrue(msg.getTpcOpId() == "0");
	}

	@Test // tests constructor for tpc log ready
	public void TPCLogReady() {
		TPCMessage msg = new TPCMessage("type", "key", "value", "message", "0");
		assertTrue(msg.getMsgType() == "type");
		assertTrue(msg.getKey() == "key");
		assertTrue(msg.getValue() == "value");
		assertTrue(msg.getMessage() == "message");
		assertTrue(msg.getTpcOpId() == "0");
	}

	@Test // tests constructor for tpc put and tpc log del messages
	public void TPCPut() { 
		TPCMessage msg = new TPCMessage("putreq", "key", "value", "0");
		assertTrue(msg.getMsgType() == "putreq");
		assertTrue(msg.getKey() == "key");
		assertTrue(msg.getValue() == "value");
		assertTrue(msg.getTpcOpId() == "0");
		TPCMessage msg2 = new TPCMessage("ready", "key", "msg", "0");
		assertTrue(msg2.getMsgType() == "ready");
		assertTrue(msg2.getKey() == "key");
		assertTrue(msg2.getMessage() == "msg");
		assertTrue(msg2.getTpcOpId() == "0");
	}

	@Test // tests constructor for 2PC Ready Messages, 2PC Decisions, 2PC Acknowledgement, Register, Registration ACK, Error Message, Server response, 2PCLog abort, 2PCLog commit, and KeyRequest
	public void TPEtc() { // 
		TPCMessage msg = new TPCMessage("delreq", "key", "0", false);
		assertTrue(msg.getMsgType() == "delreq");
		assertTrue(msg.getKey() == "key");
		assertTrue(msg.getTpcOpId() == "0");
		TPCMessage msg2 = new TPCMessage("abort", "msg", "0", false);
		assertTrue(msg2.getMsgType() == "abort");
		assertTrue(msg2.getMessage() == "msg");
		assertTrue(msg2.getTpcOpId() == "0");
	}

	@Test
	public void Test2pcPUTReq() {
		BasicAttribute keyTest = new BasicAttribute("key");
		BasicAttribute valueTest = new BasicAttribute("value");
		TPCMessage test = null;
		try {
			test = new TPCMessage("putreq", KVMessage.encodeObject(keyTest), KVMessage.encodeObject(valueTest), "2PC Operation ID");
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		assertTrue(test.getMsgType() == "putreq");
		try {
			assertEquals(valueTest, TPCMessage.decodeObject(test.getValue()));
			assertEquals(keyTest, TPCMessage.decodeObject(test.getKey()));
		} catch (KVException e) {
			// Auto-fail if an exception is thrown
			assertTrue(false);
			e.printStackTrace();
			System.exit(1);
		}
		assertTrue(test.getMessage() == null);
		assertTrue(test.getTpcOpId() == "2PC Operation ID");
		String xml = null;
		try {
			xml = test.toXML();
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		String x = null;
		try {
			x = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" 
					+ "<KVMessage type=\"putreq\">\n" +
					"<Key>" + KVMessage.encodeObject(keyTest) + "</Key>\n" + 
					"<Value>" + KVMessage.encodeObject(valueTest) + "</Value>\n" + 
					"<TPCOpId>2PC Operation ID</TPCOpId>\n" +
					"</KVMessage>\n";
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		assertEquals(x, xml);
	}

	@Test
	public void Test2pcDELReq() {
		BasicAttribute keyTest = new BasicAttribute("key");
		TPCMessage test = null;
		try {
			test = new TPCMessage("delreq", KVMessage.encodeObject(keyTest), "2PC Operation ID", true);
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		assertTrue(test.getMsgType() == "delreq");
		try {
			assertEquals(keyTest, TPCMessage.decodeObject(test.getKey()));
		} catch (KVException e) {
			// Auto-fail if an exception is thrown
			assertTrue(false);
			e.printStackTrace();
			System.exit(1);
		}
		assertTrue(test.getMessage() == null);
		assertTrue(test.getTpcOpId() == "2PC Operation ID");
		String xml = null;
		try {
			xml = test.toXML();
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		String x = null;
		try {
			x = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" 
					+ "<KVMessage type=\"delreq\">\n" +
					"<Key>" + KVMessage.encodeObject(keyTest) + "</Key>\n" + 
					"<TPCOpId>2PC Operation ID</TPCOpId>\n" +
					"</KVMessage>\n";
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		assertEquals(x, xml);
	}

	@Test
	public void Test2pcREADYresp() {
		TPCMessage test = new TPCMessage("ready","2PC Operation ID");
		assertTrue(test.getMsgType() == "ready");
		assertTrue(test.getKey() == null);
		assertTrue(test.getValue() == null);
		assertTrue(test.getMessage() == null);
		assertTrue(test.getTpcOpId() == "2PC Operation ID");
		String xml = null;
		try {
			xml = test.toXML();
		} catch (KVException e) {
			e.printStackTrace();
			fail();
		}
		String x = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" 
				+ "<KVMessage type=\"ready\">\n" + 
				"<TPCOpId>2PC Operation ID</TPCOpId>\n" +
				"</KVMessage>\n";
		assertEquals(x, xml);
	}

	/*	@Test // tests sending a message
	public void sendTPCMessageTest() {
		ServerSocket server = null;
		Socket client = null;
		BufferedReader in = null;
		PrintWriter out = null;
		try{
		    server = new ServerSocket(0); 
		  } catch (IOException e) {
		    System.err.println("Could not listen on port 8080");
		  }

		TPCMessage testMsg = new TPCMessage("test string");
		Socket testSocket = null;
		try {
			testSocket = new Socket("localhost", server.getLocalPort());
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try{
		    client = server.accept();
		  } catch (IOException e) {
		    System.err.println("Accept failed: 8080");
		  }
		try{
			in = new BufferedReader(new InputStreamReader(
			                           client.getInputStream()));
			out = new PrintWriter(client.getOutputStream(), 
			                         true);
			  } catch (IOException e) {
			    System.err.println("Read failed");
			  }


		try {
			TPCMessage.sendTPCMessage(testSocket, testMsg);
		} catch (KVException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while(true){
		      try{
		        String line = in.readLine();
		        //Send data back to client
		        assertTrue(line == testMsg.toXML());
		        out.println(line);
		      } catch (IOException e) {
		        System.out.println("Read failed");
		      } catch (KVException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}*/


}
