package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

public class TPCLogTest {

	//test that adds to disk successfully
	@Test
	public void testAppendAndFlush() {
		//set up TPCLog
		KeyServer<String, String> server = new KeyServer<String, String>(9); 
		TPCLog<String, String> log = new TPCLog<String, String> ("logPath", server);
		ArrayList<KVMessage> list = log.getEntries();
		
		//test put request & commit
		TPCMessage put1 = new TPCMessage ("ready", "key1", "value1", "putreq", "1");
		log.appendAndFlush(put1);
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).equals(put1));
		TPCMessage commitPut1 = new TPCMessage ("commit", "1");
		log.appendAndFlush(commitPut1);
		assertTrue(list.size() == 2);
		assertTrue(list.get(1).equals(commitPut1));
		
		//test del request & commit
		TPCMessage del1 = new TPCMessage("ready", "key2", "delreq", "2");
		log.appendAndFlush(del1);
		assertTrue(list.size() == 3);
		assertTrue(list.get(2).equals(del1));
		TPCMessage commitDel1 = new TPCMessage ("commit","2");
		log.appendAndFlush(commitDel1);
		assertTrue(list.size() == 4);
		assertTrue(list.get(3).equals(commitDel1));
		
		//test put request & abort
		TPCMessage put2 = new TPCMessage ("ready", "key3", "value3", "putreq", "3");
		log.appendAndFlush(put2);
		assertTrue(list.size() == 5);
		assertTrue(list.get(4).equals(put2));
		TPCMessage abortPut2 = new TPCMessage ("abort", "3");
		log.appendAndFlush(abortPut2);
		assertTrue(list.size() == 6);
		assertTrue(list.get(5).equals(abortPut2));
		
		//test del request & abort
		TPCMessage del2 = new TPCMessage("ready", "key4", "delreq", "4");
		log.appendAndFlush(del2);
		assertTrue(list.size() == 7);
		assertTrue(list.get(6).equals(del2));
		TPCMessage abortDel2 = new TPCMessage ("abort","2");
		log.appendAndFlush(abortDel2);
		assertTrue(list.size() == 8);
		assertTrue(list.get(7).equals(abortDel2));
	}
	
	
	//test that correctly rebuilds keyserver by executing each entry in the log
	@Test
	public void testRebuildKeyServer() {
		//set up TPCLog
		KeyServer<String, String> server = new KeyServer<String, String>(10); 
		TPCLog<String, String> log = new TPCLog<String, String> ("logPath", server);
		
		//adds put request & abort
		TPCMessage put2 = new TPCMessage ("ready", "key1", "value1", "putreq", "1");
		log.appendAndFlush(put2);
		TPCMessage abortPut2 = new TPCMessage ("abort", "1");
		log.appendAndFlush(abortPut2);
		
		//adds put request & commit
		TPCMessage put1 = new TPCMessage ("ready", "key2", "value2", "putreq", "2");
		log.appendAndFlush(put1);
		TPCMessage commitPut1 = new TPCMessage ("commit", "2");
		log.appendAndFlush(commitPut1);
		
		//adds del request & abort
		TPCMessage del2 = new TPCMessage("ready", "key3", "delreq", "3");
		log.appendAndFlush(del2);
		TPCMessage abortDel2 = new TPCMessage ("abort","3");
		log.appendAndFlush(abortDel2);
		
		//adds del request & commit
		TPCMessage del1 = new TPCMessage("ready", "key2", "delreq", "4");
		log.appendAndFlush(del1);
		TPCMessage commitDel1 = new TPCMessage ("commit","4");
		log.appendAndFlush(commitDel1);
		
		//adds interrupted operation
		TPCMessage put3 = new TPCMessage("ready", "key4", "value4", "putreq", "5");
		log.appendAndFlush(put2);
		
		//test rebuilding
		String logPath = log.logPath;
		TPCLog<String, String> log2 = new TPCLog<String, String> (logPath, server);
		try {
			log2.rebuildKeyServer();
		} catch (KVException e) {
			System.out.println("Error rebuilding " + e);
		}
		for (int x = 0; x < log.getEntries().size(); x++) {
			TPCMessage y1 = (TPCMessage) log2.getEntries().get(x);
			TPCMessage y2 = (TPCMessage) log.getEntries().get(x);
			assertTrue(y1.equals(y2));
		}

		
	}

}
