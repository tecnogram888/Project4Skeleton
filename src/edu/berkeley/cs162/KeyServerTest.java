package edu.berkeley.cs162;

import static org.junit.Assert.*;

import org.junit.Test;

public class KeyServerTest {
	

	@Test
	public void testPut() {
		KeyServer<String, String> server = new KeyServer<String, String>(10);
		try {
			assertFalse(server.put("Test1", "Test2"));
			
			assertFalse(server.put("Test3", "Test4"));
			
			assertTrue(server.put("Test1", "Test5"));
			
			assertEquals(server.get("Test1"), "Test5");
			
			assertEquals(server.get("Test3"), "Test4");
			
		} catch (KVException e) {
			fail("Basic put threw a KVException");
		}
	}
	
	@Test
	public void testGet() {
		KeyServer<String, String> server = new KeyServer<String, String>(3);
		try {
			server.put("Test1", "Test2");
			server.put("Test3", "Test4");
			server.put("Test5", "Test6");
			server.put("Test7", "Test8");//At this point, Test1-Test2 should not be in the cache
		
		} catch (KVException e) {
			fail("Basic put threw a KVException");
		}
		
		try {//Test that normal get works
			assertEquals(server.get("Test7"), "Test8");
		} catch (KVException e) {
			fail("Basic get threw a KVException");
		}
		try {//Test it throws KVException on non-existent key
			server.get("Test9");
		} catch (KVException e) {
			System.out.println(e.getMsg().getMessage());
			assertEquals(e.getMsg().getMessage(), "Does not exist");
		}
		
		long start = System.nanoTime();//Time with value in cache
		long end;
		try {
			server.get("Test7");
		} catch (KVException e) {
			fail("Basic get threw a KVException");
		} finally {
			end = System.nanoTime();
		}
		long time = end - start;
		System.out.println("Get call with K-V in cache (ms): " + time);
		assert(time < 1000*1000*1000);
		
		start = System.nanoTime();//Time with value not in cache
		try {
			server.get("Test1");
		} catch (KVException e) {
			fail("Basic get threw a KVException");
		} finally {
			end = System.nanoTime();
		}
		time = end - start;
		System.out.println("Get call with K-V not in cache (ms): " + time);
		assert(time > 1000*1000*1000);
		
	}
	
	@Test
	public void testDel() {
		KeyServer<String, String> server = new KeyServer<String, String>(3);
		try {
			server.put("Test1", "Test2");
		
		} catch (KVException e) {
			fail("Basic put threw a KVException");
		}
		try {//Should find key
			assertEquals(server.get("Test1"), "Test2");
		} catch (KVException e) {
			fail("Basic get threw a KVException");
		}
		try {//Should delete key
			server.del("Test1");
		} catch (KVException e) {
			fail("Basic del threw a KVException");
		}
		try {//Should not find key
			server.get("Test1");
		} catch (KVException e) {
			assertEquals(e.getMsg().getMessage(), "Does not exist");
		}
		
		try {
			server.del("Test11");
		} catch (KVException e) {
			assertEquals(e.getMsg().getMessage(), "Does not exist");
		}
	}

}
