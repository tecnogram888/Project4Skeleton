package edu.berkeley.cs162;

import static org.junit.Assert.*;

import org.junit.Test;

public class KVCacheTest {
	/** 
	 * Correctness Constraints:
Cache only stores the specified number of key value pairs.
Cache removes the correct key value pair according to LRU.
Cache works--returns a value that it has in the cache faster than if it had to fetch it.

Testing Plan:
call put() more times than the size was initialized to be, then peek at the linkedList.size().
place N items in a N sized cache, then call all N of them again and make sure the response time is faster than a call to KVStore.  Then the N+1th item should take longer
place N items in a N sized cache in order, then place an N+1th item, and then ask for the first item inserted: it should take the same amount of time as a call to KVStore. 

	 */
	
	String key = "hello";
	String value = "world";
	String key1 = "good";
	String value1 = "bye";
	String key2 = "roses";
	String value2 = "red";

	@Test
	public void test_initialize_Small() {
		System.out.println("Create KVCache of size 2");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
	}

	@Test
	public void test_put1_Small() {
		System.out.println("Test if put works");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		assertFalse( cache.put(key, value) );
	}
	
	@Test
	public void test_put2_Small() {
		System.out.println("Test put returns true if there exists a previous <k, v> pair");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		assertFalse( cache.put(key, value) );
		assertFalse( cache.put(key1, value1) );
		assertTrue( cache.put(key1, value1) );
	}
	
	@Test 
	public void test_del1() {
		System.out.println("Test del with empty cache");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		cache.del(key);
	}
	
	@Test 
	public void test_del2() {
		System.out.println("Test del with empty cache");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		cache.put(key, value);
		cache.del(key);
	}

	@Test
	public void test_get1_Small() {
		System.out.println("Test one put and one get");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		assertFalse( cache.put(key, value) );
		assertTrue(cache.get(key) == value);
	}

	@Test
	public void test_get2_Small() {
		System.out.println("Test cacheSize puts and gets");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		cache.put(key, value);
		assertTrue(cache.get(key) == value);
		cache.put(key1, value1);
		assertTrue(cache.get(key1) == value1);
	}
	
	@Test
	public void test_putAndGet1_Small() {
		System.out.println("Test put(k, v) then put(k, v1) has get(k) == v1");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		cache.put(key, value);
		assertTrue(cache.get(key) == value);
		cache.put(key, value1);
		assertTrue(cache.get(key) == value1);
	}
	
	@Test
	public void test_putAndGet2_Small() {
		System.out.println("Test if the right value is evicted when cache is overloaded");
		KVCache<String, String> cache = new KVCache<String, String>(2);
		assertTrue(cache != null);
		cache.put(key, value);
		assertTrue(cache.get(key) == value);
		cache.put(key1, value1);
		assertTrue(cache.get(key1) == value1);
		cache.put(key2, value2);
		System.out.println("wat" + cache.get(key));
		cache.del(key);
		assertTrue(cache.get(key) == null);
		assertTrue(cache.get(key1) == value1);
		assertTrue(cache.get(key2) == value2);
	}
}
