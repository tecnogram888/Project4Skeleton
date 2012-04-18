package edu.berkeley.cs162;

import org.junit.Test;
import static org.junit.Assert.*;

public class ThreadPoolTest {
	
	// tests that threadPool instantiates successfully
	@Test
	public void testPoolSize() {
		int size = 10;
		ThreadPool testPool = new ThreadPool(size);
		assertEquals(testPool.threads.length, size);
		for (int i = 0; i < size; i++){
			assertFalse(testPool.threads[i] == null);
		}
	}
	
	// tests whether multiple adds occurs successfully
	@Test
	public void testAddQueueSize() throws InterruptedException {
		ThreadPool testPool = new ThreadPool(0);
		int size = 10;
		for (int i=0; i<size; i++) {
			testPool.addToQueue(new Thread());
		}
		assertEquals(testPool.tasks.size(), size);
	}
	
	// tests whether a Task was successfully added to the queue
	@Test
	public void testAddQueue() throws InterruptedException {
		ThreadPool testPool = new ThreadPool(0);
		Thread testThread = new Thread();
		testPool.addToQueue(testThread);
		assertTrue(testPool.tasks.contains(testThread));
	}

}
