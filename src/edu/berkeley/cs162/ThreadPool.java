/**
 * Java Thread Pool
 * 
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

import java.util.LinkedList;


public class ThreadPool {
	/**
	 * Set of threads in the threadpool
	 */
	protected Thread threads[];
	protected LinkedList<Runnable> tasks;
	protected int size;

	/**
	 * Initialize the number of threads required in the threadpool. 
	 * 
	 * @param size  How many threads in the thread pool.
	 */
	public ThreadPool(int size)
	{
		// implement me
		this.size = size;
		tasks = new LinkedList<Runnable>();
		threads = new Thread[size];
		for (int i=0;i<size;i++) {
			threads[i] = new WorkerThread(this);
			threads[i].start();
		}
	}
	
	// this constructor allows for a delayed start
	public ThreadPool(int size, boolean start) {
		this.size = size;
		tasks = new LinkedList<Runnable>();
	}
	
	// call to start threadpool on a delayed start - only to be used with 'delayed start' constructor
	public void startPool() {
		threads = new Thread[size];
		for (int i=0;i<size;i++) {
			threads[i] = new WorkerThread(this);
			threads[i].start();
		}
	}

	/**
	 * Add a job to the queue of tasks that has to be executed. As soon as a thread is available, 
	 * it will retrieve tasks from this queue and start processing.
	 * @param r job that has to be executed asynchronously 
	 * @throws InterruptedException 
	 */
	public synchronized void addToQueue(Runnable r) throws InterruptedException
	{
		// implement me
		tasks.add(r);
		this.notify();
	}
}

/**
 * The worker threads that make up the thread pool.
 */
class WorkerThread extends Thread {
	/**
	 * @param o the thread pool 
	 */
	ThreadPool pool;
	Runnable r;
	
	WorkerThread(ThreadPool o)
	{
		// implement me
		pool = o;
		
	}

	/**
	 * Scan and execute tasks.
	 */
	public void run()
	{
		// implement me
		while (true){
			if (!pool.tasks.isEmpty()) {
				r = pool.tasks.poll();
				r.run();
			} else {
				try {
					synchronized(pool){
					pool.wait();
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
