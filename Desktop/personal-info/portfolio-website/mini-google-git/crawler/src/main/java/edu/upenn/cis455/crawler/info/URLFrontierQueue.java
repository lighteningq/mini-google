package edu.upenn.cis455.crawler.info;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import edu.upenn.cis455.crawler.workerServer.WorkerServer;
import edu.upenn.cis455.storage.DBWrapper;

public class URLFrontierQueue {
	static Logger log = Logger.getLogger(URLFrontierQueue.class);
	public Queue<String> curQueue = new LinkedBlockingQueue<String>();
	public Queue<String> nextQueue = new LinkedBlockingQueue<String>();
	
	public DBWrapper db;
	
	public volatile int urlExec;
	
	
	public URLFrontierQueue(DBWrapper db) {
		this.db = db;
		db.pollFromDiskQueue(1000, curQueue);
	}
	

	public void pushURLToQueue(String url) {
		
	//	log.debug("queue size is: " +curQueue.size());
			if(curQueue.isEmpty() || curQueue.size()<1000) {
				curQueue.offer(url);
				return;
			}
			else {
				nextQueue.offer(url);
				updateDiskQueue();
			}
			
			isEmpty();
			
		


	}
	
	public boolean isEmpty() {
		if(curQueue.isEmpty()) {
			db.pollFromDiskQueue(200, curQueue);
		}
		updateDiskQueue();
		return curQueue.isEmpty();
	}
	
	public String getFromQueue() {
		if(curQueue.isEmpty()) {
			db.pollFromDiskQueue(200, curQueue);
		}
		else if(curQueue.size()<30) {
			int cnt = 0;
			while(!nextQueue.isEmpty() && cnt < 500) {
				curQueue.add(nextQueue.poll());
			}
		}
			
		urlExec++;
		return curQueue.poll();
	}
	
	public void updateDiskQueue() {
		if(nextQueue.size()>1000) {
			db.pushNextQueueToDisk(nextQueue);
			nextQueue = new LinkedBlockingQueue<String>();
		}
			
	}
	
	public int getSize() {
		return curQueue.size();
	}
	
	public void shutdown() {
		db.pushNextQueueToDisk(curQueue);
		db.pushNextQueueToDisk(nextQueue);
	}
	
	
}
