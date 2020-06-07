package edu.upenn.cis455.crawler;

import java.net.InetAddress;
import org.apache.log4j.Logger;

import edu.upenn.cis455.crawler.info.URLFrontierQueue;
import edu.upenn.cis455.storage.DBWrapper;

public class DistributedCrawler {
	static Logger log = Logger.getLogger(DistributedCrawler.class);
	static DistributedCrawler theInstance;
	boolean running;
	
	

	// parameters
	public String startURL;
	public String directory;
	public long maxDocSize; // make an automic integer
	public volatile int maxNumFile = -1;

	public boolean hasBound = false;
	public String monitorHost;
	public DBWrapper db;
	public InetAddress host;
	
	public URLFrontierQueue queue;

	
	
	DistributedCrawler() {running=false;};
	
	public static DistributedCrawler getInstance() {
		if(theInstance==null)
			theInstance = new DistributedCrawler();
		return theInstance;
	}
	
	public DistributedCrawler(String[] args) {
		init(args);
		db.initQueue();
		queue = new URLFrontierQueue(db);
		queue.pushURLToQueue(startURL);
		theInstance = this;
		
	}
	
	public DBWrapper getDB() {
		return db;
	}
	


	
	public void init(String args[]) {
		if (args.length < 3 || args.length > 5) {
			System.out.println("Initialize Crawler Error, Please Input Command Line Again");
			System.exit(1);
		}

		for (int i = 0; i < args.length; i++) {
			if (i == 0)
				startURL = args[i];
			if (i == 1) {
				directory = args[1];
				db = new DBWrapper(directory);
			}
			if (i == 2)
				maxDocSize = Integer.parseInt(args[2]) * 1000000;
			if (i == 3) {
				maxNumFile = Integer.parseInt(args[3]);
				hasBound = true;
			}
			if (i == 4)
				monitorHost = args[4];
		}
		if (monitorHost == null)
			monitorHost = "cis455.cis.upenn.edu"; 
	}
		

	
	public boolean hasCrawled(String url) {
		return db.containsVisitedURL(url);
	}
	
	public void addSeenURL(String url) {
		db.putVisitedToDB(url);
	}
	
	public void shutdown() {
		db.shutdownDB();
	}
	


	
	

	
	

}
