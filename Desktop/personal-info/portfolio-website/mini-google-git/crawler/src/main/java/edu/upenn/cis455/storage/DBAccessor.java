package edu.upenn.cis455.storage;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;


public class DBAccessor {

	
	public DBAccessor(EntityStore store) {
		// Getting the primary index for data from crawler
		crawlerData = store.getPrimaryIndex(String.class, DocEntry.class);
		
		// Getting the primary index for data from urlHash
		urlHashData = store.getPrimaryIndex(String.class, URLHashEntry.class);
		
		// Getting the primary index for data from queue
		queueData = store.getPrimaryIndex(String.class, FrontierQueue.class);
		
		
		robotData = store.getPrimaryIndex(String.class, RobotTxtEntry.class);
		
		
		visitedURLData = store.getPrimaryIndex(String.class, VisitedURL.class);
		
		
		outURLData = store.getPrimaryIndex(String.class, ExtractedURLEntry.class);
		
		metaData = store.getPrimaryIndex(String.class, MetaDataEntry.class);
	}
	
	PrimaryIndex<String,DocEntry> crawlerData;
	PrimaryIndex<String,URLHashEntry> urlHashData;
	PrimaryIndex<String,FrontierQueue> queueData;
	PrimaryIndex<String,RobotTxtEntry> robotData;
	PrimaryIndex<String,VisitedURL> visitedURLData;
	PrimaryIndex<String,ExtractedURLEntry> outURLData;
	PrimaryIndex<String,MetaDataEntry> metaData;
	
}

