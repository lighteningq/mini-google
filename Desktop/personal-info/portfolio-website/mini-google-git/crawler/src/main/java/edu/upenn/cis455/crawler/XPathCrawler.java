package edu.upenn.cis455.crawler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.text.ParseException;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis455.crawler.bolts.CrawlerBolt;
import edu.upenn.cis455.crawler.bolts.CrawlerQueueSpout;
import edu.upenn.cis455.crawler.bolts.DocParserBolt;
import edu.upenn.cis455.crawler.bolts.URLFilterBolt;
import edu.upenn.cis455.storage.DBWrapper;

public class XPathCrawler {
	static Logger log = Logger.getLogger(XPathCrawler.class);
	static XPathCrawler theInstance;
	static boolean running;
	
	
	// bolt info 
	private static final String QUEUE_SPOUT = "QUEUE_SPOUT";
	private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
	private static final String DOC_PARSER_BOLT = "DOC_PARSER_BOLT";
	private static final String URL_FILTER_BOLT = "URL_FILTER_BOLT";
	

	// parameters
	public static String startURL;
	public static String directory;
	public static long maxDocSize; // make an automic integer
	public static volatile int maxNumFile = -1;

	public static boolean hasBound = false;
	public static String monitorHost;
	public static DBWrapper db;
	public static InetAddress host;

																																																																																																																																																																																																																																																																																																																																																																																																																																																																	
	
	
	
	XPathCrawler() {running=false;};
	
	public static XPathCrawler getInstance() {
		if(theInstance==null)
			theInstance = new XPathCrawler();
		return theInstance;
	}
	
	public XPathCrawler(String[] args) {
		init(args);
		db.initQueue();
		db.putToQueue(startURL);
		
	}
	
	public DBWrapper getDB() {
		return theInstance.db;
	}
	

	public void addToFrontierQueue(String url) {
		db.putToQueue(url);
	}
	
	public boolean isQueueEmpty() {
		return db.isQueueEmpty();
	}
	
	public String pollFromQueue() {
		return db.pollFromQueue();
	}
	


	public static void main(String args[]) throws MalformedURLException, IOException, ParseException, InterruptedException {
	
		init(args);
		
		db.initQueue();
		db.putToQueue(startURL);
		
		
		
		// define stormlite topology
		Config config = new Config();
		CrawlerQueueSpout spout = new CrawlerQueueSpout();
		CrawlerBolt crawlerbolt = new CrawlerBolt();
		DocParserBolt parser = new DocParserBolt();
		URLFilterBolt urlfilter = new URLFilterBolt();
		
		/***
		 * function: 		                                                        generate_hash_id
		 *            check_delay           get_content                extract           filterURL
	 	 *          crawler_sprout =======> crawler_bolt ============> parser ==========> urlfilter
           emit:          <url>            <url, content>          <url, outLink>         <out_link>
		  database:                       <url_id, content>                          <url _id, outLink>      
		 * 
		 * 
		 * ****/

		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(QUEUE_SPOUT, spout, 1);
		builder.setBolt(CRAWLER_BOLT, crawlerbolt, 10).shuffleGrouping(QUEUE_SPOUT);;
		builder.setBolt(DOC_PARSER_BOLT, parser, 5).shuffleGrouping(CRAWLER_BOLT);
		builder.setBolt(URL_FILTER_BOLT, urlfilter, 2).shuffleGrouping(DOC_PARSER_BOLT);
		
		LocalCluster cluster = new LocalCluster();
		Topology topo = builder.createTopology();
		
        ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);
			
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			cluster.submitTopology("crawler", config, builder.createTopology());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread.sleep(30000);
		
		
		cluster.killTopology("crawler");
		cluster.shutdown();
		System.exit(0);

		db.shutdownDB();
		
	}


	
	public static void init(String args[]) {
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
	


	
	

	
	

}

