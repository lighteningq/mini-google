package edu.upenn.cis455.crawler.bolts;

import java.util.*;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.handler.URLFilterHandler;
//import edu.upenn.cis455.crawler.XPathCrawler_Modified;
import edu.upenn.cis455.crawler.workerServer.WorkerServer;

public class URLFilterBolt implements IRichBolt {
	
	static Logger log = Logger.getLogger(URLFilterBolt.class);
	
	Fields schema = new Fields();
	
	String executorId = UUID.randomUUID().toString();
	
	
//	private OutputCollector collector;
	
    public URLFilterBolt() {
    	log.debug("Starting URLFilterBolt");
    }
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.currentTimeMillis();
		if(input.isEndOfStream())WorkerServer.getCrawler().getDB().test();
		String curLink = input.getStringByField("url");
		List<String> links = (List<String>) input.getObjectByField("extractURLs");
		List<String> outLinks = new ArrayList<>();
		
		for(String link : links){
			if(System.currentTimeMillis()-startTime > 5000) return;
			// filter
			if(!WorkerServer.getCrawler().hasCrawled(link)){
				String url=URLFilterHandler.filter(link);
				//System.out.println("link is:  "+ link + " || visited? url in bolt: "+ WorkerServer.getCrawler().getDB().containsVisitedURL(link));
				if(url!=null) {
					WorkerServer.getCrawler().getDB().putURLHash(url);
					outLinks.add(url);
					if(!WorkerServer.getCrawler().hasCrawled(url))
						WorkerServer.getCrawler().queue.pushURLToQueue(url);
				}

			}
		}
		
		
		
		
		
		// put outlinks to db <id, outIds>
		//WorkerServer.getCrawler().getDB().putExtractedURL(curLink, outLinks);
		
		log.info("[URLFilterBolt] Emitting: "+curLink+"-------> | Duration : "+ (System.currentTimeMillis() - startTime)+"ms | ");
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		//this.collector = collector;

	}

	@Override
	public void setRouter(StreamRouter router) {
		//this.collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
