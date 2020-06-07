package edu.upenn.cis455.crawler.bolts;
import java.util.*;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.handler.RobotHandler;
//import edu.upenn.cis455.crawler.XPathCrawler_Modified;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.crawler.workerServer.WorkerServer;

public class URLDistributeBolt implements IRichBolt {
	static Logger log = Logger.getLogger(URLDistributeBolt.class);
	Fields schema = new Fields("url");
	String executorId = UUID.randomUUID().toString();
	private String url;
	private OutputCollector collector;
	public int workerIndex = WorkerServer.index;
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
		url = null;
		
	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.currentTimeMillis();
		url = input.getStringByField("url");
		collector.emit(new Values<Object>(url));
		log.info("[URLDistributeBolt] Emitting-: "+url+"---------->| Duration: "+(System.currentTimeMillis()-startTime)+"ms |");
		Thread.yield();
		
	}
	
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);

	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
