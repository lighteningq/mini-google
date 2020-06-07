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

public class DocParserBolt implements IRichBolt {
	static Logger log = Logger.getLogger(DocParserBolt.class);
	Fields schema = new Fields("extractURLs","url");
	String executorId = UUID.randomUUID().toString();
	private String url;
	private String type;
	private Document content;
	private OutputCollector collector;
	private String id;
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
		type = null;
		content = null;
		
	}

	@Override
	public void execute(Tuple input) {
		url = input.getStringByField("url");
		content = (Document) input.getObjectByField("document");
		type = input.getStringByField("type");
		
		parseDocAndExtractLinks(url,content, type);
	}
	
	private void parseDocAndExtractLinks(String url, Document content, String type) {
		long startTime = System.currentTimeMillis();
		List<String> extractURLs = new ArrayList<>();
		RobotHandler robot = new RobotHandler();
		if (robot.isHTML(type) && content!=null) {
			//System.out.println("Extracting Links from URL: |" + url);
			for (Element link : content.select("a")) {
				String absHref = link.attr("abs:href");
				if (absHref != null &&!absHref.equals("") && !absHref.equals("\n"))
					extractURLs.add(absHref);
				
				//System.out.println("Now Adding to Queue: " + url + ".............");
			}
			
			collector.emit(new Values<Object>(extractURLs,url));
			log.info("[DocParserBolt] Emitting-: "+url+"---------->| Duration: "+(System.currentTimeMillis()-startTime)+"ms |");
			Thread.yield();
		}

		
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
