package edu.upenn.cis455.crawler.bolts;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.UUID;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.CrawlerUtil;
import edu.upenn.cis455.crawler.handler.RobotHandler;
import edu.upenn.cis455.crawler.info.URLFrontierQueue;
//import edu.upenn.cis455.crawler.XPathCrawler_Modified;
import edu.upenn.cis455.crawler.workerServer.WorkerServer;

import org.apache.log4j.Logger;

public class CrawlerQueueSpout implements IRichSpout{
	static Logger log = Logger.getLogger(CrawlerQueueSpout.class);

	
	String executorId = UUID.randomUUID().toString();
	SpoutOutputCollector collector;
	
	
	@Override
	public String getExecutorId() {
		log.debug("Starting spout");
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
		
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;
	
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	

	@Override
	public void nextTuple() {
		URLFrontierQueue q = WorkerServer.getCrawler().queue;
		if(WorkerServer.startTime==0) WorkerServer.startTime = System.currentTimeMillis();
		//System.out.println("[Spout]: starting spout....");
		//System.out.println("Worker Server Crawler is: "+WorkerServer.getCrawler().isQueueEmpty());
		long startTime = System.currentTimeMillis();
		if(!q.isEmpty()) {
			
			String cur = q.getFromQueue();
			
			log.debug("queue spout is starting on.--->"+cur);
			RobotHandler robot = new RobotHandler();
			try {
				if(!WorkerServer.getCrawler().hasCrawled(cur) &&!robot.checkDelay(cur)) {
					log.debug("crawl delay pause");
					q.pushURLToQueue(cur);
				}else 
				// politeness
				if (!WorkerServer.getCrawler().hasCrawled(cur) && robot.robotTxtCheck(cur)  && robot.headerCheck(cur)) {
						q.pushURLToQueue(cur);
						this.collector.emit(new Values<Object> (cur));
						long endTime = System.currentTimeMillis();
						log.info("[CrawlerSpout]Emitting: "+cur + "----------->  | Duration: "+ (endTime - startTime)+"ms |");
						Thread.yield();
				}else {
					log.debug("did not pass checking... not emitting"+cur);
				}
				WorkerServer.getCrawler().addSeenURL(cur);	
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			
			

		}
		
		
		
		//Thread.yield();
		
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);

		
	}

}
