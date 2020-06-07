package edu.upenn.cis455.crawler.bolts;



import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.CrawlerUtil;
import edu.upenn.cis455.crawler.UploaderS3;
import edu.upenn.cis455.crawler.handler.MonitorClient;
import edu.upenn.cis455.crawler.handler.RobotHandler;
import edu.upenn.cis455.crawler.handler.URLProcessHandler;
//import edu.upenn.cis455.crawler.XPathCrawler_Modified;
import edu.upenn.cis455.crawler.workerServer.WorkerServer;

public class CrawlerBolt implements IRichBolt {
	static Logger log = Logger.getLogger(CrawlerBolt.class);
	
	Fields schema = new Fields("url", "extractURLs");
	String executorId = UUID.randomUUID().toString();
	private OutputCollector collector;
	private String type;
	public CrawlerBolt() {
		log.debug("Starting CrawlerBolt");
	}
	
	@Override
	public String getExecutorId() {
		// TODO Auto-generated method stub
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(schema);
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.currentTimeMillis();
		String url = input.getStringByField("url");
		if(!WorkerServer.getCrawler().hasCrawled(url) ) {
		Document d;
		try {
			d = processURL(url);
			log.debug("before process url"+url);
			
			
			if(d!=null) {
				//put chunkbody
//				String title = d.title();
//				byte[] body = d.body().text().substring(0,100).getBytes();
//				WorkerServer.getCrawler().getDB().putMetaData(url, title, body);
				WorkerServer.ws.addFileProcessed();
				
				
				List<String> extractURLs = new ArrayList<>();
				List<String> extractIds = new ArrayList<>();
				RobotHandler robot = new RobotHandler();
				if (robot.isHTML(type) && d!=null) {
					//System.out.println("Extracting Links from URL: |" + url);
					for (Element link : d.select("a")) {
						String absHref = link.attr("abs:href");
						if (absHref != null &&!absHref.equals("") && !absHref.equals("\n")) {
							extractURLs.add(absHref);
							extractIds.add(CrawlerUtil.generateURLId(absHref));
						}
							
						
						//System.out.println("Now Adding to Queue: " + url + ".............");
					}
				
				}
				
				
				// put id -> doc to db
				//WorkerServer.getCrawler().getDB().putDoc(url, d.toString().getBytes());
				
				//upload to s3
				UploaderS3.uploadfileS3(url, d.toString().getBytes(), extractIds);
				
				// emit if doc exist
				collector.emit(new Values<Object>(url,extractURLs));
				log.info("[CrawlerBolt] crawling-----> "+url+"------> | Duration:"+(System.currentTimeMillis()-startTime)+"ms |");
				Thread.yield();
			}
		} catch (MalformedURLException e) {
			log.error("url process error",e);
			e.printStackTrace();
		} catch (UnknownHostException e) {
			log.error("url process error",e);
			e.printStackTrace();
		} catch (IOException e) {
			log.error("url process error",e);
			e.printStackTrace();
		} catch (Exception e) {
			log.error("url process error",e);
			e.printStackTrace();
		}
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
	
	public Document processURL(String url) throws MalformedURLException, UnknownHostException, IOException, Exception {
		if(WorkerServer.getCrawler().maxNumFile - WorkerServer.ws.getFileProcessed()==0) {
			log.info("Reached Limit...... Now Stop Crawler");
			return null;
		}
		
		log.debug("Now poll an URL for process: "+ url);

		MonitorClient monitor = new MonitorClient(WorkerServer.getCrawler().monitorHost);	
		URLProcessHandler processor = new URLProcessHandler();
		Document d = processor.processURL(url);
		type = processor.getDocType();
		
		monitor.sendUDPMonitor(url);// monitoring

		
		return d;
		

	}
	
//	
//	/** Process HTTP request for each URL
//	 * @param url
//	 * @return true if a new doc is put into DB, false otherwise
//	 * @throws UnknownHostException
//	 * @throws IOException
//	 */
//	private boolean processHTTP(String url) throws UnknownHostException, IOException {
//
//		String type = "";
//		String content = "";
//		String location = "";
//		// 1. send get request
//		//System.out.println("HTTP: Sending Get Request to " + url + "...............");
//		URLInfo cur = new URLInfo(url);
////		Socket conn = new Socket(cur.getHostName(), cur.getPortNo());
////		StringBuilder sb = new StringBuilder();
////		sb.append("GET " + cur.getFilePath() + " HTTP/1.1\r\n");
////		sb.append("Host: " + cur.getHostName()+"\r\n");
////		sb.append("User-Agent: cis455crawler\r\n");
////		sb.append("Connection: Close\r\n\r\n");
////		//System.out.println(sb.toString());
////		PrintWriter pr = new PrintWriter(conn.getOutputStream());
////		pr.write(sb.toString());
////		pr.flush();
//		HttpURLConnection conn;
//		URLInfo curInfo = new URLInfo(url);
//		log.debug("HostName is: "+ curInfo.getHostName()+ " | Port is "+ curInfo.getPortNo());
//
//		int len=0;
//		String type="";
//		try {
//			conn  = (HttpURLConnection) (new URL(url)).openConnection();
//			conn.setRequestProperty("User-Agent", "cis455crawler");
//			conn.setRequestProperty("Connection", "close");
//			conn.setRequestMethod("GET");
//			conn.setConnectTimeout(XPathCrawler.HTTP_TIMEOUT);
//			conn.setReadTimeout(XPathCrawler.READFILE_TIMEOUT);
//			conn.connect();
//			len = conn.getContentLength();
//			type = conn.getContentType();
//			status = conn.getResponseCode();
//			content = conn.getContent();
//			
//			conn.disconnect();
//		}catch (IOException e1) {
//			// TODO Auto-generated catch block
//			log.info(ExceptionUtils.getStackTrace(e1));
//			e1.printStackTrace();
//		}
//
//		log.debug("processing sending http:....."+url);
//		// 2. read response and generate string
//		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//
//		String line = br.readLine();
//		int status = Integer.parseInt(line.split(" ")[1]);
//		// 2.1 process redirect , download if necessary
//		if (status == 301 || status == 302) {
//			while (!(line = br.readLine()).equals("")) {
//				if (line.startsWith("Location:")) {
//					log.debug("Redirecting Line is: "+ line);
//					location = line.substring(10);
//					if (!location.equals(" ")) {
//						
//					//	location = cur.getProtocol() + cur.getHostName() + location;
//						XPathCrawler.addToFrontierQueue(location);
//						log.debug(location + " : Adding Redirecting Location:.......");
//						conn.close();
//						return true;
//					}
//				}
//			}
//			// 2.2 process 400 and 500 response, skipping
//		} else if (status / 100 == 4 || status / 100 == 5) {
//			conn.close();
//			return false;
//		} // 2.3 else read in document and put into db
//		else if (status == 200 || status == 304) { // 2 cases
//			while (!(line = br.readLine()).equals("")) {
//				if (line.startsWith("Content-Type:")) {
//					type = line.split(":")[1].split(";")[0].trim();
//				}
//			}
//			while ((line = br.readLine()) != null) {
//				content += line;
//			}
//			conn.close();
//
//			if (status == 200) {
//				//System.out.println("number left:" +maxNumFile);
//				collector.emit(new Values<Object>(url, content, status,type));
//				return true;
//
//			} else {
//				log.debug(url + ": Not Modified ........");
//			}
//
//		} else {
//			log.debug("Invalid Response... Skipping");
//			conn.close();
//			return false;
//		}
//		conn.close();
//		return true;
//	}
//	
//	
//	/** Process HTTPS request and put documents into DB
//	 *
//	 * @param url
//	 * @return true if a new document is generated, false otherwise
//	 * @throws MalformedURLException
//	 * @throws IOException
//	 */
//	private boolean processHTTPS(String url) throws MalformedURLException, IOException {
//		String content = "";
//		String type;
//		int status;
//		URLInfo cur = new URLInfo(url);
//		log.debug("processing sending https:....."+url);
//		HttpsURLConnection conn = (HttpsURLConnection) (new URL(url)).openConnection();
//		conn.setRequestMethod("GET");
//		conn.setRequestProperty("User-Agent", "cis455crawler");
//		type = conn.getContentType();
//		status = conn.getResponseCode();
//		if (status == 301 || status == 302) {
//
//			String redirectLink = conn.getHeaderField("Location");
//			if (redirectLink != null) {
//				log.debug(url + " : Redirecting.....");
//				XPathCrawler.addToFrontierQueue(cur.getProtocol() + cur.getHostName() + redirectLink);
//				conn.disconnect();
//				return true;
//			}
//			conn.disconnect();
//			return false;
//
//		} else if (status / 100 == 4 || status / 100 == 5) {
//			log.debug(url + " : Invalid Response 4xx or 5xx .....");
//			conn.disconnect();
//			return false;
//
//		} else if (status == 200 || status == 304) {
//
//			InputStream in = (InputStream) conn.getContent();
//			BufferedReader br = new BufferedReader(new InputStreamReader(in));
//			String line;
//			while ((line = br.readLine()) != null) {
//				content += line + "\n";
//			}
//			conn.disconnect();
//
//
//
//			if (status == 200) {
//				log.debug("Emitting New Content....-----> "+ url);
//				collector.emit(new Values<Object>(url, content, status,type));
//				//System.out.println("number left:" +maxNumFile);
//				return true;
//			} else {
//				log.debug(url + " : Not Modified.....");
//				return false;
//			}
//			
//					
//
//		} else {
//			System.out.println(url + " : Other Invalid Response... Skipping....");
//			conn.disconnect();
//			return false;
//		}
//		
//
//
//	}
//	

	
	

}
