package edu.upenn.cis455.crawler.masterServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.crawler.bolts.CrawlerBolt;
import edu.upenn.cis455.crawler.bolts.CrawlerQueueSpout;
import edu.upenn.cis455.crawler.bolts.DocParserBolt;
import edu.upenn.cis455.crawler.bolts.URLDistributeBolt;
import edu.upenn.cis455.crawler.bolts.URLFilterBolt;

public class CrawlerMasterUtil {
	static Logger log = Logger.getLogger(CrawlerMasterUtil.class);
	// bolt info 
	private static final String QUEUE_SPOUT = "QUEUE_SPOUT";
	private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
	//private static final String DOC_PARSER_BOLT = "DOC_PARSER_BOLT";
	private static final String URL_FILTER_BOLT = "URL_FILTER_BOLT";
	private static final String URL_DISTRIBUTE_BOLT = "URL_DISTRIBUTE_BOLT";
	
	// http_timeout
	public static final int HTTP_TIMEOUT = 70000;
	public static final int READFILE_TIMEOUT = 60000;
	

	
	
	public static void setTopology(String maxNumFile, String maxSize, List<String> startURLs) {
		
		
				
				// create configuration object     
				Config config = new Config();
				String workersList = String.join("," ,CrawlerMaster.workerStatusMap.keySet());
				log.debug("Worker List is : "+workersList);
				config.put("job", "crawler");
				config.put("workerList", workersList);
		        config.put("maxFileSize", maxSize);
				config.put("maxNumFile", maxNumFile);
				
				
				CrawlerQueueSpout spout = new CrawlerQueueSpout();
				CrawlerBolt crawlerbolt = new CrawlerBolt();
			//	DocParserBolt parser = new DocParserBolt();
				URLFilterBolt urlfilter = new URLFilterBolt();
				URLDistributeBolt urldistribute = new URLDistributeBolt();
				
				/***
				 * function: 		                                                        generate_hash_id
				 *            check_delay           get_content                extract           filterURL
			 	 *          crawler_sprout =======> crawler_bolt ============> parser ==========> urlfilter
		           emit:          <url>            <url, content>          <url, outLink>         <out_link>
				  database:                       <url_id, content>                          <url _id, outLink>      
				 * 
				 * 
				 * ****/
				// define stormlite topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout(QUEUE_SPOUT, spout, 8);
				builder.setBolt(URL_DISTRIBUTE_BOLT, urldistribute, 5).fieldsGrouping(QUEUE_SPOUT, new Fields("url"));
				builder.setBolt(CRAWLER_BOLT, crawlerbolt, 47).shuffleGrouping(URL_DISTRIBUTE_BOLT);
			//	builder.setBolt(DOC_PARSER_BOLT, parser, 10).fieldsGrouping(CRAWLER_BOLT, new Fields("url"));
				builder.setBolt(URL_FILTER_BOLT, urlfilter, 40).shuffleGrouping(CRAWLER_BOLT);
				Topology topo = builder.createTopology();
				

				
		        ObjectMapper mapper = new ObjectMapper();


				 String[] workers = WorkerHelper.getWorkers(config);
			      mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
			      try {
			    	  int i = 0;
						for(String worker : workers) {
							config.put("workerIndex", String.valueOf(i++));
							config.put("seedURL", startURLs.get(i-1));
							//create new worker job
							WorkerJob workerJob = new WorkerJob(topo, config);
							String jsonToSend = mapper.writeValueAsString(workerJob);
						//	log.info("sending json in master: "+ jsonToSend);
						if(sendJobs("POST", worker,jsonToSend,"definejob")!=200) throw new RuntimeException("defineJob request fail");
					}
					
					
					i = 0;
					for(String worker : workers) {
						//config.put("workerIndex", String.valueOf(i++));
						//create new worker job
						//WorkerJob workerJob = new WorkerJob(topo, config);
						//String jsonToSend = mapper.writeValueAsString(workerJob);
						if(sendJobs("POST", worker,null,"runjob")!=200) throw new RuntimeException("runJob request fail");
					}
					
			      } catch (IOException e) {
					// TODO Auto-generated catch block
			    	  e.printStackTrace();
			    	  log.debug("json writing output wrong. or connection error" + e);
			      }
	}
	
	private static int sendJobs(String requestType, String ip_port, String data, String path) throws IOException {
		URL url = new URL(ip_port + "/" + path);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(requestType);
		if(requestType.equals("POST") && data!=null) {
			conn.setRequestProperty("Content-Type", "application/json");
			OutputStream os = conn.getOutputStream();
			os.write(data.getBytes());
			os.flush();
		}
			
		return conn.getResponseCode();
	}
	
	
	
	public static String writeStatusPage() {
		StringBuilder sb = new StringBuilder();
		sb.append("<html>");
		sb.append("<head>\n" + 
				"<style>\n" + 
				"table {\n" + 
				"  font-family: arial, sans-serif;\n" + 
				"  border-collapse: collapse;\n" + 
				"  width: 100%;\n" + 
				"}\n" + 
				"\n" + 
				"td, th {\n" + 
				"  border: 1px solid #dddddd;\n" + 
				"  text-align: left;\n" + 
				"  padding: 8px;\n" + 
				"}\n" + 
				"\n" + 
				"tr:nth-child(even) {\n" + 
				"  background-color: #dddddd;\n" + 
				"}\n" + 
				"</style>\n" + "<p>Written by: <font color=\"red\">Jingwen Qiang</font>"
				+ "<p>Master Ip Address: <font color=\"red\">"+CrawlerMaster.masterIpPort+"</font>"+
				"<p>This Time URLProcessed:  <font color=\"red\">"+getTotalProcessed()+"</font>"+
				"<p>Total URL Processed:  <font color=\"red\">"+(CrawlerMaster.urlCrawled + getTotalProcessed())+"</font>"+
				"</head>\n" + 
				"<body>\n" );
		sb.append(generatworkerTable());
		sb.append(writeRunJobForm());
		sb.append("</body>\n" + 
				"</html>");
		
		return sb.toString();
		
	}
	
	private static int getTotalProcessed(){
		int res = 0;
		for(Map<String, String> worker : CrawlerMaster.workerStatusMap.values()) {
			res+=Integer.parseInt(worker.get("fileProcessed"));
		}
		
		return res;
		
	}	
	
	public static String generateWorkerStatus(String name, Map<String, String> workerInfo) {
		StringBuilder sb = new StringBuilder();
		sb.append("<tr>");
		sb.append("<td>"+name+"</td>");
		sb.append("<td>"+workerInfo.get("ip_port")+"</td>");
		sb.append("<td>"+workerInfo.get("fileProcessed")+"</td>");
		sb.append("<td>"+workerInfo.get("fileLeftToCrawl")+"</td>");
		sb.append("<td>"+workerInfo.get("averagePerFile")+"</td>");
		sb.append("</tr>");
		
		return sb.toString();
	}
	
	public static String generatworkerTable() {
		StringBuilder sb = new StringBuilder();
		// write headers
		sb.append( 
				"<h3>Worker Status</h3>\n" + 
				"\n" + 
				"<table>\n" + 
				"  <tr>\n" + 
				"    <th>Worker</th>\n" + 
				"    <th>IP:Port</th>\n" + 
				"    <th># of Files Crawled</th>\n" + 
				"    <th># of Files Left to Crawl</th>\n" + 
				"    <th>Average Process Time Per File</th>\n" + 
				"  </tr>");
		
		for(String worker: CrawlerMaster.workerStatusMap.keySet()) {
			if(System.currentTimeMillis()-Long.parseLong(CrawlerMaster.workerStatusMap.get(worker).get("last_accessed")) < 300000) {
				sb.append(generateWorkerStatus(worker, CrawlerMaster.workerStatusMap.get(worker)));
				
			}else {
				log.debug("worker Id: "+ worker + "Inactive");
				CrawlerMaster.workerStatusMap.remove(worker);
			}
			
		}
		
		sb.append("</table>");
		
		return sb.toString();
		
	}
	
	public static String writeRunJobForm() {
		StringBuilder sb = new StringBuilder();
		sb.append("<h3>Submitting Job</h3>");
		sb.append("<form method = \"POST\" name =\"statusform\" action = \"/create\" ><br/>");
		sb.append("Max File Size Per File: <input type = \"text\" \" name = \"maxFileSize\"/><br/>");
		sb.append("Max Number of File: <input type = \"text\"  \" name = \"maxNumFile\"/><br/>");
		sb.append("<input type = \"submit\" value = \"crawl\"/></form>");
		
		return sb.toString();
			
	}
	
	public static String shutdown() {
		for(String worker: CrawlerMaster.workerStatusMap.keySet()) {
			String ip = "";
			try {
				ip = CrawlerMaster.workerStatusMap.get(worker).get("ip_port");
				sendJobs("GET",ip,null,"/shutdown");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.debug("worker shutting down inproperly at: "+ip);
				e.printStackTrace();
			}
		}
		CrawlerMaster.urlCrawled = CrawlerMaster.urlCrawled+getTotalProcessed();
		
		return "All Worker has been shut down";
		
	}
	

	
	

}


