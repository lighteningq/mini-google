package edu.upenn.cis455.crawler.masterServer;
import static spark.Spark.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.upenn.cis455.crawler.CrawlerUtil;
import edu.upenn.cis455.crawler.SearchEngineDB;


public class CrawlerMaster {
	public static Map<String, Map<String,String>> workerStatusMap = new HashMap<>();
	public static List<String> seedURLs = new ArrayList<>();
	public static String masterIpPort;
	static Logger log = Logger.getLogger(CrawlerMaster.class);
	public static volatile int urlCrawled = 0;
	public static SearchEngineDB search = new SearchEngineDB(5);
  public static void main(String args[]) {
	  
	 try {
		masterIpPort = InetAddress.getLocalHost().getHostAddress() + ":8000";
	} catch (UnknownHostException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    port(8000);
    // seed url fixed size 5 for 5 workers
    seedURLs.add("https://www.cdc.gov/");
    seedURLs.add("https://www.weather.com/");
    seedURLs.add("https://www.cnn.com/");
    seedURLs.add("https://www.nytimes.com/");
    seedURLs.add("https://en.wikipedia.com/");
    
    
    /* Just in case someone opens the root URL, without /status... */

    get("/", (request,response) -> {
      return "Please go to the <a href=\"/status\">status page</a>!";
    });

    /* Status page, for launching jobs and for viewing the current status */

    get("/status", (request,response) -> {
      return CrawlerMasterUtil.writeStatusPage();
    });
    
    
    post("/create", (request,response) -> {
    	String maxFile = request.queryParams("maxNumFile");
    	String maxSize = request.queryParams("maxFileSize");
    	log.debug("max num file:"+ maxFile +"|| max size: "+maxSize);
    	CrawlerMasterUtil.setTopology(maxFile,maxSize,seedURLs);
    	
    	response.redirect("/status");
    	return null;
    });

    /* Workers submit requests for /workerstatus; human users don't normally look at this */

    get("/workerstatus", (request,response) -> {
    	//log.debug("inside workersatus");
    	String ip = request.ip();
    	String port = request.queryParams("port");
    	String ip_port = ip+":"+port;
    	String fileProcessed = request.queryParams("fileProcessed");
    	String fileLeft = request.queryParams("fileLeftToCrawl");
    	String avgPerFile = request.queryParams("averagePerFile");
    	
    	Map<String,String> map = new HashMap<>();;
    	map.put("ip_port", ip_port);
    	map.put("fileProcessed", fileProcessed);
    	map.put("fileLeftToCrawl", fileLeft);
    	map.put("averagePerFile", avgPerFile);
    	map.put("last_accessed", Long.toString(System.currentTimeMillis()));
    	workerStatusMap.put(ip_port, map);
    	
      return "200 OK";
    });
    
    get("/geturl", (request,response) -> {
    	String id = request.queryParams("id");
    	String url = search.getUrl(id);
    	log.info("inside request: "+ url );
    	response.type("text/plain");
    	response.status(200);
    	response.body(url);
    	return url;
    	
    });
    
    
    
    get("/shutdownworker", (request,response) ->{
    	String s = CrawlerMasterUtil.shutdown();
    		s+=	"worker has shut down!"+ urlCrawled;
    		
    	return s;
    });
    
    get("/shutdownmaster", (request,response) ->{
    		String s=	"master server has shut down! Total URL Processed: "+ urlCrawled;
    		System.exit(0);
    	return s;
    });
    
    
  }
}
