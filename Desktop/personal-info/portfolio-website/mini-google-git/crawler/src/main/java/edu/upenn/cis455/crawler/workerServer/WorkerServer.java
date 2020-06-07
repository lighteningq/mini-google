package edu.upenn.cis455.crawler.workerServer;

import static spark.Spark.setPort;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.DistributedCrawler;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 * Simple listener for worker creation 
 * 
 * @author zives, Jingwen Qiang
 *
 */
public class WorkerServer {
    static Logger log = Logger.getLogger(WorkerServer.class);
        
    static DistributedCluster cluster = new DistributedCluster(50);
    
    List<TopologyContext> contexts = new ArrayList<>();
    
    int myPort;
        
    static List<String> topologies = new ArrayList<>();

	public static String storeDir;
	
	public static WorkerStatus ws = new WorkerStatus();

	public static String dbDir; // /db
	
	public static String masterIpPort;
	
	static WorkerReporter reporter;
	
	static DistributedCrawler crawler;
	
	static String seedUrl; 
	
	public static long startTime = 0;
	
	public static int index;
	
	// http_timeout
	public static final int HTTP_TIMEOUT = 70000;
	public static final int READFILE_TIMEOUT = 60000;
        
    public WorkerServer(int myPort) throws MalformedURLException {
    	
                
        log.info("Creating server listener at socket " + myPort);
        
        setPort(myPort);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        Spark.post("/definejob", new Route() {

                @Override
                public Object handle(Request arg0, Response arg1) {
                        
                    WorkerJob workerJob;
                    try {
                        workerJob = om.readValue(arg0.body(), WorkerJob.class);
                           
                        
                        
                        seedUrl = workerJob.getConfig().get("seedURL");
                        String maxFileSize = workerJob.getConfig().get("maxFileSize");
                        String maxFileCrawl = workerJob.getConfig().get("maxNumFile");
                        storeDir =  "~/crawler_00"+workerJob.getConfig().get("workerIndex");
                        System.out.println("worker index is "+ storeDir + "|| seed url is "+ seedUrl);
                        /** init crawler **/ 
                       //seedUrl = "http://crawltest.cis.upenn.edu";
                        String[] args = {seedUrl, storeDir,maxFileSize, maxFileCrawl};
                        crawler = new DistributedCrawler(args);
        				if(crawler.queue.isEmpty()) {
        					crawler.queue.pushURLToQueue(seedUrl);
        					System.out.println("queue is empty");
        				}
        		        index = Integer.parseInt(workerJob.getConfig().get("workerIndex"));
        				log.debug("current url in crawler: "+ crawler.queue.isEmpty()); 
                        try {
                            log.info("Processing job definition request " + workerJob.getConfig().get("job") +
                                     " on machine " + workerJob.getConfig().get("workerIndex"));
                            contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(), 
                                                                workerJob.getTopology()));
                                                
                            synchronized (topologies) {
                                topologies.add(workerJob.getConfig().get("job"));
                            }
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return "Job launched";
                    } catch (IOException e) {
                        e.printStackTrace();
                                        
                        // Internal server error
                        arg1.status(500);
                        return e.getMessage();
                    } 
                        
                }
                
            });
        
        Spark.post("/runjob", new Route() {

                @Override
                public Object handle(Request arg0, Response arg1) {
                    
                    cluster.startTopology();
                    
                    log.info("Starting job!");      
                    return "Started";
                }
            });
        
        Spark.post("/push/:stream", new Route() {

                @Override
                public Object handle(Request arg0, Response arg1) {
                    try {
                        String stream = arg0.params(":stream");
                        Tuple tuple = om.readValue(arg0.body(), Tuple.class);
                                        
                        log.debug("Worker received: " + tuple + " for " + stream);
                                        
                        // Find the destination stream and route to it
                        StreamRouter router = cluster.getStreamRouter(stream);
                                        
                        if (contexts.isEmpty())
                            log.error("No topology context -- were we initialized??");
                                        
                        if (!tuple.isEndOfStream())
                            contexts.get(contexts.size() - 1).incSendOutputs(router.getKey(tuple.getValues()));
                                        
                        if (tuple.isEndOfStream())
                            router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1));
                        else
                            router.executeLocally(tuple, contexts.get(contexts.size() - 1));
                                        
                        return "OK";
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                                        
                        arg1.status(500);
                        return e.getMessage();
                    }
                                
                }
                
            });

        // TODO: Handle /shutdown to shut down the worker
        Spark.get("/shutdown", (reqeust, response) -> {
        	log.info("Worker Shutting Down......");
        	crawler.getDB().shutdownDB();
        	WorkerServer.shutdown();
        	return "ShutDown WorkerServer";
        	
        });

    }
        
    public static void createWorker(Map<String, String> config) {
        if (!config.containsKey("workerList"))
            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

        if (!config.containsKey("workerIndex"))
            throw new RuntimeException("Worker spout doesn't know its worker ID");
        else {
            String[] addresses = WorkerHelper.getWorkers(config);
            String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];
            
            log.debug("Initializing worker and worker reporter" + myAddress);
            
            URL url;
           

           // if(!new File(storeDir).isDirectory()) dbDir = System.getProperty("user.dir") +"/"+storeDir;
            masterIpPort = config.get("masterIpPort");
            try {
                url = new URL(myAddress);
                
                // Init server and reporter
               new WorkerServer(url.getPort());
                reporter = new WorkerReporter(masterIpPort,Integer.toString(url.getPort()));
            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    public static DistributedCrawler getCrawler() {
    	return crawler;
    }
    

    public static void shutdown() {
        synchronized(topologies) {
            for (String topo: topologies)
                cluster.killTopology(topo);
        }
    	reporter.shutdown();
        cluster.shutdown();
        System.exit(0);
    	log.info("reporter has been shut down for server");
    }
}
