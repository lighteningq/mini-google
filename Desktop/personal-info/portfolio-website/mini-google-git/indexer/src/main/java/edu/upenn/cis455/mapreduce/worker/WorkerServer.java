package edu.upenn.cis455.mapreduce.worker;

import static spark.Spark.setPort;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.indexer.storage.DBIndexer;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 * Simple listener for worker creation
 * 
 * @author zives
 *
 */
public class WorkerServer {
	static Logger log = Logger.getLogger(WorkerServer.class);

	static DistributedCluster cluster = new DistributedCluster();

	List<TopologyContext> contexts = new ArrayList<>();

	int myPort;

	static int workerNum = 0;

	String masterIpPort;
	String storageDir;
	String jobClass;
	String status = "idle";
	
	static DBIndexer dbIndexer;
	
	// the directory to store BDBs
//    public static final String DBDirName = "DB_temp";

	static List<String> topologies = new ArrayList<>();
	
	static long startTime = System.currentTimeMillis();
	static long runningTime;

	/**
	 * A thread that sends http /status to master every 10 seconds
	 */
	private Thread timer;

	/**
	 * constructor
	 * 
	 * @param myPort
	 * @throws MalformedURLException
	 */
	public WorkerServer(int myPort) throws MalformedURLException {
		this.myPort = myPort;
		workerNum++;
		log.info("Creating server listener at socket " + myPort);

		setPort(myPort);
		final ObjectMapper om = new ObjectMapper();
		om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		Spark.post("/definejob", new Route() {

			@Override
			public Object handle(Request arg0, Response arg1) {

//                	String DBDirFullPath = storageDir+DBDirName;
//                	clearDB(DBDirFullPath);  // clear old DB data

				WorkerJob workerJob;
				try {
					workerJob = om.readValue(arg0.body(), WorkerJob.class);
					// pass storageDir to printBolt and other bolts
					workerJob.getConfig().put("storageDir", storageDir);
					jobClass = workerJob.getConfig().get("jobClass");
					// DB store dir
					String DBStoreDir = storageDir;
					DBStoreDir += "DBIndexer";
					dbIndexer = new DBIndexer(DBStoreDir);
					log.info("Initiate BDB in " + DBStoreDir); //// debug

					try {
						// set thread number in thread pool
						cluster.setThreadNum(Integer.parseInt(workerJob.getConfig().get("threadNum")));
						
						log.info("Processing job definition request" + workerJob.getConfig().get("job") + " on machine "
								+ workerJob.getConfig().get("workerIndex"));
						contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(),
								workerJob.getTopology()));

						synchronized (topologies) {
							topologies.add(workerJob.getConfig().get("job"));
						}
					} catch (ClassNotFoundException e) {
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
				setStartTime();
				log.info("Starting job!");
				cluster.startTopology();
				
				return "Started";
			}
		});

		Spark.post("/shutdown", new Route() {

			@Override
			public Object handle(Request arg0, Response arg1) {
				log.info("Shutting down server!");
				shutdown();
				timer.interrupt();

				return "Shutted";
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
					e.printStackTrace();

					arg1.status(500);
					return e.getMessage();
				}

			}

		});

	}

	/**
	 * masterIpPort Setter
	 * 
	 * @param masterIpPort
	 */
	public void setMasterIpPort(String masterIpPort) {
		this.masterIpPort = masterIpPort;
	}

	public void setStorageDir(String storageDir) {
		this.storageDir = storageDir;
	}

	public void setJobClass(String jobClass) {
		this.jobClass = jobClass;
	}

	/**
	 * start the timer. It sends status to the master every 10 seconds.
	 */
	public void startTimer() {
		timer = new Thread(() -> {
			try {
				while (true) {
					try {
						int spoutOutputs = 0;
						int mappersOutputs = 0;
						int reducersOutputs = 0;
						if (contexts.size() == 0) {
							jobClass = "None";
							runningTime = 0;
						} else {
							TopologyContext context = contexts.get(contexts.size() - 1);
							if (context.getState().equals(TopologyContext.STATE.INIT)) {
								status = "idle";  // running time remains
							} else if (context.getState().equals(TopologyContext.STATE.MAP)) {
								status = "mapping";
								runningTime = System.currentTimeMillis()-startTime;
							} else if (context.getState().equals(TopologyContext.STATE.REDUCE)) {
								status = "reducing";
								runningTime = System.currentTimeMillis()-startTime;
							} else {
								status = "waiting";
								runningTime = System.currentTimeMillis()-startTime;
							}
							spoutOutputs = context.getSpoutOutput();
							mappersOutputs = context.getMapOutputs();
							reducersOutputs = context.getReduceOutputs();
						}
						String urlString = "http://" + masterIpPort + "/workerstatus";
						urlString += ("?port=" + myPort);
						urlString += ("&status=" + status);
						urlString += ("&job=" + jobClass);
						urlString += ("&spoutOutputs=" + spoutOutputs);
						urlString += ("&mappersOutputs=" + mappersOutputs);
						urlString += ("&reducersOutputs=" + reducersOutputs);
						urlString += (String.format("&runningTime=%.2fs",(runningTime*1.0/1000.0)));

						URL url = new URL(urlString);

						System.out.println("Sending request to " + url.toString());

						HttpURLConnection conn = (HttpURLConnection) url.openConnection();
						conn.setDoOutput(true);
						conn.setRequestMethod("GET");
						BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
						String line = br.readLine();
						System.out.println(line);
						if (conn.getResponseCode() != HttpURLConnection.HTTP_OK || line.equals("fail")) {
							throw new RuntimeException("Job execution request failed");
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}

					Thread.sleep(10000);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		timer.start();

//		Thread timer2 = new Thread(() -> {
//			try {
//				while (true) {
//					try {
//						if (contexts.size() != 0) {
//							TopologyContext context = contexts.get(contexts.size() - 1);
//							System.out.println("STATUS: " + context.getState());
//						}
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//
//					Thread.sleep(1000);
//				}
//			} catch (InterruptedException e) {
//				Thread.currentThread().interrupt();
//			}
//		});
//		timer2.start();

	}

	/**
	 * static function for creating a worker
	 * 
	 * @param config
	 */
	public static void createWorker(Map<String, String> config) {
		if (!config.containsKey("workerList"))
			throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

		if (!config.containsKey("workerIndex"))
			throw new RuntimeException("Worker spout doesn't know its worker ID");
		else {
			String[] addresses = WorkerHelper.getWorkers(config);
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

			log.debug("Initializing worker " + myAddress);
			URL url;
			try {
				url = new URL(myAddress);

				WorkerServer workerServer = new WorkerServer(url.getPort());
				workerServer.setMasterIpPort(config.get("masterIpPort"));
				workerServer.setStorageDir(config.get("storageDir"));
				workerServer.startTimer();

			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void shutdown() {
		synchronized (topologies) {
			for (String topo : topologies)
				cluster.killTopology(topo);
		}

		cluster.shutdown();
		dbIndexer.shutdownDB();
	}

	/**
	 * delete the old DB files
	 */
	public static void clearDB(String DBDirFullPath) {
		File file = new File(DBDirFullPath);
		if (file.exists() && file.isDirectory()) {
			deleteDir(DBDirFullPath);
		}
		file.mkdirs();
	}

	/**
	 * delete the whole directory
	 * 
	 * @param dirPath
	 */
	public static void deleteDir(String dirPath) {
		File file = new File(dirPath);
		if (file.exists()) {
			if (file.isFile()) {
				file.delete();
			} else {
				File[] files = file.listFiles();
				if (files == null) {
					file.delete();
				} else {
					for (int i = 0; i < files.length; i++) {
						deleteDir(files[i].getAbsolutePath());
					}
					file.delete();
				}
			}
		}
	}
	
	/**
	 * reset start time
	 */
	public static void setStartTime() {
		startTime = System.currentTimeMillis();
	}
	
	public static DBIndexer getDBIndexer() {
		return dbIndexer;
	}
}
