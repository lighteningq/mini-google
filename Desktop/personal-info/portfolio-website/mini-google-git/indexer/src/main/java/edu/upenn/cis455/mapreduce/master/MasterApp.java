package edu.upenn.cis455.mapreduce.master;

import static spark.Spark.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.indexer.mapreduce.DBCrawlerSpout;
import edu.upenn.cis455.indexer.mapreduce.MapBolt;
import edu.upenn.cis455.indexer.mapreduce.PrintBolt;
import edu.upenn.cis455.indexer.mapreduce.ReduceBolt;
import edu.upenn.cis455.indexer.mapreduce.S3Spout;
import edu.upenn.cis455.mapreduce.WordFileSpout;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

class MasterApp {

	static Map<String, Map<String, String>> workerStatus = new HashMap<String, Map<String, String>>();
	static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss");

	static Logger log = Logger.getLogger(MasterApp.class);

	private static final String DOC_SPOUT = "DOC_SPOUT";
	private static final String MAP_BOLT = "MAP_BOLT";
	private static final String REDUCE_BOLT = "REDUCE_BOLT";
	private static final String PRINT_BOLT = "PRINT_BOLT";
	private static final String SENDER_BOLT = "SENDER_BOLT";

	public static void main(String args[]) {

		port(8080);

		/* Just in case someone opens the root URL, without /status... */

		get("/", (request, response) -> {
			return "Please go to the <a href=\"/status\">status page</a>!";
		});

		/*
		 * Status page, for launching jobs and for viewing the current status The class
		 * name of the job (e.g., edu.upenn.cis.cis455.mapreduce.job.MyJob) The input
		 * directory, relative to the storage directory (e.g., if this is set to bar and
		 * the storage directory is set to ~/foo, the input should be read from
		 * ~/foo/bar) The output directory, relative to the storage directory The number
		 * of map threads (MapBolt executors) to run on each worker The number of reduce
		 * threads (ReduceBolt executors) to run on each worker
		 */

		get("/status", (request, response) -> {
			StringBuilder body = new StringBuilder();
			body.append("<html><body>");
			// extract all the workers status from the hashmap, and
			// check the time if they are active or not
			// (1) IP:port, (2) the status; (3) the job; (4) the keys read, and (5) the keys
			// written
			body.append("<table border=\"1\"><tr><th>IP:port</th><th>status</th>"
					+ "<th>job</th><th>spout outputs</th>"
					+ "<th>mappers outputs</th><th>reducers outputs</th><th>running time</th></tr>");
			for (String ipPort : workerStatus.keySet()) {
				// check time
				Map<String, String> singleWorkerStatus = workerStatus.get(ipPort);
				String time = singleWorkerStatus.get("time");
				LocalDateTime lastTime = LocalDateTime.parse(time, formatter);
				if (lastTime.plusSeconds(30).isBefore(LocalDateTime.now())) {
					continue;
				}
				// active
				body.append("<tr>");
				body.append("<td>").append(ipPort).append("</td>");
				body.append("<td>").append(singleWorkerStatus.get("status")).append("</td>");
				body.append("<td>").append(singleWorkerStatus.get("job")).append("</td>");
				body.append("<td>").append(singleWorkerStatus.get("spoutOutputs")).append("</td>");
				body.append("<td>").append(singleWorkerStatus.get("mappersOutputs")).append("</td>");
				body.append("<td>").append(singleWorkerStatus.get("reducersOutputs")).append("</td>");
				body.append("<td>").append(singleWorkerStatus.get("runningTime")).append("</td>");
				body.append("</tr>");
			}
			body.append("</table>");

			// job submission form
			// inputDir, outputDir, mapNum, reduceNum
			body.append("<p>Please fill the form to start a job: <form action=\"/createJob\" method=\"POST\"></p>"
					+ "<p>The job class name:<input type=\"text\" name=\"jobClass\"/></p>"
					+ "<p>The input directory:<input type=\"text\" name=\"inputDir\"/></p>"
					+ "<p>The output directory:<input type=\"text\" name=\"outputDir\"/></p>"
					+ "<p>The number of map threads on each worker:<input type=\"text\" name=\"mapNum\"/></p>"
					+ "<p>The number of reduce threads on each worker:<input type=\"text\" name=\"reduceNum\"/></p>"
					+ "<p>The number of threads on each worker:<input type=\"text\" name=\"threadNum\"/></p>"
					+ "<input type=\"submit\" value=\"create job\"/></form>");

			body.append("<p>Written by: <font color=\"red\">Kai Zhong</font> login: zhongkai");
			body.append("</body></html>");
			return body.toString();
		});

		/*
		 * Workers submit requests for /workerstatus; human users don't normally look at
		 * this port: the port number on which the worker is listening for HTTP requests
		 * (e.g., port=4711) status: mapping, waiting, reducing or idle, depending on
		 * what the worker is doing (e.g., status=idle) job: the name of the class that
		 * is currently being run (for instance,
		 * job=edu.upenn.cis455.mapreduce.job.MyJob) keysRead: the number of non-EOS
		 * tuples that have been read so far (if the status is mapping), the number of
		 * reduce functions invoked (if the status is reducing), the number of non-EOS
		 * tuples that were read by the last map (if the status is waiting), or zero if
		 * the status is idle. keysWritten: the number of tuples that have been emitted
		 * so far (if the status is mapping or reducing), the number of tuples that were
		 * written by the last map (if the status is waiting) or the number of tuples
		 * that were written by the last reduce (if the status is idle). If the node has
		 * never run any jobs, return 0.
		 * 
		 */
		get("/workerstatus", (request, response) -> {
			/* do something with the information in the request */
			String port = request.queryParams("port");
			String ip = request.ip();
			String status = request.queryParams("status");
			String job = request.queryParams("job");
			String spoutOutputs = request.queryParams("spoutOutputs");
			String mappersOutputs = request.queryParams("mappersOutputs");
			String reducersOutputs = request.queryParams("reducersOutputs");
			String runningTime = request.queryParams("runningTime");
			String time = formatter.format(LocalDateTime.now());

			if (port != null) {
				Map<String, String> singleWorkerStatus = new HashMap<String, String>();
				String key = ip + ":" + port;
				singleWorkerStatus.put("status", status);
				singleWorkerStatus.put("job", job);
				singleWorkerStatus.put("spoutOutputs", spoutOutputs);
				singleWorkerStatus.put("mappersOutputs", mappersOutputs);
				singleWorkerStatus.put("reducersOutputs", reducersOutputs);
				singleWorkerStatus.put("runningTime", runningTime);
				singleWorkerStatus.put("time", time); // test for this one

				System.out.println(key + ", " + time);
				workerStatus.put(key, singleWorkerStatus);
				return "success";
			} else {
				return "fail";
			}
		});
		
		/**
		 * shutdown all known workers
		 */
		get("/shutdown", (request, response) -> {
			/* do something with the information in the request */
			/**     workerList        **/

			Config config = new Config();
			String workerList = "[";
			for(String ipPort:workerStatus.keySet()) {
				workerList += (ipPort+",");
			}
			workerList = workerList.substring(0, workerList.length()-1) + "]";
			System.out.println("workerList: "+workerList);    //// debug
			
			
			config.put("workerList", workerList);
			String[] workers = WorkerHelper.getWorkers(config);
			
			for (String dest : workers) {
				try {
					workerStatus.remove(dest);
//					if (!dest.startsWith("http")) dest = "http://"+dest;
					if (sendJob(dest, "POST", config, "shutdown", "").getResponseCode() != HttpURLConnection.HTTP_OK) {
						throw new RuntimeException("shutdown request failed for "+dest);
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			
			return "shutdown all workers.";
		});

		/**
		 * create a job
		 */
		post("/createJob", (request, response) -> {
			// inputDir, outputDir, mapNum, reduceNum
			String jobClass = request.queryParams("jobClass");
			String inputDir = request.queryParams("inputDir");
			String outputDir = request.queryParams("outputDir");
			String mapNum = request.queryParams("mapNum");
			String reduceNum = request.queryParams("reduceNum");
			String threadNum = request.queryParams("threadNum");

			Config config = new Config();

			// Complete list of workers, comma-delimited
			// *** NOTE: This value is hard-coded here, but this list will be decided based
			// on
			// /workerstatus requests coming to the MasterServlet in your implementation
			
			/**     workerList        **/
			
			String workerList = "[";
			for(String ipPort:workerStatus.keySet()) {
				workerList += (ipPort+",");
			}
			workerList = workerList.substring(0, workerList.length()-1) + "]";
			System.out.println("workerList: "+workerList);    //// debug
			
			
			config.put("workerList", workerList);

			// If we're the Master, we need to initiate the computation
			// Let the server start up

			log.info("************ Creating the job request ***************");
			// Job name
			config.put("job", "MyJob1");

			// IP:port for /workerstatus to be sent
			config.put("masterIpPort", "127.0.0.1:8000");

			// Class with map and reduce function "edu.upenn.cis.stormlite.mapreduce.GroupWords"
			config.put("jobClass", jobClass);
			config.put("inputDir", inputDir);
			config.put("outputDir", outputDir);

			// Numbers of executors (per node)
			config.put("spoutExecutors", "1");
			config.put("mapExecutors", mapNum);
			config.put("reduceExecutors", reduceNum);
			config.put("threadNum", threadNum);

//			DBCrawlerSpout spout = new DBCrawlerSpout();
			S3Spout spout = new S3Spout();
			MapBolt bolt = new MapBolt();
			ReduceBolt bolt2 = new ReduceBolt();
			PrintBolt printer = new PrintBolt();

			TopologyBuilder builder = new TopologyBuilder();

			// Only one source ("spout") for the words
			builder.setSpout(DOC_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));

			// Parallel mappers, each of which gets specific words
			builder.setBolt(MAP_BOLT, bolt, Integer.valueOf(config.get("mapExecutors"))).shuffleGrouping(DOC_SPOUT);

			// Parallel reducers, each of which gets specific words
			builder.setBolt(REDUCE_BOLT, bolt2, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT,
					new Fields("key"));

			// Only use the first printer bolt for reducing to a single point
//			builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);

			Topology topo = builder.createTopology();

			WorkerJob job = new WorkerJob(topo, config);

			ObjectMapper mapper = new ObjectMapper();
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
			try {
				String[] workers = WorkerHelper.getWorkers(config);

				int i = 0;
				for (String dest : workers) {
					config.put("workerIndex", String.valueOf(i++));
					if (sendJob(dest, "POST", config, "definejob",
							mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
									.getResponseCode() != HttpURLConnection.HTTP_OK) {
						throw new RuntimeException("Job definition request failed");
					}
				}
				for (String dest : workers) {
					if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {
						throw new RuntimeException("Job execution request failed");
					}
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				System.exit(0);
			}

			return "Job created.";
		});

	}

	static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters)
			throws IOException {
		URL url = new URL(dest + "/" + job);

		log.info("Sending request to " + url.toString());

		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);

		if (reqType.equals("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");

			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else
			conn.getOutputStream();

		return conn;
	}
}