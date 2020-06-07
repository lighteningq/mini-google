package edu.upenn.cis455.mapreduce.master.routes;

import com.amazonaws.services.guardduty.model.Master;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.PrintBolt;

import edu.upenn.cis.stormlite.bolt.pagerank.PRMapBolt;
import edu.upenn.cis.stormlite.bolt.pagerank.PRFirstMapBolt;
import edu.upenn.cis.stormlite.bolt.pagerank.PRLastReduceBolt;
import edu.upenn.cis.stormlite.bolt.pagerank.PRReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerJob;

import edu.upenn.cis.stormlite.spout.PRSpout;
import edu.upenn.cis.stormlite.tuple.Fields;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;

import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import java.util.Arrays;

/*
 * Handle /submitjob.
 */
public class SumitJobHandler implements Route {

    private static final String FILE_SPOUT = "FILE_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCER_BOLT = "REDUCER_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";
    
    private MasterAppConfig master;
    

    public SumitJobHandler(MasterAppConfig master) {
        this.master = master;
    }
    
    /*
     * Handle POST /submitjob.
     */
    @Override
    public Object handle(Request request, Response response) throws IOException {
    	master.cleanHashToBolt();
    	master.cleanWorkerAddrs();
        JobInfo job = getJobInfo(request);
        String[] workersList = master.getWorkersArray();
        int result = sendJobToAll(workersList, job);
        return result + " workers success. Total workers:  " + workersList.length + ".";
    }

    /*
     * sent job to all workers
     */
    private int sendJobToAll(String[] workersList, JobInfo jobInfo) {
        int numSuccess = 0;
        Topology topo = createTopologyWith(jobInfo);
        for (int i = 0; i < workersList.length; i++) {
            String dest = workersList[i];
            Config config = createConfigWith(jobInfo, i, workersList);
            WorkerJob wj = new WorkerJob(topo, config);
            try {
                if (postJob(wj, dest).getResponseCode() < 300) {
                    numSuccess++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        master.setJobReady(true);
        return numSuccess;
    }

    /*
     * get job info form the form in status page.
     */
    private JobInfo getJobInfo(Request req) throws IOException {
        JobInfo jobInfo = new JobInfo();
        jobInfo.className = req.queryParams("class_name");
        jobInfo.inputDir = req.queryParams("input_directory_path");
        jobInfo.outputDir = req.queryParams("output_directory_path");
        
        jobInfo.mapExecutors =  parseOrOne(req.queryParams("num_map_bolts"));
        jobInfo.reduceExecutors = parseOrOne(req.queryParams("num_reduce_bolts"));
        jobInfo.iterations = parseOrOne(req.queryParams("num_iterations"));
        return jobInfo;
    }
    
    /*
     * parse to integer, default to 1.
     */
    private int parseOrOne(String s) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return 1;
        }
    }
    
    /*
     * create config with jobinfo.
     */
    private Config createConfigWith(JobInfo jobInfo, int index, String[] workersList) {
        Config config = new Config();
        config.put("job", jobInfo.className);
        config.put("workerList", Arrays.toString(workersList));
        config.put("workerIndex", String.valueOf(index));
        config.put("mapClass", jobInfo.className);
        config.put("reduceClass", jobInfo.className);
        config.put("inputDir", jobInfo.inputDir);
        config.put("outputDir", jobInfo.outputDir);
        config.put("spoutExecutors", "1");
        config.put("mapExecutors",  String.valueOf(jobInfo.mapExecutors));
        config.put("reduceExecutors", String.valueOf(jobInfo.reduceExecutors));
        config.put("iterations", String.valueOf(jobInfo.iterations));
        return config;
    } 
    
    private class JobInfo {
        String className, inputDir, outputDir;
        int mapExecutors, reduceExecutors, iterations;
    }

    /*
     * create topology with job info
     */
    private Topology createTopologyWith(JobInfo jobInfo) {
    	
    	PRSpout pageRankSpout = new PRSpout(); 
        PRMapBolt mapBolt = new PRMapBolt();
        PRFirstMapBolt firstMapBolt = new PRFirstMapBolt();
        
        PRReduceBolt reduceBolt = new PRReduceBolt();
        PRLastReduceBolt lastReduceBolt = new PRLastReduceBolt();
        PrintBolt printer = new PrintBolt();
        
        int iterations = jobInfo.iterations;
    
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(FILE_SPOUT, pageRankSpout, 1);
        builder.setBolt(MAP_BOLT+"1", firstMapBolt, jobInfo.mapExecutors).fieldsGrouping(FILE_SPOUT, new Fields("key"));
        
        for (int i = 1; i < iterations; i++) {
        	builder.setBolt(REDUCER_BOLT + i, reduceBolt, jobInfo.reduceExecutors).fieldsGrouping(MAP_BOLT + i, new Fields("key"));
        	builder.setBolt(MAP_BOLT + String.valueOf(i+1), mapBolt, jobInfo.mapExecutors).fieldsGrouping(REDUCER_BOLT + i, new Fields("key"));
        }
        
        builder.setBolt(REDUCER_BOLT + iterations, lastReduceBolt, jobInfo.reduceExecutors).fieldsGrouping(MAP_BOLT + iterations, new Fields("key"));
        builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCER_BOLT + iterations);
        
        return builder.createTopology();
    }

    /*
     * prepare to send POST /definejob to worker, WorkerJob as payload. 
     */
    public HttpURLConnection postJob(WorkerJob workerJob, String dest) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        String tempDest = "http://" + dest;
        try {
            return postJob(tempDest, "POST", "definejob", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(workerJob));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    /*
     * send POST /definejob to worker, WorkerJob as payload. 
     */
    public HttpURLConnection postJob(String dest, String reqType,  String job, String parameters) throws IOException {
        URL url = new URL(dest + "/" + job);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();

        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);
        conn.setRequestProperty("Content-Type", "application/json");
        OutputStream os = conn.getOutputStream();
        byte[] toSend = parameters.getBytes();
        os.write(toSend);
        os.flush();
        return conn;
    }
}
