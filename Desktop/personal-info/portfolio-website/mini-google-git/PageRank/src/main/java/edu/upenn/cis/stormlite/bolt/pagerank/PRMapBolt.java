package edu.upenn.cis.stormlite.bolt.pagerank;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import edu.upenn.cis455.mapreduce.Workerstatus;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.*;
import edu.upenn.cis.stormlite.distributed.EOSChecker;

import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.PRSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.PRJob;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "map"
 * on a per-tuple basis.
 * 
 * 
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class PRMapBolt implements IRichBolt {

	PRJob mapJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	
	/*
	 * check if all eos received.
	 */
	EOSChecker eosChecker;
	/*
	 * marked true if emitted EOS to down stream bolt.
	 */
	boolean sentEos = false;
	
	static boolean isFirstMapBolt = false;
	
	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    
    
    public PRMapBolt() {
    	
    }

    
	/**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        if (!stormConf.containsKey("mapClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("mapClass");
        	
        	try {
				mapJob = (PRJob)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        
        if (!stormConf.containsKey("spoutExecutors")) {
        	throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        }

		System.out.println("[MapBolt]: StormConfig = " + stormConf.toString());
        
        // determine how many end-of-stream requests are needed, create a ConsensusTracker
        // or whatever else you need to determine when votes reach consensus
		
		int votedNeeded = 1;
		int numWorkers = stormConf.get("workerList").split(",").length ;
		if (isFirstMapBolt) {
			int sproutExecutors = 1;
			votedNeeded = numWorkers * sproutExecutors;
			//System.out.println("[FirstPRMapBolt]: votedNeeded = " + votedNeeded + ", id = " + getExecutorId());
		} else {

			int reduceExecutors = Integer.parseInt(stormConf.getOrDefault("reduceExecutors", "1"));
	
			votedNeeded = numWorkers * reduceExecutors;
			//System.out.println("[PRMapBolt]: votedNeeded = " + votedNeeded+ ", id = " + getExecutorId());
		}
		eosChecker = new EOSChecker(votedNeeded);
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public synchronized boolean execute(Tuple input) {
    	// READ from PRSpout
    	if (!input.isEndOfStream()) {
			WorkerAdmin.getInstance().updateWorkerStatus(Workerstatus.MAPPING);
	        String key = input.getStringByField("key");    // url 	      
	        String value = input.getStringByField("value");// outUrl or "SINK"
	        
	        if (sentEos) {
	        	throw new RuntimeException("[MAPBOLT]: We received data from " + input.getSourceExecutor() + " after we thought the stream had ended!");
	        }

			Context outputContext = new Context() {
				@Override
				public void write(String key, String value) {
					// System.out.println("[MAPBOLT]: write to reduceBolt: { " + key + " | " + value + "}. " + getExecutorId());
					collector.write(key, value);
					
				}
			};
			mapJob.map(key, value, outputContext);

    	} else if (input.isEndOfStream()) {
			if (this.eosChecker.addVoterAndCheckEos(input.getSourceExecutor(), "MapBolt") && !sentEos) {
				collector.emitEndOfStream(this.getExecutorId());
				sentEos = true;
				WorkerAdmin.getInstance().updateWorkerStatus(Workerstatus.WAITING);
//			    try {
//					// addUrlCnt(PRSpout.outUrlCnt);
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
    	}
    	return true;
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
	
	/*
	 * Send sink node to the master
	 */

	public synchronized void addUrlCnt(AtomicInteger outUrlCnt) throws IOException {
			System.out.println("[PRMapBolt]: addUrlCnt(), outUrlCnt = " + outUrlCnt);
	        StringBuilder sb = new StringBuilder();
	        sb.append(WorkerAdmin.masterLocation + "/addCnt?");
	        sb.append("outurlcnt=" + outUrlCnt);
	        // sb.append("&outurlcnt=" + outUrlCnt);
	        URL url = new URL(sb.toString());
	        HttpURLConnection conn;
	        conn = (HttpURLConnection)url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.connect();
	        conn.getResponseCode();
	}
}
