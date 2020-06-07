
package edu.upenn.cis.stormlite.bolt.pagerank;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Workerstatus;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;


import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.*;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.distributed.EOSChecker;
import edu.upenn.cis.stormlite.distributed.WorkerUtils;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.PRJob;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
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

public class PRLastReduceBolt implements IRichBolt {
	
	PRJob reduceJob;

	/**
	 * This object can help determine when we have
	 * reached enough votes for EOS
	 */
	EOSChecker eosChecker;

	/**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEof = false;
	
	String outputFilePath;
	
	

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context; 
    boolean writtenToBolt = false;
    
    int neededVotesToComplete = 0;
    
    int totalIterations = 1;
    
	/**
	 * Buffer for state, same function as stateByKey map.
	 */
    private DBWrapper reduceDB;
    private DBWrapper resDB;
    String myWorkerAddr = null;
	
	public PRLastReduceBolt() {
    
	}  

	
	public String getMyWorkerAddr() {
		return this.myWorkerAddr;
	}
	
    public void createDir(String dir) {
    	if(!Files.exists(Paths.get(dir))) {
    		try {
				Files.createDirectories(Paths.get(dir));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
	
	
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        
        String reducePath = WorkerAdmin.dbStore + "/" + WorkerAdmin.reducePrefix;
        createDir(reducePath);
        this.reduceDB =  new DBWrapper(reducePath);
        
        String resPath = WorkerAdmin.dbStore + "/" + "Worker" + stormConf.get("workerIndex");
        createDir(resPath);
        this.resDB =  new DBWrapper(resPath);
        
        this.totalIterations = Integer.parseInt(stormConf.get("iterations"));
        this.outputFilePath = WorkerAdmin.workerStorage + "/" + stormConf.get("outputDir") + "/" + WorkerAdmin.outputFileName;
        // System.out.println("[ReduceBolt]:OutputFilePath = " + outputFilePath);
        
        if (!stormConf.containsKey("reduceClass")) {
        	throw new RuntimeException("Mapper class is not specified as a config option");
        } else {
        	String reduceClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (PRJob)Class.forName(reduceClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + reduceClass);
			}
        }
        
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

        // Determine how many votes to complete
		int numWorkers = stormConf.get("workerList").split(",").length;
        int numMapExecutors = Integer.parseInt(stormConf.getOrDefault("mapExecutors", "1"));
        this.neededVotesToComplete = numMapExecutors  * numWorkers;
		System.out.println("[ReduceBolt]: neededVotesToComplete = " + neededVotesToComplete +", id = " + getExecutorId());
        this.eosChecker = new EOSChecker(neededVotesToComplete);
        
        // send BoltId to Master.

        
        
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized boolean execute(Tuple input) {
    	
    	if (sentEof) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
	        return false;
		} else if (input.isEndOfStream() ) {
			
    		// WRITE: the associated key, and output all state
			// System.out.println("[ReduceBolt]: input.isEndOfStream() = true. " + getExecutorId());
			if (this.eosChecker.addVoterAndCheckEos(input.getSourceExecutor(), "ReduceBolt") && !writtenToBolt ) {
				
				Context outputContext = new Context() {
					@Override
					public void write(String key, String value) {
						// write result to next bolt
						collector.write(key, value);
						
						
						// write to master for debugging
						try {
							int iterIdx = key.indexOf("_");
				    		int iter = Integer.parseInt(key.substring(0, iterIdx));
				    		
				    		if (iter > totalIterations) {
				    			// write final result to DB:
				    			key = key.substring(iterIdx+1);
				    			int hypenIdx = value.indexOf("-");
				    			if (hypenIdx != -1) {
				    				value = value.substring(0, hypenIdx);
				    			}
				    			resDB.resPut(key, value);
				    			//WorkerAdmin.getInstance().sendResToMater(key, value);
				    		}
						} catch (Exception e) {
							e.printStackTrace();
						}
						
						// write to partial results to worker's own outputDir
						/*String kvpair = key + "," + value + "\n";
				        BufferedWriter writer = null;
				        	
						try {
							writer = new BufferedWriter(new FileWriter(outputFilePath, true));
							writer.append(kvpair);
							writer.close();
						} catch (IOException e) {
							e.printStackTrace();
						} 
						System.out.println("[ReduceBolt]: Worker result:  " + key + "," + value);*/
				        
					}
				};
				
				System.out.println("[ReduceBolt]: Agreed, reduceMap key = " + reduceDB.keySet() +". "+ getExecutorId() + "\n");
				
				//get iterations:
				int currIter = 1; 
				for (String key : reduceDB.keySet()) {
					int underScoreIdx = key.indexOf("_");
					currIter = Integer.parseInt(key.substring(0, underScoreIdx));
					System.out.println("[Reducebolt]: currIter = " + currIter);
					break;
				}
				
				for (String key : reduceDB.keySet()) {
					// System.out.println("[ReduceBolt]: is eos, about to reduce:  " + key + ". "+ getExecutorId());
					WorkerAdmin.getInstance().updateKeysWritten();
					if(currIter < totalIterations ) {
						// reduceJob.computeSinkRankShare(currIter);
						reduceJob.reduce(key, reduceDB.get(key).iterator(), outputContext);
					} else {
						reduceJob.reduce(key, reduceDB.get(key).iterator(), outputContext);
					}
				}
				writtenToBolt = true;
				WorkerAdmin.getInstance().updateWorkerStatus(Workerstatus.IDLE);
				collector.emitEndOfStream(this.getExecutorId());
			}

    	} else {
    		// READ: if not EOS, read from MapBolt or ReduceBolt, add to reduceMap
			WorkerAdmin.getInstance().updateWorkerStatus(Workerstatus.REDUCING);
			
    		String key = input.getStringByField("key");
    		
    		String value = input.getStringByField("value");
    		// System.out.println("[ReduceBolt_"+iter+"]: read :  { " + key + " , " + value +  "}. " + getExecutorId());
    		
    		ListOfString values;
    		if (reduceDB.containsKey(key)) {
				values = reduceDB.get(key);
				reduceDB.remove(key);
			} else {
    			values = new ListOfString();
			}
			values.add(value);
			reduceDB.put(key, values);
    		
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
	 * Called during topology setup, sets the router to the next bolt
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

}


