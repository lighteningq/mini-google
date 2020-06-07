package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.Map.Entry;


import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public abstract class FileSpout implements IRichSpout {

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * WordSpout, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	/**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;

	/**
	 * This is a simple file reader
	 */
	String filename;
	BufferedReader reader;
	Random r = new Random();

	// For default, read all files under "inputDir"
	String inputDir = ""; 
	Map<String, BufferedReader> readersMap = new HashMap<>();
	BufferedReader currReader = null;
	String currFileName = null;
	Iterator<Entry<String, BufferedReader>> itr;

	int inx = 0;
	boolean sentEos = false;

	public FileSpout() {
		filename = getFilename();
	}

	public abstract String getFilename();

	/**
	 * Initializes the instance of the spout (note that there can be multiple
	 * objects instantiated)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		if (conf.containsKey("inputDir")) {
			this.inputDir = WorkerAdmin.workerStorage + "/" + conf.get("inputDir");
		} else {

			this.inputDir = WorkerAdmin.workerStorage;
		}

		try {
			// For default, read all files under "inputDir"
	    	System.out.println("[Spout]: Starting spout from diretory:  " + this.inputDir);
			File directory = new File(this.inputDir);
			for (File f : directory.listFiles()) {
				BufferedReader reader = new BufferedReader(new FileReader(f));
				this.readersMap.put(f.getName(), reader);
			}
			this.itr = this.readersMap.entrySet().iterator();
			if(itr.hasNext()) {
				Map.Entry<String,BufferedReader> entry = itr.next();
				this.currFileName = entry.getKey();
				this.currReader = entry.getValue();
			}
			System.out.println("[FileSpout]: readersMap.size() = " + readersMap.size());

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Shut down the spout
	 */
	@Override
	public void close() {
		if (reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	/**
	 * The real work happens here, in incremental fashion. We process and output the
	 * next item(s). They get fed to the collector, which routes them to targets
	 */
	@Override
	public synchronized boolean nextTuple() {

		// For default, emit(currFileName, each line), only support one spout

    	if (this.currReader != null  && !sentEos) {
	    	try {
		    	String line = this.currReader.readLine();
		    	
		    	if(line == null && this.itr.hasNext()) {
		    		
		    			Map.Entry<String,BufferedReader> entry = this.itr.next();
		    			this.currFileName = entry.getKey();
		    			this.currReader = entry.getValue();
		    			line = currReader.readLine();
		    			System.out.println("[FileSpout]: reading file = " + currFileName + ", firstline  = " + line);
		    			
		    	} else if (line == null && !this.itr.hasNext() && !sentEos) {
		    		this.collector.emitEndOfStream(getExecutorId());
		    		this.sentEos = true;
		    		// return false;
		    	}
		    		
		    	
		    	if (line != null) {
		        	 // System.out.println("[Spout]: read from file : {" + currFileName + ", " + line + "}");
		    	     this.collector.emit(new Values<Object>(currFileName, line));

		    	} else if (!this.readersMap.entrySet().iterator().hasNext() && !sentEos) {
	    	        this.collector.emitEndOfStream(getExecutorId());
	    	        this.sentEos = true;
		    	}
		    	
//		    	try {
//		    		Thread.sleep(100);
//				} catch (Exception e) {
//		    		e.printStackTrace();
//				}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}
	        Thread.yield();
	        return true;
	        
    	} else {
    		return false;
    	}
    	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "value"));
	}

	@Override
	public String getExecutorId() {

		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}
