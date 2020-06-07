package edu.upenn.cis455.indexer.mapreduce;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import edu.upenn.cis455.indexer.storage.DBIndexer;
import edu.upenn.cis455.indexer.storage.IndexEntry;
import edu.upenn.cis455.mapreduce.DBWrapper;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
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

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);

	Job reduceJob;

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * WordCounter, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	Fields schema = new Fields("key", "value");

	boolean sentEos = false;

	/**
	 * Buffer for state, by key
	 */
//	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
	 * This is where we send our output stream
	 */
	private OutputCollector collector;

	private TopologyContext context;

	int neededVotesToComplete;

	int workerNum;

	int numEOS = 0;

	String DBStoreDir;

	public ReduceBolt() {
	}

	/**
	 * Initialization, just saves the output stream destination
	 */
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.context = context;

		if (!stormConf.containsKey("jobClass"))
			throw new RuntimeException("Mapper class is not specified as a config option");
		else {
			String mapperClass = stormConf.get("jobClass");

			try {
				reduceJob = (Job) Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
		}
		if (!stormConf.containsKey("mapExecutors")) {
			throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
		}

		// determine how many EOS votes needed
		this.workerNum = WorkerHelper.getWorkers(stormConf).length;
		int mapNum = Integer.parseInt(stormConf.get("mapExecutors"));
		int reduceNum = Integer.parseInt(stormConf.get("reduceExecutors"));
		neededVotesToComplete = mapNum + (workerNum - 1) * mapNum * reduceNum;

	}

	/**
	 * Process a tuple received from the stream, buffering by key until we hit end
	 * of stream
	 */
	@Override
	public synchronized void execute(Tuple input) {
		if (!input.isEndOfStream()) {
			if (sentEos) {
//				throw new RuntimeException("We received data after we thought the stream had ended!");
				log.error("We received data after we thought the stream had ended!");
			}

			String word = input.getStringByField("key");
			@SuppressWarnings("unchecked")
			ArrayList<String> value = (ArrayList<String>) input.getObjectByField("value");
			log.debug(getExecutorId() + " received " + word + " / " + value);
			String docID = value.get(0);

			IndexEntry wordIndexEntry;
			if (WorkerServer.getDBIndexer().containsIndex(word)) {
				// if the entry exists
				wordIndexEntry = WorkerServer.getDBIndexer().getIndexEntry(word);
			} else {
				// create a new IndexEntry
				wordIndexEntry = new IndexEntry(word);
			}
			// create docMeta
			ArrayList<String> docMeta = new ArrayList<>();
			docMeta.add(value.get(1)); // title tf
			docMeta.add(value.get(2)); // title locations
			docMeta.add(value.get(3)); // body tf
			docMeta.add(value.get(4)); // body locations
			wordIndexEntry.putID(docID, docMeta);
			try {
				WorkerServer.getDBIndexer().putIndexEntry(wordIndexEntry);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			context.incReduceOutputs();

		} else {

			// only if at EOS do we trigger the reduce operation and output all state
			numEOS++;

			if (numEOS >= neededVotesToComplete) {
				sentEos = true;
			}
			if (sentEos) {
				this.collector.emitEndOfStream();
			}

		}
		if (context.getReduceOutputs()%1000==0) {
			log.info("[reducer]"+context.getReduceOutputs());
		}

		
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
