
package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.AmazonS3;
import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis.aws.S3Client;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.pagerank.DBWrapper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.job.PageRankJob;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;
import edu.upenn.cis455.storage.DBCrawler;
import edu.upenn.cis455.storage.DocEntry;
import edu.upenn.cis455.storage.ExtractedURLEntry;
import sun.security.krb5.Config;

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

public class PRSpout implements IRichSpout {

	static String spliter = ":::";
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

	int workerIdx = 0;
	boolean sentEos = false;

	public AtomicInteger urlCnt = new AtomicInteger(0);
	public AtomicInteger outUrlCnt = new AtomicInteger(0);

	public void createDir(String dir) {
		if (!Files.exists(Paths.get(dir))) {
			try {
				Files.createDirectories(Paths.get(dir));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * This is a DB
	 */
	DBCrawler db;
	String dBPath;
	EntityCursor<ExtractedURLEntry> cursor;
	boolean createCursor = false;
	TopologyContext context;
	boolean sentEof = false;

	public static DBWrapper urlIDSet;

	public PRSpout() {
		filename = getFilename();
	}

	public String getFilename() {
		return "S3 DB";
	}

	/**
	 * Initializes the instance of the spout (note that there can be multiple
	 * objects instantiated)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.context = context;

		if (conf.containsKey("inputDir")) {
			this.inputDir = WorkerAdmin.workerStorage + "/" + conf.get("inputDir");

		} else {
			this.inputDir = WorkerAdmin.workerStorage;
		}
		
		System.out.println("[PRSpout]:inputDir =  " + inputDir);
		PRSpout.urlIDSet = new DBWrapper(WorkerAdmin.workerStorage + "/" + WorkerAdmin.urlIDSet);
		System.out.println("[PRSpout]: New ");
	//	System.out.println("[PRSpout]: UrlIDSet = " + urlIDSet.urlIdKeySet());
		// System.out.println("[PRSpout]: UrlIDSet size = " + urlIDSet.urlIdSize());
		try {
			// For default, read all files under "inputDir"
			// System.out.println("[Spout]: Starting spout from diretory: " +
			// this.inputDir);

			File directory = new File(this.inputDir);

			for (File f : directory.listFiles()) {
				if (f.isFile()) {
					BufferedReader reader = new BufferedReader(new FileReader(f));
					this.readersMap.put(f.getName(), reader);
				}
			}
			this.itr = this.readersMap.entrySet().iterator();
			if (itr.hasNext()) {
				Map.Entry<String, BufferedReader> entry = itr.next();
				this.currFileName = entry.getKey();
				this.currReader = entry.getValue();
			}
			// System.out.println("[PageRankSpout]: how many files: " + readersMap.size());

		} catch (FileNotFoundException e) {
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

		if (this.currReader != null && !sentEos) {
			try {
				String urlId = this.currReader.readLine();

				if (urlId == null && this.itr.hasNext()) {
					Map.Entry<String, BufferedReader> entry = this.itr.next();
					this.currFileName = entry.getKey();
					this.currReader = entry.getValue();
					urlId = currReader.readLine();
				} else if (urlId == null && !this.itr.hasNext() && !sentEos) {
					this.collector.emitEndOfStream(getExecutorId());
					this.sentEos = true;
				}

				if (urlId != null) {

					List<String> outUrlsList = S3Client.getExtractedIds(urlId);
					//Set<String> outUrlsSet = new HashSet<String>(outUrlsList);
					StringBuilder sb = new StringBuilder();
					System.out.println("[PRSpout] :outUrlsList size " + outUrlsList.size());
					int i = 0;
					for (String ourUrlId : outUrlsList) {
						if (i >  10){
							continue;	
						}
						if (/*PRSpout.urlIDSet.containsUrlId(ourUrlId) && */ourUrlId != urlId) {
							
								 //System.out.println("[PRSpout] AA !!!!!!!!!!!!!!!!!! processed !!!!!!!!!!!!!!!!! " + urlId
									//	+ " outUrlsSet size " + outUrlsSet.size());
									sb.append(ourUrlId).append(PageRankJob.urlSpliter);
									PRSpout.urlIDSet.urlIdInc(urlId);

						} else {
							// System.out.println("[PRSpout] =============  skip ================ " + " outUrlsSet size"
									// + outUrlsSet.size());
						}
					}
					
					String outUrls = null;
					
					if (sb.length() > 3) {
						outUrls = sb.substring(0, sb.length() - PageRankJob.urlSpliter.length());
						System.out.println("[PRSpout] AAE !!!!!!!!!!!!!!!!!! emit !!!!!!!!!!!!!!!!! urlId = " + urlId + " outUrls = " + outUrls);
						this.collector.emit(new Values<Object>("1_" + urlId, outUrls));
						WorkerAdmin.getInstance().updateKeysRead();
					} else  {
						outUrls = "SINK";
						this.collector.emit(new Values<Object>("1_" + urlId, outUrls));
						WorkerAdmin.getInstance().updateKeysRead();
					} 

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

	public String removeSelfLoop(String url, String outUrls) {

		outUrls = outUrls.replace(url, "");
		String[] splits = outUrls.split(PageRankJob.urlSpliter, -1);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < splits.length; i++) {
			if (splits[i].length() > 0) {
				outUrlCnt.addAndGet(1);
				sb.append(splits[i]).append(PageRankJob.urlSpliter);
			}
		}
		String res = sb.substring(0, sb.length() - 3);
		if (res.startsWith(PageRankJob.urlSpliter)) {
			res = res.substring(3);
		}
		return res;
	}
}
