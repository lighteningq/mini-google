package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

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
	static Logger log = Logger.getLogger(FileSpout.class);

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * FileSpout, aka each "executor"
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
	ArrayList<BufferedReader> readers = new ArrayList<>();
	int readerIdx = 0;

	int inx = 0;
	boolean sentEof = false;

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

		try {
			log.debug("Starting spout for " + filename);
			log.debug(getExecutorId() + " opening file reader");

			// TODO: Add logic to get a buffered reader for all files in the directory
			String inputPath = conf.get("storageDir").toString() + conf.get("inputDir").toString();
			System.out.println("inputDir: " + inputPath);

			File file = new File(inputPath);
			File[] files = file.listFiles();
			if (files == null) {
				throw new FileNotFoundException();
			} else {
				for (int i = 0; i < files.length; i++) {
					readers.add(new BufferedReader(new FileReader(files[i].getAbsolutePath())));
				}
			}
			// TODO: The line below is meant for single file only - modify as needed
//    		reader = new BufferedReader(new FileReader(filename + "." + conf.get("workerIndex")));

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
		// TODO: Change logic to close bufferedReaders for all open files
		// TODO: The logic below closes only a single file - modify as needed
		for (int i = 0; i < readers.size(); i++) {
			if (readers.get(i) != null) {
				try {
					readers.get(i).close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * The real work happens here, in incremental fashion. We process and output the
	 * next item(s). They get fed to the collector, which routes them to targets
	 */
	@Override
	public synchronized void nextTuple() {
		// TODO: Logic below is meant to read from a single file only - modify this to
		// read from multiple files
		if (!sentEof) {
			BufferedReader reader = readers.get(readerIdx);
			if (reader != null) {
				try {
					String line = reader.readLine();
					if (line != null) {
						// emit
						System.out.println(getExecutorId() + " emitting " + line); //// debug
						log.debug(getExecutorId() + " emitting " + line);
						this.collector.emit(new Values<Object>(String.valueOf(inx++), line));
					} else if (!sentEof) {
						log.info(getExecutorId() + " finished file " + getFilename() + " file number " + readerIdx);
						readerIdx ++;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				// reader is null
				readerIdx ++;
			}
			// finished reading
			if (readerIdx>=readers.size()) {
				log.info(getExecutorId() + " finished reading all files.");
				this.collector.emitEndOfStream();
				sentEof = true;
			}
		}

		Thread.yield();
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
