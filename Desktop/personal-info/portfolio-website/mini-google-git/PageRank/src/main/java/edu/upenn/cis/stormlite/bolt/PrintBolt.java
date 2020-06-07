package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;


import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.EOSChecker;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {

	
	Fields myFields = new Fields();

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    String outputFilePath;
	@Override
	public void cleanup() {
		// Do nothing

	}

	@Override
	public boolean execute(Tuple input) {
		if (!input.isEndOfStream()) {
			String rawKvpair = input.getValues().toString();
	        String kvpair = rawKvpair.substring(1,rawKvpair.length()-1) + "\n";
	        System.out.println("[PrintBolt]: " + kvpair);
	        
	        // write result to output.txt
	        BufferedWriter writer = null;
			try {
				writer = new BufferedWriter(new FileWriter(outputFilePath, true));
				writer.append(kvpair);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
		return true;
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		/*
		 * create output.txt if not exist.
		 */
		String outputDir =  WorkerAdmin.workerStorage + "/" + stormConf.get("outputDir") + "/";
		File dir = new File(outputDir);
		dir.mkdir();

		this.outputFilePath = WorkerAdmin.workerStorage + "/" + stormConf.get("outputDir") + "/" + WorkerAdmin.outputFileName;
		try {
			File outputFile = new File(outputFilePath);
			// System.out.println("[PrintBolt]: create file : " + this.outputFilePath + ", id = " + getExecutorId());
			outputFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// System.out.println("[PrintBolt]: create file : " + this.outputFilePath);
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
