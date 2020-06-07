package edu.upenn.cis455.indexer.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collector;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Context;

/**
 * A trivial bolt that simply outputs its input stream to the console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);

	Fields myFields = new Fields();

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * PrintBolt, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();
	
	String outputDir;
	
	int neededVotesToComplete;
	
	int numEOS;
	
	File file;
	FileWriter fileWriter;
	BufferedWriter bw;
	
	TopologyContext context;
	
	@Override
	public void cleanup() {
		// Do nothing

	}

	@Override
	public void execute(Tuple input) {

		// TODO: Write to an output file in output directory until all EOS received
		
        
        if (numEOS<neededVotesToComplete) {
			if (!input.isEndOfStream()) {
				String result = input.getStringByField("key");  // word
				try {
					bw.write(result+"\r\n");
					bw.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				numEOS ++;
//				log.info("PrintBolt received a EOS. now:"+numEOS+". Expected:"+neededVotesToComplete);  //// debug
			}
        }
        if (numEOS >= neededVotesToComplete) {
        	try {
        		log.info("Print done!");
	        	bw.close();
	        	context.setState(TopologyContext.STATE.INIT);
	        	
			} catch (IOException e) {
				e.printStackTrace();
			}
        }

	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// /storage/ + output
		this.outputDir = stormConf.get("storageDir")+stormConf.get("outputDir");
		// EOS = reduceBolts * workers
		this.neededVotesToComplete = Integer.parseInt(stormConf.get("reduceExecutors")) * WorkerHelper.getWorkers(stormConf).length;
		this.context = context;
		try {
			File dir = new File(outputDir);
			if (!dir.exists()) dir.mkdirs();  // create output dir if not exists
			System.out.println("Output Directory: "+outputDir);
			file =new File(outputDir+"/output.txt");
			 
	        if(!file.exists()){
	        	file.createNewFile();
	        }
	        fileWriter = new FileWriter(file.getAbsoluteFile());
	        bw = new BufferedWriter(fileWriter);
	        
		} catch (Exception e) {
			e.printStackTrace();
		}
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
