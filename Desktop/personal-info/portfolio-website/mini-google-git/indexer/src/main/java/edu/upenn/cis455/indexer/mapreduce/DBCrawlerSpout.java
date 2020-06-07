package edu.upenn.cis455.indexer.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.indexer.storage.IndexEntry;
import edu.upenn.cis455.storage.DBCrawler;
import edu.upenn.cis455.storage.DocEntry;

public class DBCrawlerSpout implements IRichSpout {
	static Logger log = Logger.getLogger(FileSpout.class);

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * FileSpout, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	/**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	private SpoutOutputCollector collector;
	
	private TopologyContext context;
	/**
	 * This is a DB
	 */
	DBCrawler db;
	String dBPath;
//	EntityCursor<DocEntry> cursor;
//	boolean createCursor = false;
	
	BufferedReader br;
	
	boolean sentEos = false;
	String filename;
	
	
	

	public DBCrawlerSpout() {
		filename = getFilename();
	}

	public String getFilename() {
		return "Crawler DB";
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
		try {
			log.debug("Starting spout for " + filename);
			log.debug(getExecutorId() + " opening BerkeleyDB");

			String inputPath = conf.get("storageDir").toString() + conf.get("inputDir").toString();
			log.info("inputDir: " + inputPath);

			// open DB
//			this.db = new DBCrawler(inputPath);
			this.db = new DBCrawler(inputPath);
			EntityCursor<DocEntry> cursor = DBCrawler.getDocEntryCursor();
			String outputDir = conf.get("storageDir").toString()+conf.get("outputDir");
			File dir = new File(outputDir);
			if (!dir.exists()) dir.mkdirs();  // create output dir if not exists
			System.out.println("Output Directory: "+outputDir);
			
			File file =new File(outputDir+"/urlid_temp.txt");
			
	        if(!file.exists()){
	        	file.createNewFile();
	        } else {
	        	file.delete();
	        	file.createNewFile();
	        }
	        FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
	        BufferedWriter bw = new BufferedWriter(fileWriter);
	        for(DocEntry docEntry: cursor) {
	        	String urlid = docEntry.getId();
	        	bw.write(urlid+"\r\n");
	        }
	        bw.flush();
	        bw.close();
	        fileWriter.close();
	        
	        this.br = new BufferedReader(new FileReader(file));
	        

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Shut down the spout
	 */
	@Override
	public void close() {
		if (this.db!=null) {
//			cursor.close();
			DBCrawler.shutdownDB();
		}
		if (br!=null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * The real work happens here, in incremental fashion. We process and output the
	 * next item(s). They get fed to the collector, which routes them to targets
	 */
	@Override
	public synchronized void nextTuple() {
//		if (!createCursor) {
//			this.cursor = DBCrawler.getDocEntryCursor();
//			createCursor = true;
//		}
		if (!sentEos) {
			String urlId;
			DocEntry docEntry = null;
			try {
				urlId = br.readLine();
				if (urlId==null) {
					// read over
					this.collector.emitEndOfStream();
					sentEos = true;
					log.info("emitted EOS");
				}
				else {
					// send doc
					docEntry = db.getDoc(urlId.trim());
					log.info("emitting " + docEntry.getId());
					this.collector.emit(new Values<Object>(docEntry.getId(), new String(docEntry.getContent())));
					context.incSpoutOutput();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
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
