package edu.upenn.cis455.indexer.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import javax.management.RuntimeErrorException;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.storage.DBCrawler;
import edu.upenn.cis455.storage.DocEntry;
import edu.upenn.cis455.storage.UploaderS3;

public class S3Spout implements IRichSpout {
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
//	DBCrawler db;
//	String dBPath;
//	EntityCursor<DocEntry> cursor;
//	boolean createCursor = false;

	BufferedReader br;
	// Amazon S3
	AmazonS3 s3client;

	boolean sentEos = false;
	String filename;

	public S3Spout() {
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
		try {
			log.debug("Starting spout for " + filename);
			log.debug(getExecutorId() + " opening S3 DB");

			String inputPath = conf.get("storageDir").toString() + conf.get("inputDir").toString();
			log.info("inputDir: " + inputPath);

			// connect S3
			this.s3client = UploaderS3.getS3();

			// output dir
			String outputDir = conf.get("storageDir").toString() + conf.get("outputDir");
			File dir = new File(outputDir);
			if (!dir.exists())
				dir.mkdirs(); // create output dir if not exists
			System.out.println("Output Directory: " + outputDir);

			// input dir
			String filepath = inputPath + "/urlid_0.txt";
			File file = new File(filepath);
			if (!file.exists()) {
				throw new IllegalArgumentException("Missing input data:"+filepath);
			}

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
//		if (this.db != null) {
////			cursor.close();
//			DBCrawler.shutdownDB();
//		}
		if (br != null) {
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
				if (urlId == null) {
					// read over
					this.collector.emitEndOfStream();
					sentEos = true;
					log.info("emitted EOS");
				} else {
					// send doc
					String urlContent = UploaderS3.extractContentWithS3(urlId, s3client);
					log.info("emitting " + urlId);
					this.collector.emit(new Values<Object>(urlId, new String(urlContent)));
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
