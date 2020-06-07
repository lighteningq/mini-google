package edu.upenn.cis455.mapreduce.job;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import java.util.HashMap;
import edu.upenn.cis455.mapreduce.Context;

import edu.upenn.cis455.mapreduce.PRJob;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

public class PageRankJob implements PRJob {

	DecimalFormat df = new DecimalFormat("#.0000000000");
	long totalNodes = 4;
	long allTotalNodes = 4;
	double lambda = 0.80;
	String SINK = "SINK";
	static Map<String, Double> iter2SinkRank = new HashMap<>();
	public static String urlSpliter = ";;;";
	static String urlPrefix = "www";
	Double sinkRankShare = null;

	/**
	 * If iter = 0: read from spoutBolt: <url, outUrlsOrSink>. 
	 * 				Emit: <outUrl, 1 / N> and <url, outUrlsOrSink>
	 * If iter > 0: read from reduceBolt: <url, outUrlsOrSink-NormalizedRank>. 
	 * 				Emit: <outUrl, avgRank> or <url, outUrlsOrSink>
	 */
	// Do the mapping in the first iteration.
	@Override
	public void map(String key, String normalizedRankAndOutUrlsOrSink, Context context){
		
		// MAPPING: Not final iteration, emit to next reducer.
		
		int underscoreIdx = key.indexOf("_");
		String url = null;
		Integer iter = 1;
		if (underscoreIdx != -1) {
			iter = Integer.parseInt(key.substring(0, underscoreIdx));
			url = key.substring(underscoreIdx+1);
		}
		System.out.println("[PageRankJob_" +iter+ "]: map(), read:  { " + key + " | " + normalizedRankAndOutUrlsOrSink + "}");
	
		Double normalizedRank = null;
		if (iter == 1) {
			normalizedRank = 1.0 / totalNodes;
		} 
		String outUrlsOrSink = null;
		String[] outUrlsArr = null;
		int hyphenIdx = normalizedRankAndOutUrlsOrSink.indexOf("-");
		if (hyphenIdx != -1 && normalizedRankAndOutUrlsOrSink.startsWith(".")) {
			normalizedRank = Double.parseDouble(normalizedRankAndOutUrlsOrSink.substring(0, hyphenIdx));
			// System.out.println("[PageRankJob_" +iter+ "]: map(), normalizedRank = " + normalizedRank);
			outUrlsOrSink = normalizedRankAndOutUrlsOrSink.substring(hyphenIdx+1);
		} else {
			outUrlsOrSink = normalizedRankAndOutUrlsOrSink;
		}
		outUrlsArr = outUrlsOrSink.split(urlSpliter);
		
				
		//System.out.println("[PageRankJob_" +iter+ "]: map() : { normalizedRank = "+normalizedRank + ",outUrlsOrSink = "+ outUrlsOrSink + "}");
		
		if(!outUrlsOrSink.startsWith(SINK) && outUrlsArr != null && normalizedRank != null ) {
			//System.out.println("[PageRankJob_" +iter+ "]: map(), NOT SINK! ");
			// If Not Sink Node, emit avgRank.
			// add rank to master
			try {
				//System.out.println("[PageRankJob_" +iter+ "]: map(), addRank():  { " + normalizedRank + " | " +  iter.toString() + "}");
				addRank(normalizedRank,  iter.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// divide rank and send to outUrls
			// rule out self loop.
			
			Double avgRank = normalizedRank / outUrlsArr.length;
			String avgRankStr = df.format(avgRank);
			for(String outUrl: outUrlsArr) {
				context.write(iter+"_"+outUrl, avgRankStr);
				//System.out.println("[PageRankJob_" +iter+"]: map(), emit(), { " + iter+"_"+outUrl + " | " + avgRankStr+ " }");
			}
		} 
		
		// emit outUrls or SINK.
		context.write(iter+"_"+url, outUrlsOrSink);
		//System.out.println("[PageRankJob_" +iter+"]: map(), emit(), { " + iter+"_"+url + " | " + outUrlsOrSink + " }");
		
		
	}
	
	
	/**
	 * read from mapBolt: <url, avgRank>. 
	 *    			Emit: <url, normalizedRankStr-outUrlsOrSink>
	 */
	@Override
	public void reduce( String url, Iterator<String> values, Context context) {
		
		System.out.println("[PageRankJob]: reduce(), read:  { " + url + " |  values }");
		boolean isLastIter = false;
		
		//REDUCING1: sum up the rank.
		Double rank = new Double(0);
		String outUrlsOrSink = SINK;
				
		int uderscoreIdx = url.indexOf("_");
		Integer iter = Integer.parseInt(url.substring(0, uderscoreIdx));
		url = url.substring(uderscoreIdx + 1);
		
		while (values.hasNext()) {
			String value = values.next();
			
			if(value.startsWith(".")) {
				// value: avgRank 			
				try {
					rank += Double.parseDouble(value);
				} catch(Exception e){
					
				}
			} else {
				if (isLastIter) {
					continue;
				}
				// value :outUrlsOrSink 
				if (value.length() == 0) {
					outUrlsOrSink = SINK;
				} else {
					outUrlsOrSink = value;
				}
				// context.write(url, outUrlsOrSink);
			}
		}
		
		// Reducing step 2: Normalize Rank by adding back sinkRank share.
		double S = 0.0; // S: the total rank lost due to sink nodes in last iteration.
		try {
			S = 1 - (double) getRank(iter);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//System.out.println("[PageRankJob_" +iter+"]: reduce(), S =  " + S + ", url = " + url);
		this.sinkRankShare =  (1.0 - lambda + lambda * S) / allTotalNodes;
		Double normalizedRank = lambda * rank + this.sinkRankShare;
		String normalizedRankStr = df.format(normalizedRank);
		//System.out.println("[PageRankJob_" +iter+"]: reduce(),sinkRankShare =  " + sinkRankShare + ",  url = " + url);
		
		String nextIter = String.valueOf(iter+1);
		//System.out.println("[PageRankJob_" +iter+"]: reduce(), emit : {  " + nextIter + "_" + url + " | " + normalizedRankStr+"-"+outUrlsOrSink + "}");
		context.write(nextIter + "_" + url, normalizedRankStr+"-"+outUrlsOrSink);
	}


	/*
	 * Send sink node to the master
	 */
	@Override 
	public synchronized Double getRank(Integer iter) throws IOException {
	
	        StringBuilder sb = new StringBuilder();
	        sb.append(WorkerAdmin.masterLocation + "/getrank?");
	        sb.append("iter=" + iter);
	        URL url = new URL(sb.toString());
	
	        HttpURLConnection conn;
	        conn = (HttpURLConnection)url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.connect();
	        
	        InputStream in = conn.getInputStream();
	        String encoding = conn.getContentEncoding();
	        encoding = encoding == null ? "UTF-8" : encoding;
	        String rankSum = IOUtils.toString(in, encoding);
	        Double sum = 0.8;
	        try {
	        	sum = Double.parseDouble(rankSum);
	        	if (sum > 1 || sum == null) {
		        	sum = 0.8;
		        }
	        } catch (Exception e){
	        	sum = 0.8;
	        } 
	        //System.out.println("[PageRankJob]: getRank(), rankSum =  " + sum);
	       
	        return sum;
	}
	
	
	/*
	 * Send sink node to the master
	 */
	@Override 
	public synchronized void addRank(double rank, String iter) throws IOException {
			System.out.println("[PageRankJob]: addRank(), rank = " + rank);
	        StringBuilder sb = new StringBuilder();
	        sb.append(WorkerAdmin.masterLocation + "/addrank?");
	        sb.append("iter=" + iter);
	        sb.append("&rank=" + rank);
	        URL url = new URL(sb.toString());
	
	        HttpURLConnection conn;
	        conn = (HttpURLConnection)url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.connect();
	        conn.getResponseCode();
	        
	}


}
