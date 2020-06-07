package edu.upenn.cis455.mapreduce.job;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.upenn.cis455.indexer.IndexerUtil;
import edu.upenn.cis455.indexer.Lemmarize;
import edu.upenn.cis455.indexer.storage.IndexEntry;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class IndexerJob implements Job {

	@Override
	public void map(String key, String value, Context context) {
		
		/* parse doc  */
		
		Document doc = Jsoup.parse(value);

		String title = doc.title();
		String body = doc.body().text();
		
		boolean isTitleEmpty = false;
		boolean isBodyEmpty = false;
		
		/* lemmarize */
		
//        long startTime =  System.currentTimeMillis(); // 



		/* variables */

		Map<String, Integer> wordTitleCountMap = new HashMap<String, Integer>();
		Map<String, String> wordTitleLocationMap = new HashMap<String, String>();
		
		/*  word count in title */
        
		IndexerUtil.segmentAndCountWords(title, wordTitleCountMap, wordTitleLocationMap);
        
        /* variables */

		Map<String, Integer> wordBodyCountMap = new HashMap<String, Integer>();
		Map<String, String> wordBodyLocationMap = new HashMap<String, String>();
		
		// body must not be empty
		
		int maxCount = IndexerUtil.segmentAndCountWords(body, wordBodyCountMap, wordBodyLocationMap);

		// calculate TF
		Map<String, Double> wordBodyTFMap = new HashMap<String, Double>();
		Iterator it = wordBodyCountMap.keySet().iterator();

		while (it.hasNext()) {
			String word = (String) it.next();
			int count = wordBodyCountMap.get(word);
			Double tf = IndexerUtil.calculateNormalizedTF(count, maxCount);
			wordBodyTFMap.put(word, tf);
		}

		/* emit doc meta */
		
		// traverse body word first
		it = wordBodyCountMap.keySet().iterator();
		while (it.hasNext()) {
			// iterate over words in a docID
			String word = (String) it.next();
			ArrayList<String> meta = new ArrayList<>();
			meta.add(key);  // docID
			if (wordTitleCountMap.containsKey(word)) {
				// word in title
				meta.add(String.valueOf(wordTitleCountMap.get(word)));
				meta.add(wordTitleLocationMap.get(word));
			} else {
				// word not in title
				meta.add("0");
				meta.add("");
			}
			meta.add(String.format("%.3f", wordBodyTFMap.get(word)));  // body tf
			meta.add(wordBodyLocationMap.get(word));  // body locations
			
			context.write(word, meta);
		}

//        long mapTime =  System.currentTimeMillis(); //
//        System.out.println(String.format("[Map]: %.2f s",((mapTime-startTime)*1.0/1000.0)) ); // 
	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		String word;
		while (values.hasNext()) {
			word = values.next();
			context.write(word, word);
		}

	}
}
