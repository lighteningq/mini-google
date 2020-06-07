package edu.upenn.cis455.indexer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.AbstractDocument.BranchElement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis455.indexer.storage.IndexEntry;
import edu.upenn.cis455.indexer.storage.DBIndexer;
import edu.upenn.cis455.mapreduce.DBWrapper;
import edu.upenn.cis455.storage.DBCrawler;
import edu.upenn.cis455.storage.DocEntry;
import edu.upenn.cis455.storage.UploaderS3;

public class IndexerTest {
	
	public static long lastTime = System.currentTimeMillis();
	
	public static void main(String[] args) {
		
//		retrieveCrawlerDoc();
//		indexer();
//		retrieveIndex();
//		wordSegmentation();
//		metaExtract();
//		executorTest();
//		readTest();
//		getWorkers();
//		getWordWorker("yanda", 5, 50);  //zulu zuma ynet yang yandy yanda
//		indexerFinal();
		seeUrlContent();
	}
	
	public static void seeUrlContent() {
		String content;
		try {
			content = UploaderS3.extractContent("1d9ffb59d3c27d55afc219e9871cd04f");
			System.out.println(content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static int getWordWorker(String word, 
			int workerNum, int executorNum) {
		int ret=0;
		
		ret ^= word.hashCode();
		int size = workerNum*executorNum;
		ret = ret % size;
		if (ret < 0)
			ret = ret + size;
		
		ret = ret/executorNum;
		System.out.println(ret);
		return ret;
	}
	
	
	public static void getWorkers() {
		String list = "[127.0.0.1:8001,127.0.0.1:8002]";
		if (list.startsWith("["))
			list = list.substring(1);
		if (list.endsWith("]"))
			list = list.substring(0, list.length() - 1);
		
		String[] servers = list.split(",");
		
		String[] ret = new String[servers.length];
		int i = 0;
		for (String item: servers) {
			if (!item.startsWith("http"))
				ret[i++] = "http://" + item;
			else
				ret[i++] = item;
		}
		for (String retString:ret) {
			System.out.println(retString);
		}
		
	}
	
	public static void readTest() {
		File file = new File("/home/cis455/project_crawled/worker_1/output/urlid_temp.txt");
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while (true) {
				String line = br.readLine().trim();
				System.out.println(line);
				if (line==null) {
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void executorTest() {
		ExecutorService executor = Executors.newFixedThreadPool(3);
		
		for (int i=0;i<10;i++) {
			System.out.println("Round "+i);
			Runnable task = new RunnableTest(i);
			executor.execute(task);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void wordSegmentation() {
//		String pattern = "[.,@&-\\s\\?]+[a-zA-Z]+[.,@&-\\s\\?]+";
		String pattern = "\\b[a-zA-Z]+\\b";
		String text = "I want to find a Home. Numbers 12345 &some stuひpid sh*t. I've done it. You'll get it."
				+ " zk@upenn.edu multi-thread camelCaseStyle "
				+ "java_function_style https://a3.espncdn.com/combiner/i?img=%2Fi%2Fespn%2Fmisc_logos%2F500%2Fwwe.png";
		Pattern p = Pattern.compile(pattern);
		Pattern pan2 = Pattern.compile("[a-zA-Z]+");
		Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");
		Pattern pan = Pattern.compile("^[a-zA-Z0-9]+[.@&-]*[a-zA-Z0-9]+");
		Matcher matcher = pan2.matcher(text);
		while (matcher.find()) {
			System.out.println(matcher.group(0));
		}
		
	}
	
	
	public static void metaExtract() {
		String docID = "cccd9ac1684b6ca1a898e3472342467c";
		String DBCrawlerPath = "/home/cis455/project_crawled/worker_1/crawler";
		DBCrawler dbCrawler = new DBCrawler(DBCrawlerPath);
		
//		DocEntry docEntry = dbCrawler.getDoc(docID);
//		String contentString = new String(docEntry.getContent());
//		System.out.println(docEntry.getUrl());
		checkTime("Load DB");
		
		// parse body

		EntityCursor<DocEntry> cursor = DBCrawler.getDocEntryCursor();
		for(DocEntry docEntry: cursor) {
			String contentString = new String(docEntry.getContent());
			Document doc = Jsoup.parse(contentString);

			String title = doc.title();
			String body = doc.body().text();

//			System.out.println("After parsing, Title : " + title);
//			System.out.println("Afte parsing, Body : " + body);
			checkTime("Extracting body");
			
			// put to file
//			writeFile("/home/cis455/project_crawled/worker_1/htmlbody.txt", body);
//			checkTime("Write file");
			Map<String, String> metas = new HashMap<>();
			Elements metaTags = doc.getElementsByTag("meta");

			for (Element metaTag : metaTags) {
			  String content = metaTag.attr("content");
			  String name = metaTag.attr("name");
			  System.out.println("["+name+"]"+content);
			}
			
			String head = doc.head().text();
			System.out.println("[head]"+head);
			System.out.println("[title]"+title+"\n\n");
		}
		
		
		
		

		
	}
	
	public static void retrieveCrawlerDoc() {
		String DBCrawlerPath = "/home/cis455/project_crawled/worker_1/crawler";
		DBCrawler dbCrawler = new DBCrawler(DBCrawlerPath);
		DBCrawler.retrieveURL();
	}
	
	public static void retrieveIndex() {
		String DBIndexerPath = "/home/cis455/worker_3/DBIndexer";
		DBIndexer dbIndexer = new DBIndexer(DBIndexerPath);
		dbIndexer.retrieveIndexInfo();
	}
	
	public static void indexerFinal() {
		
		String docID = "cccd9ac1684b6ca1a898e3472342467c";
		String DBCrawlerPath = "/home/cis455/project_crawled/worker_1/crawler";
		DBCrawler dbCrawler = new DBCrawler(DBCrawlerPath);
		DocEntry docEntry = dbCrawler.getDoc(docID);
		/* parse doc  */
		
		Document doc = Jsoup.parse(new String(docEntry.getContent()));

		String title = doc.title();
		String body = doc.body().text();
		
        long startTime =  System.currentTimeMillis(); // 

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
		String maxWord = null;
		while (it.hasNext()) {
			// iterate over words in a docID
			String word = (String) it.next();
			ArrayList<String> meta = new ArrayList<>();
			meta.add(docID);  // docID
			if (wordTitleCountMap.containsKey(word)) {
				// word in title
				meta.add(String.valueOf(wordTitleCountMap.get(word)));
				meta.add(wordTitleLocationMap.get(word));
			} else {
				// word not in title
				meta.add("0");
				meta.add("");
			}
			if (wordBodyTFMap.get(word) == 1.0) maxWord = word;
			meta.add(String.format("%.3f", wordBodyTFMap.get(word)));  // body tf
			meta.add(wordBodyLocationMap.get(word));  // body locations
			
			for(String metaString:meta) {
				System.out.println(metaString);
			}
		}
		System.out.println(maxWord);
	}
	
	public static void indexer() {

		String directory = "/home/cis455/project_store_test/DBCrawler/crawler_000";
		DBCrawler db = new DBCrawler(directory);
		DBCrawler.retrieveURL();
		
		String DBIndexerPath = "/home/cis455/project_store_test/DBIndexer";
		DBIndexer.clearDB(DBIndexerPath);
		DBIndexer dbIndexer = new DBIndexer(DBIndexerPath);
		
		EntityCursor<DocEntry> cursor = DBCrawler.getDocEntryCursor();
		for(DocEntry docEntry: cursor) {
			
			byte[] contentBytes = docEntry.getContent();
			String docID = docEntry.getId();
			if (contentBytes!=null) {
				String contentString = new String(contentBytes);
//				System.out.println(contentString);
				
				Document doc = Jsoup.parse(contentString);

				String title = doc.title();
				String body = doc.body().text();

				System.out.println("After parsing, Title : " + title);
				System.out.println("Afte parsing, Body : " + body);

				title = Lemmarize.removeStopWords(title);
				body = Lemmarize.removeStopWords(body);

				System.out.println("After lemmarization, Title : " + title);
				System.out.println("Afte lemmarization, Body : " + body);

				String[] titleStrings = title.split(" ");
				String[] bodyStrings = body.split(" ");
				
				// html
				int wordCount = 0; // location

				/* variables */

				Map<String, Integer> wordCountMap = new HashMap<String, Integer>();
				Map<String, String> wordLocationMap = new HashMap<String, String>();
				int maxCount = 0;
				

				for (String word : titleStrings) {
					if (word == "")
						continue;
					int thisWordCount;
					if (wordCountMap.containsKey(word)) {
						// count
						int lastCount = wordCountMap.get(word);
						thisWordCount = lastCount + 1;
						wordCountMap.replace(word, thisWordCount);
						// location
						wordLocationMap.replace(word, wordLocationMap.get(word) + " " + (String.valueOf(wordCount)));
					} else {
						// count
						thisWordCount = 1;
						wordCountMap.put(word, thisWordCount);
						// location
						wordLocationMap.put(word, String.valueOf(wordCount));
					}
					maxCount = (thisWordCount > maxCount ? thisWordCount : maxCount);
					wordCount++;
				}
				wordCount++;
				for (String word : bodyStrings) {
					if (word == "")
						continue;
					int thisWordCount;
					if (wordCountMap.containsKey(word)) {
						// count
						int lastCount = wordCountMap.get(word);
						thisWordCount = lastCount + 1;
						wordCountMap.replace(word, thisWordCount);
						// location
						wordLocationMap.replace(word, wordLocationMap.get(word) + " " + (String.valueOf(wordCount)));
					} else {
						// count
						thisWordCount = 1;
						wordCountMap.put(word, thisWordCount);
						// location
						wordLocationMap.put(word, String.valueOf(wordCount));
					}
					maxCount = (thisWordCount > maxCount ? thisWordCount : maxCount);
					wordCount++;
				}

				// calculate TF
				Map<String, Double> wordTFMap = new HashMap<String, Double>();
				Iterator it = wordCountMap.keySet().iterator();

				while (it.hasNext()) {
					String word = (String) it.next();
					int count = wordCountMap.get(word);
					Double tf = calculateTF(count, maxCount);
					wordTFMap.put(word, tf);
				}

				// put in DB
				it = wordCountMap.keySet().iterator();
				while (it.hasNext()) {
					// iterate over words in a docID
					String word = (String) it.next();
					IndexEntry wordIndexEntry;
					if (dbIndexer.containsIndex(word)) {
						// if the entry exists
						wordIndexEntry = dbIndexer.getIndexEntry(word);
					} else {
						// create a new IndexEntry
						wordIndexEntry = new IndexEntry(word);
					}
					// create docMeta
					ArrayList<String> docMeta = new ArrayList<>();
					docMeta.add(String.format("%.3f", wordTFMap.get(word)));  // tf
					docMeta.add(wordLocationMap.get(word));  // locations
					wordIndexEntry.putID(docID, docMeta);
					try {
						dbIndexer.putIndexEntry(wordIndexEntry);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
				}
				
				
			}
			
		}
		
		
		cursor.close();
		DBCrawler.shutdownDB();
		dbIndexer.shutdownDB();
	}
	
	
	///////////////////////////////////////////////////////
	
	public static void main2(String[] args) {

		String DBPath = "/home/cis455/project_store_test";
		DBIndexer.clearDB(DBPath);
		
		DBIndexer db = new DBIndexer(DBPath);

//		// create index
//		IndexEntry indexEntry1 = new IndexEntry("word");
//		String[] meta1 = { "0", "1", "2" };
//		indexEntry1.putID("docID1", meta1);
//
//		for (int i = 0; i <= 2; i++)
//			System.out.println(indexEntry1.getMeta().get("docID1")[i]);
//		
//		// put into DB
//		try {
//			db.putIndexEntry(indexEntry1);
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		}
//		
//		// extract from DB
//		try {
//			IndexEntry entry = db.getIndexEntry("word");
//			for (int i = 0; i <= 2; i++)
//				System.out.println(indexEntry1.getMeta().get("docID1")[i]);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

		String crawledDBPath = "/home/cis455/project_crawled";
		DBWrapper crawledDB = new DBWrapper();
		crawledDB.setEnvDir(crawledDBPath);
		crawledDB.initiate();

		Cursor cursor = crawledDB.getCursor();
		DatabaseEntry theKey = new DatabaseEntry();
		DatabaseEntry theData = new DatabaseEntry();

		/** for each document **/

		while (cursor.getNext(theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			String key = new String(theKey.getData());
			
			if (key.endsWith("%_addtime") || key.endsWith(".xml"))
				continue;
			String value = new String(theData.getData());
			System.out.println(key);
			System.out.println(value);

			// html
			int wordCount = 0; // location

			/* variables */

			Map<String, Integer> wordCountMap = new HashMap<String, Integer>();
			Map<String, String> wordLocationMap = new HashMap<String, String>();
			int maxCount = 0;

			Document doc = Jsoup.parse(value);

			String title = doc.title();
			String body = doc.body().text();

			System.out.println("After parsing, Title : " + title);
			System.out.println("Afte parsing, Body : " + body);

			title = Lemmarize.removeStopWords(title);
			body = Lemmarize.removeStopWords(body);

			System.out.println("After lemmarization, Title : " + title);
			System.out.println("Afte lemmarization, Body : " + body);

			String[] titleStrings = title.split(" ");
			String[] bodyStrings = body.split(" ");

			for (String word : titleStrings) {
				if (word == "")
					continue;
				int thisWordCount;
				if (wordCountMap.containsKey(word)) {
					// count
					int lastCount = wordCountMap.get(word);
					thisWordCount = lastCount + 1;
					wordCountMap.replace(word, thisWordCount);
					// location
					wordLocationMap.replace(word, wordLocationMap.get(word) + " " + (String.valueOf(wordCount)));
				} else {
					// count
					thisWordCount = 1;
					wordCountMap.put(word, thisWordCount);
					// location
					wordLocationMap.put(word, String.valueOf(wordCount));
				}
				maxCount = (thisWordCount > maxCount ? thisWordCount : maxCount);
				wordCount++;
			}
			wordCount++;
			for (String word : bodyStrings) {
				if (word == "")
					continue;
				int thisWordCount;
				if (wordCountMap.containsKey(word)) {
					// count
					int lastCount = wordCountMap.get(word);
					thisWordCount = lastCount + 1;
					wordCountMap.replace(word, thisWordCount);
					// location
					wordLocationMap.replace(word, wordLocationMap.get(word) + " " + (String.valueOf(wordCount)));
				} else {
					// count
					thisWordCount = 1;
					wordCountMap.put(word, thisWordCount);
					// location
					wordLocationMap.put(word, String.valueOf(wordCount));
				}
				maxCount = (thisWordCount > maxCount ? thisWordCount : maxCount);
				wordCount++;
			}

			// calculate TF
			Map<String, Double> wordTFMap = new HashMap<String, Double>();
			Iterator it = wordCountMap.keySet().iterator();

			while (it.hasNext()) {
				String word = (String) it.next();
				int count = wordCountMap.get(word);
				Double tf = calculateTF(count, maxCount);
				wordTFMap.put(word, tf);
			}

			// put in DB
			it = wordCountMap.keySet().iterator();
			while (it.hasNext()) {
				String word = (String) it.next();
				IndexEntry wordIndexEntry;
				if (db.containsIndex(word)) {
					// if the entry exists
					wordIndexEntry = db.getIndexEntry(word);
				} else {
					// create a new IndexEntry
					wordIndexEntry = new IndexEntry(word);
				}

//				String[] meta = {String.format("%.3f", wordTFMap.get(word)), wordLocationMap.get(word)};
//				wordIndexEntry.putID(key, meta);
				try {
					db.putIndexEntry(wordIndexEntry);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}

//			break;
		}

		cursor.close();

		crawledDB.close();
		db.shutdownDB();
	}

	/**
	 * time checker
	 * @param info
	 */
	public static void checkTime(String info) {
		long currentTime = System.currentTimeMillis();
		System.out.println(String.format("[Time Checker] "+info+" time: %.2f s",((currentTime-lastTime)*1.0/1000.0)) );
		lastTime = currentTime;
	}
	
	public static void writeFile(String filepath, String body) {
		File file =new File(filepath);
        if(!file.exists()){
        	try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(file.getAbsoluteFile());
	        BufferedWriter bw = new BufferedWriter(fileWriter);
	        bw.write(body);
	        bw.flush();
	        bw.close();
	        fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * helper function to calculate the TF value of a certain word.
	 * 
	 * @param count
	 * @param maxCount
	 * @return
	 */
	public static Double calculateTF(int count, int maxCount) {
		Double ret = TF_PARAM + (1.0 - TF_PARAM) * Double.valueOf(count) / Double.valueOf(maxCount);
		return ret;
	}

	public static final Double TF_PARAM = 0.5;

	public static final Set<String> STOPWORDS = Collections.unmodifiableSet(new HashSet<String>() {
		{
			add("'d");
			add("'ll");
			add("'re");
			add("'s");
			add("'t");
			add("'ve");
			add("n't");
			add("a");
			add("about");
			add("above");
			add("after");
			add("again");
			add("against");
			add("all");
			add("am");
			add("an");
			add("and");
			add("any");
			add("are");
			add("as");
			add("at");
			add("be");
			add("because");
			add("been");
			add("before");
			add("being");
			add("below");
			add("between");
			add("both");
			add("but");
			add("by");
			add("cannot");
			add("could");
			add("did");
			add("do");
			add("does");
			add("doing");
			add("down");
			add("during");
			add("each");
			add("few");
			add("for");
			add("from");
			add("further");
			add("had");
			add("has");
			add("have");
			add("having");
			add("he");
			add("her");
			add("here");
			add("hers");
			add("herself");
			add("him");
			add("himself");
			add("his");
			add("how");
			add("i");
			add("if");
			add("in");
			add("into");
			add("is");
			add("it");
			add("its");
			add("itself");
			add("me");
			add("more");
			add("most");
			add("my");
			add("myself");
			add("no");
			add("nor");
			add("not");
			add("of");
			add("off");
			add("on");
			add("once");
			add("only");
			add("or");
			add("other");
			add("ought");
			add("our");
			add("ours");
			add("ourselves");
			add("out");
			add("over");
			add("own");
			add("same");
			add("she");
			add("should");
			add("so");
			add("some");
			add("such");
			add("than");
			add("their");
			add("theirs");
			add("them");
			add("themselves");
			add("the");
			add("then");
			add("there");
			add("these");
			add("they");
			add("this");
			add("those");
			add("through");
			add("to");
			add("too");
			add("under");
			add("until");
			add("up");
			add("very");
			add("was");
			add("we");
			add("were");
			add("what");
			add("when");
			add("where");
			add("which");
			add("while");
			add("who");
			add("whom");
			add("why");
			add("with");
			add("would");
			add("you");
			add("your");
			add("yours");
			add("yourself");
			add("yourselves");

			add("de");
			add("del");
			add("di");
			add("y");

			add("corporation");
			add("corp");
			add("corp.");
			add("co");
			add("llc");
			add("inc");
			add("inc.");
			add("ltd");
			add("ltd.");
			add("llp");
			add("llp.");
			add("plc");
			add("plc.");

			add("&");
			add(",");
			add("-");
		}
	});



}
class RunnableTest implements Runnable {
	private int num;
	public RunnableTest(int num) {
		this.num = num;
	}
	@Override
	public void run() {
		System.out.println(num+" begin");
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(num+" completed.");
	}
}

