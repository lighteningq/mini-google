package edu.upenn.cis455.storage;

import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.StoreConfig;

import edu.upenn.cis455.crawler.CrawlerUtil;
//import edu.upenn.cis455.crawler.XPathCrawler_Modified;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.xpathengine.XPathEngineImpl;


/** (MS1, MS2) A wrapper class which should include:
  * - Set up of Berkeley DB
  * - Saving and retrieving objects including crawled docs and user information
  */
/**
 * @author cis455
 *
 */
public class DBWrapper {
	static Logger log = Logger.getLogger(DBWrapper.class);
	private static String envDirectory = null;
	
	private static Environment myEnv;
	
	private static EntityStore store;
	
	private static DBAccessor da;
	private static boolean isSetUp = false;
	
	
	
	
	/*DB Enviorment SetUp  */
	
	public DBWrapper(String path) {
		//if(!isSetUp) {
			DBWrapper.envDirectory = path;
			setup(envDirectory);
			da = new DBAccessor(store);
			//isSetUp = true;
		//}
	}

	
	
	/**
	 * setup database
	 * */
	public void setup(String path) {
		long start = System.currentTimeMillis();
		File data = new File(path);
		
		if(!data.exists()) {
			data.mkdirs();
			data.setReadable(true);
			data.setWritable(true);
		}
		
		try {
			// Create EnvironmentConfig object
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(true);
			// Create a database environment if it doesn’t already exist 

			
	        StoreConfig storeConfig = new StoreConfig();
	        storeConfig.setAllowCreate(true);
	        storeConfig.setTransactional(true);
	        // Instantiate environment
			DBWrapper.myEnv = new Environment(data, envConfig);
			store = new EntityStore(myEnv, "EntityStore",storeConfig);
			
	} catch (DatabaseException dbe) {
		System.out.println("Database Error: "+dbe.toString());
		System.out.println(dbe.getStackTrace());
		System.exit(-1);
	}
		long end = System.currentTimeMillis();
		System.out.println("bdb set up total "+(end-start)+"ms");

	}
	
	
	/**
	 * 
	 * Shutdown DB
	 * 1. shutdown store
	 * 2. shutdown env
	 * */
	
	public void shutdownDB() {
		if(store!=null) {
			try {
				store.close();
			}catch(DatabaseException dbe) {
				System.err.println("Error closing store: " + dbe.toString());
				System.exit(-1);
			}
		}
		
		if (myEnv != null) {
			try {

				myEnv.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing myEnv: " + dbe.toString());
				System.exit(-1);
			}
		}
		
		
	}
	
	public static String getEnvDirectory() {
		return envDirectory;
	}


	public static void setEnvDirectory(String envDirectory) {
		DBWrapper.envDirectory = envDirectory;
	}


	public static Environment getMyEnv() {
		return myEnv;
	}


	public static void setMyEnv(Environment myEnv) {
		DBWrapper.myEnv = myEnv;
	}


	public static EntityStore getStore() {
		return store;
	}


	public static void setStore(EntityStore store) {
		DBWrapper.store = store;
	}
	
	
	
	


	
	

	
	

	
/******************************************
 * ****************************************
 * 
 * Functions for Document Entry
 * 
 * ****************************************
 * *************************************/
	

	/** Put document into db, update entry if neccssary
	 * @param url
	 * @param content
	 * @param type
	 * @return true upon success, false otherwise
	 */
	public boolean putDoc(String url, byte[] content) {
		if(url==null || content==null) return false;
		DocEntry entry = getDoc(url);
		if(entry==null) {
			entry = new DocEntry();
		}
		entry.setUrl(url);
		//entry.setContentType(type);
		entry.setContent(content);
		entry.setLastAccessed(System.currentTimeMillis());
		da.crawlerData.put(entry);

		return true;
	}
	
	/**
	 * Check if document at url is in database
	 * @param urlId - requested document's url
	 * @return true if exists, false otherwise
	 */
	public boolean containDoc(String urlId) {
		if (urlId != null) {
			return da.crawlerData.contains(urlId);
		} else {
			return false;
		}
	}
	
	/** Get Document from DB
	 * @param urlId
	 * @return document entry
	 */
	public DocEntry getDoc(String urlId) {
		if(containDoc(urlId)) return da.crawlerData.get(urlId);
		else return null;
	}
	
	public byte[] getDocContent(String urlId) {
		byte[] content = getDoc(urlId).getContent();
		if(content ==null) return null;
		else return content;
	}
	
	
	/**
	 * Get time that last accessed for a specific doc
	 * @param url => key
	 * @return time last accessed
	 * 
	 * **/
	public long getDocLastAccessTime(String url) {
		if(containDoc(url)) 
			return da.crawlerData.get(url).getLastAccessed();
		else return -1;
	}
	
	
	
	/******************************************
	 * END 
	 * *************************************/
	
	
	
	


	
	/******************************************
	 * ****************************************
	 * 
	 * Functions for Robot Info
	 * 
	 * ****************************************
	 * *************************************/

	
	/**Checks if it contains Robots Info
	 * @param url
	 * @return
	 */
	public boolean containsRobot(String url) {
		URLInfo cur = new URLInfo(url);
		if(da.robotData.contains(cur.getHostName()))return true;
		else return false;
	}
	
	/** Put RobotsTxt Entry to DB
	 * @param robot
	 * @param url
	 * @return true upon success, false otherwise
	 */
	public boolean putRobotTxt(RobotsTxtInfo robot, String host) {
		if(containsRobot(host)) return false;
		else {
			RobotTxtEntry data = new RobotTxtEntry ();
			try {
				if(robot!=null) 
				data.setRobotsTxtInfo(CrawlerUtil.objToByte(robot));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.info("robot convert error");
			}
			data.setURI(host);
			da.robotData.put(data);
			return true;
		}
	}
	
	
	/** 
	 * @param url
	 * @return
	 */
	public RobotsTxtInfo getRobotFromURL(String url) {
		URLInfo cur = new URLInfo(url);
		if(containsRobot(cur.getHostName())) {
			byte[] info = da.robotData.get(cur.getHostName()).getRobotsInfo();
			try {
				if(info == null) return null;
				return (RobotsTxtInfo) CrawlerUtil.byteToObj(info);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				log.info("robot convert error");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.info("robot convert error");
			}
			
			
		}
		return null;
	}
	
	public long getCrawlDelay(String url) {
		URLInfo info = new URLInfo(url);
		if(containsRobot(info.getHostName())) {
			return getRobotFromURL(url).getCrawlDelay("cis455crawler");
		}
		return 0;
	}
	
	
	/******************************************
	 * END 
	 * *************************************/
	
	
	
	
	
	
	
	
	
	

	
	
	/******************************************
	 * ****************************************
	 * 
	 * Functions for Frointer Queue
	 * 
	 * ****************************************
	 * *************************************/
	

	/**Put the valid url to queue
	 * @param url
	 * @return
	 */
	public synchronized boolean putToQueue(String url) {
		if(containsVisitedURL(url)) return false;
		else{
		//	log.debug("adding a url to queue...."+url);
			FrontierQueue q = da.queueData.get("FrontierQueue");
			q.addQueue(url);
			da.queueData.put(q);
			return true;
		}
	}
	
	public void initQueue() {
		FrontierQueue q = new FrontierQueue();
		q.setKey("placeholder");
		da.queueData.put(q);
		if(!da.queueData.contains("FrontierQueue")) {
			FrontierQueue fq = new FrontierQueue();
			fq.setKey("FrontierQueue");
			da.queueData.put(fq);
		}

	}
	
	/**Poll an URL from Queue in DB
	 * @return url
	 */
	public synchronized String pollFromQueue() {
		FrontierQueue q = da.queueData.get("FrontierQueue");
		String url = q.pollQueue();
		da.queueData.put(q);
		
		return url;
	}
	
	/**Check if the Queue is Empty
	 * @return true if is empty, false otherwise
	 */
	public boolean isQueueEmpty() {
		return da.queueData.get("FrontierQueue").isEmpty();
	}
	/**Get the size of queue
	 * @return size
	 */
	public int getQueueSize() {
		return da.queueData.get("FrontierQueue").getSize();
	}
	
	public synchronized void pushNextQueueToDisk(Queue<String> q) {
		int cnt = 0;
		FrontierQueue fq = da.queueData.get("FrontierQueue");
		if(fq.getSize()<5000) {
			while(!q.isEmpty()&& cnt < 1000) {
				fq.addQueue(q.poll());
				cnt++;
				}
		}

		da.queueData.put(fq);
		
	}
	
	public synchronized void pollFromDiskQueue(int num, Queue<String> q){

			FrontierQueue fq = da.queueData.get("FrontierQueue");
			int cnt = 0;
			while(!fq.isEmpty() && cnt<num) {
					q.offer(fq.pollQueue());
					log.debug("polling from db: "+cnt);
					cnt++;
			}
			da.queueData.put(fq);
		

		
	}
	
	
	
	/******************************************
	 * END 
	 * *************************************/
	
	
	
	
	
	
	
	
	
	
	
	
	
	/******************************************
	 * ****************************************
	 * 
	 * Functions for Visited URL
	 * 
	 * ****************************************
	 * *************************************/
	
	/**Check if a URL is visited
	 * @param url
	 * @return true if the url has been visited, false otherwise
	 */
	public boolean containsVisitedURL(String url) {
		// if yes, update last visited time
		if(da.visitedURLData.contains(url)) 
			da.visitedURLData.get(url).setLastAccessTime(System.currentTimeMillis());
		else return false;
		return true;
	}
	
	/** Put url to Visited Set
	 * @param url
	 * @return true upon success, false otherwise
	 */
	public boolean putVisitedToDB(String url) {
		if(containsVisitedURL(url)) return false;
		else {
			VisitedURL entity = new VisitedURL();
			entity.addUrl(url);
			entity.setLastAccessTime(System.currentTimeMillis());
			da.visitedURLData.put(entity);
			return true;
		}
		
	}
	
	public long getLastVisitTime(String url) {
		if(containsVisitedURL(url)) return 0;
		else {
			return da.visitedURLData.get(url).getLastAccessTime();
		}
	}
	
	
	public boolean updateVisitTime(String host) {
		if(containsVisitedURL(host)) {
			da.visitedURLData.get(host).setLastAccessTime(System.currentTimeMillis());
			return true;
		}
		return false;
	}
	/******************************************
	 ******************* END ****************
	 * *************************************/
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/******************************************
	 * ****************************************
	 * 
	 * Functions for Extracted URL
	 * 
	 * ****************************************
	 * *************************************/
	

	/** Put the extracted URL to db and add it to the URL hashtable
	 * @param url, outLinks
	 * @return true upon success, false otherwise
	 */
	public boolean putExtractedURL(String url, List<String> outLinks) {

			List<String> outIds = new ArrayList<>();
			for(String link : outLinks) {
				//String linkId = hash(link);
				putURLHash(link);
				outIds.add(CrawlerUtil.generateURLId(link));
			}
			
			ExtractedURLEntry entry = new ExtractedURLEntry();
			entry.addUrl(url);
			entry.setExtractedLinks(outIds);
			da.outURLData.put(entry);

			return true;
			
	}
	
	/** Get the Extracted URL of a specified URL from DB
	 * @param url
	 * @return List<String> extracted urls, null if url DNE
	 * 
	 */
	public List<String> getExtractedURL(String url) {
		if(!da.outURLData.contains(CrawlerUtil.generateURLId(url))) return null;
		else {
			
			return da.outURLData.get(CrawlerUtil.generateURLId(url)).getExtractedLinks();
		}

	}
	
	
	/******************************************
	 ******************* END ****************
	 * *************************************/
	
	
	
	
	
	
	
	
	
	
	/******************************************
	 * ****************************************
	 * 
	 * Functions for URL Hash
	 * 
	 * ****************************************
	 * *************************************/
	
	
	

	/**Put new URL -> URLID entry to DB
	 * @param url
	 * @return true upon sucess, false otherwise
	 */
	public boolean putURLHash(String url) {
		if(da.urlHashData.contains(CrawlerUtil.generateURLId(url))) return false;
		else {
			URLHashEntry hashEntry = new URLHashEntry();
			hashEntry.setURL(url);
			da.urlHashData.put(hashEntry);
			return true;
		}
		
	}
	
	/** Get the URL id from URL Link in DB
	 * @param url
	 * @return id of the url, null if url DNE
	 */
	public String getURLFromId(String id) {
		if(!da.urlHashData.contains(id))return null;
		else return da.urlHashData.get(id).getURL();
	}
	
	
	/******************************************
	 ******************* END ****************
	 * *************************************/

	
	
	public void putMetaData(String url, String title, byte[] body) {
		if(da.metaData.contains(CrawlerUtil.generateURLId(url))) return;
		MetaDataEntry entry = new MetaDataEntry();
		entry.setContent(body);
		entry.setTitle(title);
		entry.setUrl(url);
		da.metaData.put(entry);
	}
	
	public String getChunkBody(String id) {
		MetaDataEntry entry = getMetaData(id);
		if(entry!=null) return entry.getChunkBody().toString();
		return null;
	}
	
	public String getTitle(String id) {
		MetaDataEntry entry = getMetaData(id);
		if(entry!=null) return entry.getTitle();
		return null;
	}
	
	
	
	public MetaDataEntry getMetaData(String id) {
		if(da.metaData.contains(id)) {
			return da.metaData.get(id);
		}
		
		return null;
	}
	
	
	/*****/
	
	
	
	
	
	
	
	public List<String> test() {
		//retrieveFile();
		//retrieveVisitedURL();
		//retrieveExtractedURL();
		return retrievemetaURL();
	}
	
	
	
	public void retrieveFile() {
		EntityCursor<DocEntry> cursor = da.crawlerData.entities();
		for(DocEntry doc: cursor) {
			System.out.println("[DOC Entry]--In DataBase URL Info: "+ doc.getUrl());
			System.out.println("[DOC Entry]--In DataBase id : "+ doc.getId());
//			System.out.println("In DataBase lastAccess Info: "+ doc.getLastAccessed());
		}
		cursor.close();

	}
	public void retrieveVisitedURL() {
		EntityCursor<VisitedURL> cursor = da.visitedURLData.entities();
		for(VisitedURL url: cursor) {
			System.out.println("[VisitedURL]--In DataBase visitedURL Info: "+ url.getUrl());
		}
		cursor.close();
	}
	
	public void retrieveExtractedURL() {
		
		EntityCursor<ExtractedURLEntry> cursor = da.outURLData.entities();
		for(ExtractedURLEntry entry : cursor) {
			
			System.out.println("[ExtractedURLEntry]--In DataBase url_id: "+ entry.getId());
			System.out.println("[ExtractedURLEntry]--In DataBase url: "+ entry.getUrl());
			System.out.print("[ExtractedURLEntry]--In DataBase extracted_id: {");
			for(String s : entry.getExtractedLinks()) {
				System.out.print(s+" ,");
			}
			System.out.println("}");
		}
		
		cursor.close();
		
	}
	
	
	public void retrieveQueue() {
		EntityCursor<FrontierQueue> cursor = da.queueData.entities();
		for(FrontierQueue q: cursor) {
			while(!q.isEmpty()) {
				System.out.println("[Queue]--In DataBase queue Info: "+ q.pollQueue());
			}
			
		}
		cursor.close();
	}
	
	public List<String> retrievemetaURL() {
		List<String> ids = new ArrayList<>();
		EntityCursor<MetaDataEntry> cursor = da.metaData.entities();
		for(MetaDataEntry entry : cursor) {
			ids.add(entry.getId());
			System.out.println("[MetaDataURLEntry]--In DataBase url_id: "+ entry.getId());
			System.out.println("[MetaDataURLEntry]--In DataBase url_id: "+ entry.getTitle());
			System.out.println("[MetaDataURLEntry]--In DataBase url: "+ entry.getUrl());

		}
		cursor.close();
		return ids;
	}

	
}
