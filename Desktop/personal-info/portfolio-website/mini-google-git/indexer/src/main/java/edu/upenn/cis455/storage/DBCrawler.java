package edu.upenn.cis455.storage;

import java.io.File;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

import edu.upenn.cis455.storage.DocEntry;

public class DBCrawler {
	static Logger log = Logger.getLogger(DBCrawler.class);
	private static String envDirectory = null;
	
	private static Environment myEnv;
	
	private static EntityStore store;
	
	private static DBCrawlerAccessor da;
	private static boolean isSetUp = false;
	
	
	
	/*DB Enviorment SetUp  */
	
	public DBCrawler(String path) {
		if(!isSetUp) {
			DBCrawler.envDirectory = path;
			setup(envDirectory);
			da = new DBCrawlerAccessor(store);
			isSetUp = true;
		}
	}

	
	
	/**
	 * setup database
	 * */
	public void setup(String path) {
		
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
			DBCrawler.myEnv = new Environment(data, envConfig);
			store = new EntityStore(myEnv, "EntityStore",storeConfig);
			
	} catch (DatabaseException dbe) {
		System.out.println("Database Error: "+dbe.toString());
		System.out.println(dbe.getStackTrace());
		System.exit(-1);
	}
			

	}
	
	
	/**
	 * 
	 * Shutdown DB
	 * 1. shutdown store
	 * 2. shutdown env
	 * */
	
	public static void shutdownDB() {
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
		DBCrawler.envDirectory = envDirectory;
	}


	public static Environment getMyEnv() {
		return myEnv;
	}


	public static void setMyEnv(Environment myEnv) {
		DBCrawler.myEnv = myEnv;
	}


	public static EntityStore getStore() {
		return store;
	}


	public static void setStore(EntityStore store) {
		DBCrawler.store = store;
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
	
	/**
	 * To see the URL crawled in DB
	 */
	public static void retrieveURL() {
		EntityCursor<DocEntry> cursor = da.crawlerData.entities();
		for(DocEntry doc: cursor) {
			System.out.println("In DataBase URL Info: "+ doc.getUrl());
		}
		cursor.close();
	}
	
	/**
	 * To retrieve DocEntry.
	 * @return
	 */
	public static EntityCursor<DocEntry> getDocEntryCursor(){
		return da.crawlerData.entities();
	}
	
	
}
class DBCrawlerAccessor {

	
	public DBCrawlerAccessor(EntityStore store) {
		// Getting the primary index for data from crawler
		crawlerData = store.getPrimaryIndex(String.class, DocEntry.class);
		
		// Getting the primary index for data from urlHash
		urlHashData = store.getPrimaryIndex(String.class, URLHashEntry.class);
		
		// Getting the primary index for data from queue
		queueData = store.getPrimaryIndex(String.class, FrontierQueue.class);
		
		
		robotData = store.getPrimaryIndex(String.class, RobotTxtEntry.class);
		
		
		visitedURLData = store.getPrimaryIndex(String.class, VisitedURL.class);
		
		
		outURLData = store.getPrimaryIndex(String.class, ExtractedURLEntry.class);
	}
	
	PrimaryIndex<String,DocEntry> crawlerData;
	PrimaryIndex<String,URLHashEntry> urlHashData;
	PrimaryIndex<String,FrontierQueue> queueData;
	PrimaryIndex<String,RobotTxtEntry> robotData;
	PrimaryIndex<String,VisitedURL> visitedURLData;
	PrimaryIndex<String,ExtractedURLEntry> outURLData;
	
}
