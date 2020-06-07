package edu.upenn.cis455.indexer.storage;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;


public class DBIndexer {

	static Logger log = Logger.getLogger(DBIndexer.class);
	private String envDirectory = null;

	private Environment myEnv;

	private EntityStore store;

	private DBAccessor da;
	private boolean isSetUp = false;

	public DBIndexer(String path) {
		if(!isSetUp) {
			this.envDirectory = path;
			setup(envDirectory);
			da = new DBAccessor(store);
			isSetUp = true;
		}
	}

	/**
	 * setup database
	 */
	public void setup(String path) {

		File data = new File(path);

		if (!data.exists()) {
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
			this.myEnv = new Environment(data, envConfig);
			store = new EntityStore(myEnv, "EntityStore", storeConfig);

		} catch (DatabaseException dbe) {
			System.out.println("Database Error: " + dbe.toString());
			System.out.println(dbe.getStackTrace());
			System.exit(-1);
		}
	}

	public boolean putIndexEntry(IndexEntry entry) throws NoSuchAlgorithmException {
		if (entry != null) {
			// put the object in the database
			da.index.put(entry);
			return true;
		} else {
			return false;
		}
	}

	public IndexEntry getIndexEntry(String word) {
		if (word != null && da.index.contains(word)) {
			return da.index.get(word);
		} else {
			return null;
		}
	}
	
	public boolean containsIndex(String word) {
		if (word != null && da.index.contains(word)) {
			return true;
		} else {
			return false;
		}
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
	
	/**
	 * show a brief info of all the indexes
	 */
	public void retrieveIndexInfo() {
		EntityCursor<IndexEntry> cursor = da.index.entities();
		for(IndexEntry index: cursor) {
			System.out.println("In DataBase word: "+ index.getWord());
			Map<String, ArrayList<String>> meta = index.getMeta();
			Iterator it = meta.keySet().iterator();
			while (it.hasNext()) {
				String docID = (String) it.next();
				System.out.println(docID+" "+index.getTitleTF(docID)+" "+index.getTitleLocations(docID));
			}
		}
		cursor.close();
	}
	
	/**
	 * get a cursor for all the indexes
	 * @return
	 */
	public EntityCursor<IndexEntry> getIndexCursor() {
		return da.index.entities();
	}
	
	
	/**
     * delete the old DB files
     */
    public static void clearDB(String DBDirFullPath) {
    	File file = new File(DBDirFullPath);
    	if (file.exists()&&file.isDirectory()) {
    		deleteDir(DBDirFullPath);
    	}
    	file.mkdirs();
    }
    
    /**
	 * delete the whole directory
	 * @param dirPath
	 */
	public static void deleteDir(String dirPath) {
		File file = new File(dirPath);
		if (file.exists()) {
			if (file.isFile()) {
				file.delete();
			} else {
				File[] files = file.listFiles();
				if (files == null) {
					file.delete();
				} else {
					for (int i = 0; i < files.length; i++) {
						deleteDir(files[i].getAbsolutePath());
					}
					file.delete();
				}
			}
		}
	}
}

class DBAccessor {

//	PrimaryIndex<String, DocEntry> crawlerData;
//	PrimaryIndex<String, UserEntry> userData;
//	PrimaryIndex<String, ChannelEntry> channelData;
//	PrimaryIndex<String, Class> entryData;
	PrimaryIndex<String, IndexEntry> index;

	public DBAccessor(EntityStore store) {
//		// Getting the primary index for data from crawler
//		crawlerData = store.getPrimaryIndex(String.class, DocEntry.class);
//
//		// Getting the primary index for data from users
//		userData = store.getPrimaryIndex(String.class, UserEntry.class);
//
//		// Getting the primary index for data from channels
//		channelData = store.getPrimaryIndex(String.class, ChannelEntry.class);

//		entryData = store.getPrimaryIndex(String.class, entryClass);
		index = store.getPrimaryIndex(String.class, IndexEntry.class);

	}

}