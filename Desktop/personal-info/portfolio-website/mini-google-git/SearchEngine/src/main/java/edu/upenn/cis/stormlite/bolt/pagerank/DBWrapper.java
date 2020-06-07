// use map and idmap
package edu.upenn.cis.stormlite.bolt.pagerank;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Database interface
 */
public class DBWrapper {

	private Environment env = null;
	private Database classTable = null;

	private Database idMapDB = null;
	Map<String, ListOfString> idMap = null;

	private Database resMapDB = null;
	Map<String, String> resMap = null;

	private Database hashToBoltDB = null;

//	private Database workerAddrsDB = null;
//	Map<String, String> workerStatusMap = null;



	private boolean isDBClosed;
	DatabaseConfig dbConfig = null;

	public DBWrapper(String dirWithId) {

		EnvironmentConfig envConfig = null;
		try {
			// Open the environment, creating one if it does not exist
			envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(true);
		} catch (DatabaseException dbe) {
			// System.out.println("[DBWrapper]: fail to init envConfig");
		}

		try {
			env = new Environment(new File(dirWithId), envConfig);
		} catch (DatabaseException dbe) {
			System.out.print(dbe.getMessage());
			// System.out.println("\n[DBWrapper]: fail to init env");
		}

		try {
			// Open the database, creating one if it does not exist
			dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			dbConfig.setSortedDuplicates(false);
			dbConfig.setTransactional(true);
		} catch (DatabaseException dbe) {
			// System.out.println("[DBWrapper]: fail to setup dbConfig");
		}

		try {

			classTable = env.openDatabase(null, "ClassDB", dbConfig);
			StoredClassCatalog catalog = new StoredClassCatalog(classTable);

			idMapDB = env.openDatabase(null, dirWithId, dbConfig);
			TupleBinding<String> idMapKeyBinding = TupleBinding.getPrimitiveBinding(String.class);
	        EntryBinding<ListOfString> idMapKeyEntryBinding = new SerialBinding<ListOfString>(catalog, ListOfString.class);
	        idMap = new StoredSortedMap<String, ListOfString>(idMapDB, idMapKeyBinding, idMapKeyEntryBinding, true);


			resMapDB = env.openDatabase(null, dirWithId, dbConfig);
			TupleBinding<String> resKeyBinding = TupleBinding.getPrimitiveBinding(String.class);
	        EntryBinding<String> resEntryBinding = TupleBinding.getPrimitiveBinding(String.class);
	        resMap = new StoredSortedMap<String, String>(resMapDB, resKeyBinding, resEntryBinding, true);

	        hashToBoltDB = env.openDatabase(null, dirWithId, dbConfig);
			TupleBinding<String> hashToBoltKeyBinding = TupleBinding.getPrimitiveBinding(String.class);
//	        EntryBinding<BoltInfo> hashToBoltEntryBinding = new SerialBinding<BoltInfo>(catalog, BoltInfo.class);
//	        hashToBoltMap = new StoredSortedMap<String, BoltInfo>(hashToBoltDB, hashToBoltKeyBinding, hashToBoltEntryBinding, true);


			isDBClosed = true;

		} catch (DatabaseException dbe) {
			// Exception handling
			// System.out.println("[DBWrapper]: fail to init Tables or Maps");

			if (env != null) {
				env.cleanLog();
				env.close();
			}
		}

	}

	//////////////////////////////////////////////////
	/////////////////// idMap
	/////////////////////////////////////////////////


	public synchronized void remove(String key) {
		this.idMap.remove(key);
	}
	/*
	 * idMap
	 */
	public Map<String, ListOfString> getMap() {
		return this.idMap;
	}


	/*
	 * idMap.keySet()
	 */
	public Set<String> keySet() {
		// System.out.println("[DBWrapper]:stateByKeykeySet(), idmap.size() = " + idmap.size());
		return this.idMap.keySet();
	}

	/*
	 * idMap.put(key, value)
	 */
	public synchronized void put(String key, ListOfString value) {
		this.idMap.put(key, value);
	}

	/*
	 * idMap.get(key)
	 */

	public synchronized ListOfString get( String key) {
		return this.idMap.get(key);
	}

	/*
	 * idMap.containsKey(key)
	 */
	public synchronized boolean containsKey(String key) {
		return this.idMap.containsKey(key);
	}





	//////////////////////////////////////////////////
	/////////////////// resMap
	/////////////////////////////////////////////////

	public Map<String, String> getResMap() {
		return this.resMap;
	}

	public Set<String> resKeySet() {
		return this.resMap.keySet();
	}

	public synchronized void resPut(String key, String value) {
		this.resMap.put(key, value);
	}

	public synchronized String resGet(String key) {
		String val = null;
		if(this.resMap.containsKey(key)) {
			val = this.resMap.get(key);
		}
		return val;
	}



	public synchronized String getRankById(String key) {
		return this.resMap.get(key);
	}

	public synchronized boolean resContainsKey(String key) {
		return this.resMap.containsKey(key);
	}


	//////////////////////////////////////////////////
	/////////////////// hashToBolt Mapping
	/////////////////////////////////////////////////
//	public Map<String, BoltInfo> getHashToBoltMapping() {
//		//System.out.println("[DBWrapper]:this.hashToBoltMap.keySet() =  " +  this.hashToBoltMap.keySet());
//		return this.hashToBoltMap;
//	}
//
//	public BoltInfo hashToBoltGet(String key) {
//		return this.hashToBoltMap.get(key);
//	}
//
//	public int hashToBoltSize() {
//
//		return this.hashToBoltMap.size();
//	}
//
//	public void hashToBoltPut(String key, BoltInfo value) {
//		// System.out.println("[DBWrapper]: hashToBoltPut() key =   " + key);
//		this.hashToBoltMap.put(key, value);
//	}








	//////////////////////////////////////////////////
	/////////////////// Close
	/////////////////////////////////////////////////
	/**
	 *
	 * @return if the database is closed.
	 */
	public synchronized boolean isClosed() {
		return isDBClosed;
	}

	/**
	 * close the database without flushing
	 */
	public synchronized void close() {


		if (classTable != null)
			classTable.close();

		if (env != null) {
			env.cleanLog();
			env.close();
		}
		isDBClosed = true;
	}

	/**
	 * close the database with flushing.
	 */
	public synchronized void closeWithFlushing() {
		if (classTable != null)
			classTable.close();
		if (env != null) {
			env.cleanLog();
			env.close();
		}
		isDBClosed = true;
	}






}
