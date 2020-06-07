package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.*;

import org.apache.log4j.Logger;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
	static Logger log = Logger.getLogger(DBWrapper.class);
	private String rootDir = null;
	private Environment myEnv;
	private EntityStore store;
	
	PrimaryIndex<String,ReduceState> wordIndex;
	
	public DBWrapper(String dir){
		rootDir = dir;
		
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		// check if the file exists in given root directory
		File myfile = new File(rootDir);
		if(!myfile.exists()) {
			myfile.mkdirs();
			log.debug("new directory created for DB...");
		}
		myEnv = new Environment(myfile,envConfig);
		
		//prepare and create entity store
		StoreConfig stConfig = new StoreConfig();
		stConfig.setAllowCreate(true);
		stConfig.setTransactional(true);
		store = new EntityStore(myEnv,"EntityStore",stConfig);
		
		//initialize reduce state
		wordIndex = store.getPrimaryIndex(String.class, ReduceState.class);
		log.debug("Finished Setting Up Berkeley DB...");
	}
	
	public void syncDB() {
		if(store != null){
			store.sync();
		}
		if(myEnv != null){
			myEnv.sync();
		}
		
	}
	
	public void deleteAllRecords() {
		EntityCursor<ReduceState> cursor = wordIndex.entities();
		cursor.delete();
		while(cursor.next()!=null) {
			cursor.delete();
		}
		
	}
	
	public void closeDB(){
		if(store != null){
			store.close();
		}
		if(myEnv != null){
			myEnv.close();
		}
	}

	
	public boolean containsWord(String word){
		return wordIndex.contains(word);
	}
	
	public List<String> getCountList(String word){
		ReduceState wordEntity = wordIndex.get(word);
		return wordEntity.getList();
	}
	
	public void addNewWord(String word,String count){
		ReduceState reduceState = new ReduceState();
		reduceState.setKey(word);
		reduceState.addList(count);
		wordIndex.put(reduceState);
		log.debug(word+" add to db with count "+count);
	}
	
	public void addCountToWord(String word,String count){
		ReduceState reduceState = wordIndex.get(word);
		reduceState.addList(count);
		wordIndex.put(reduceState);
	}
	
	public List<String> getWordList(){
		List<String> wordList = new ArrayList<>();
		for(String word : wordIndex.keys()){
			wordList.add(word);
		}
		return wordList;
	}
}