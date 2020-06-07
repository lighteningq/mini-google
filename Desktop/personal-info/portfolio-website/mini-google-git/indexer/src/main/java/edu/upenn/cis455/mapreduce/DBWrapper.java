package edu.upenn.cis455.mapreduce;

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.persist.EntityStore;

/** (MS1, MS2) A wrapper class which should include:
  * - Set up of Berkeley DB
  * - Saving and retrieving objects including crawled docs and user information
  */
public class DBWrapper {
	
	/**
	 * variables
	 */
	private String envDirectory = null;
	
	private Environment myEnv;
	private EntityStore store;
	
	private Database BDB;
    private boolean isrunning = false;
    
    public DBWrapper(){
    	super();
    }
    
    /**
     * set environment directory path
     * @param path
     */
    public void setEnvDir(String path) {
    	if (path != null) {
    		this.envDirectory = path;
    	}
    }
    
    /**
     * initiate the BDB
     * @param path
     */
    public void initiate() {  
        if (isrunning) {
            return;
        }  
      //--------------- file ----------------
        File envDir = new File(envDirectory);
		if (!envDir.exists()){
		    envDir.mkdir();
		}
      //--------------- environment ----------------
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        myEnv = new Environment(envDir, envConfig);
      //--------------- BDB ----------------
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
   
        BDB = myEnv.openDatabase(null, "myDB", dbConfig);
        isrunning = true;
    }
	
    /**
     * Closing the BDB
     */
    public void close() {
        if (isrunning) {
            isrunning = false;
            BDB.close();
            myEnv.close();
        }
    }
	
    /**
     * get running boolean
     * @return
     */
    public boolean isRunning() {
    	return isrunning;
    }
	
    /**
     * store key:data into BDB
     * @param key
     * @param data
     */
    public void set(byte[] key, byte[] data) {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        keyEntry.setData(key);
        dataEntry.setData(data);
        
        OperationStatus status = BDB.put(null, keyEntry, dataEntry);
        myEnv.sync();
        
        if (status != OperationStatus.SUCCESS) {
            throw new RuntimeException("Data insertion got status " + status);
        }
    }
    
    
    /**
     * overload set method
     * @param key
     * @param data
     */
    public void set(String key, String data) {
    	set(key.getBytes(StandardCharsets.UTF_8), data.getBytes(StandardCharsets.UTF_8));
    }
    
    
    /**
     * select data by certain key value
     * @param aKey
     */
    public byte[] selectByKey(String aKey) {
        DatabaseEntry theKey =null;
        DatabaseEntry theData = new DatabaseEntry();
        byte[] retData = null;
        try {
             theKey = new DatabaseEntry(aKey.getBytes("utf-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (BDB.get(null,theKey, theData,  
                LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            retData = theData.getData();
            String foundData = new String(retData);
//            System.out.println("For key: '" + aKey + "' found data: '" + foundData + "'.");
            System.out.println("Found in DB.");
        }
        return retData;
    }
    
    /**
     * select all in the BDB
     */
    public void selectAll() {
        Cursor cursor = null;
        cursor = BDB.openCursor(null, null);
        DatabaseEntry theKey=null;
        DatabaseEntry theData=null;
        theKey = new DatabaseEntry();
        theData = new DatabaseEntry();
           
        while (cursor.getNext(theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            System.out.println(new String(theData.getData()));
        }
        cursor.close();
    }
    
    /**
     * get a cursor of this DB
     * @return
     */
    public Cursor getCursor() {
        Cursor cursor = null;
        cursor = BDB.openCursor(null, null);
    	return cursor;
    }
    
    /**
     * delete entry by certain key
     * @param key
     */
    public void delete(String key) {
        DatabaseEntry keyEntry =null;
        try {
            keyEntry = new DatabaseEntry(key.getBytes("utf-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        BDB.delete(null, keyEntry);
    }  
    
    
    /**
     * get the number of entries in BDB
     * @return
     */
    public long getCount() {
    	return BDB.count();
    }
    
    
}