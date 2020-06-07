package storage;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
	private static String envDirectory = null;
	
	private static Environment myEnv;
	private static EntityStore store;
	
	public DBWrapper(String dbdir){
		
		File dir = new File(dbdir);
		dir.mkdirs();

		 EnvironmentConfig envConfig = new EnvironmentConfig();
	     StoreConfig storeConfig = new StoreConfig();
	     envConfig.setAllowCreate(true);
	     envConfig.setTransactional(false);
	     storeConfig.setAllowCreate(true);
	     storeConfig.setTransactional(false);
	        
	     myEnv = new Environment(dir,envConfig);
		
	}

}
