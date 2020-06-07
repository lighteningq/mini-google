package edu.upenn.cis455.crawler;

import java.util.*;

import org.apache.log4j.Logger;

import edu.upenn.cis455.crawler.masterServer.CrawlerMaster;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.MetaDataEntry;
import edu.upenn.cis455.storage.URLHashEntry;

public class SearchEngineDB {
	static Logger log = Logger.getLogger(SearchEngineDB.class);
	public Map<Integer, DBWrapper> dbMap = new HashMap<Integer,DBWrapper>();
	int num;
	
	public SearchEngineDB(int numWorker) {
		num = numWorker;
		setup(num);
	}
	
	
	void setup(int num) {
		for(int i = 0; i<num; i++) {
			try {
				String dir = "/storage/000/home/ec2-user/crawler_00"+String.valueOf(i);
				DBWrapper db = new DBWrapper(dir);
				dbMap.put(i,db);
			}catch(Exception e) {
				e.printStackTrace();
			}

		}
	}
	
	

	// id -> url
	public String getUrl(String id){
		if(id==null || id.equals("")) return null;
		int idx = getDBIndex(id);
		log.info("id is: " + id+" idx is:"+idx);
		DBWrapper db = dbMap.get(idx);
		String url = db.getURLFromId(id);
		log.info("url is: " +url);
		
		return url;
	}
	
	
	
	
	// id -> url
	public List<Map<String,String>> getQueryMetaData(List<String> ids){
		if(ids==null || ids.size()==0) return null;
		List<Map<String, String>> res = new ArrayList<>();
		List<Integer> idx = getIndexList(ids);
		
		for(int i = 0; i<ids.size();i++) {
			Map<String, String> map = new HashMap<>();
			DBWrapper db = dbMap.get(idx.get(i));
			String url = db.getURLFromId(ids.get(i));
			map.put(ids.get(i), url);
			res.add(map);
		}
		
		return res;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	private List<Integer> getIndexList(List<String> ids){
		List<Integer> res = new ArrayList<>();
		for(String id : ids) {
			res.add(getDBIndex(id));
		}
		return res;
	}
	
	private int getDBIndex(String urlId) {
		int hash = 0;
		hash^= (urlId).hashCode();
		hash = hash % (5* num); // assume 5 urlDistributeBolt per worker
		if (hash<0) hash = hash + (5* num);
		
		return hash / 5;
	}
}
