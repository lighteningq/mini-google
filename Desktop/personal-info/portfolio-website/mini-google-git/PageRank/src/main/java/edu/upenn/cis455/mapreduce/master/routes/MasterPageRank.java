package edu.upenn.cis455.mapreduce.master.routes;


import java.io.IOException;

import java.util.*;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.bolt.pagerank.DBWrapper;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Master: Handle POST /pageRanks. Send request to worker server. or to EBS direclty.
 */
public class MasterPageRank implements Route {
    private MasterAppConfig master;
    // private Map<String, BoltInfo> hashToBolt;
    private int bucketSize = 1;
    // DBWrapper hashDB;
    public MasterPageRank(MasterAppConfig master) {
        this.master = master;
        // this.hashDB = new DBWrapper(MasterApp.HashToBolt);
        this.bucketSize = MasterAppConfig.hashToBoltDB.hashToBoltSize();
        //System.out.println("[MasterPageRank]:hashToBoltDB.getHashToBoltMapping().keySet()=  " + MasterAppConfig.hashToBoltDB.getHashToBoltMapping().keySet() + ", bucketSize = " + bucketSize);
        // System.out.println("[MasterPageRank]:master.workerAddrsDB.resKeySet() =  " + MasterAppConfig.workerAddrsDB.resKeySet());
         
    }

    @SuppressWarnings("unchecked")
	@Override
    public Object handle(Request req, Response res) {
    	System.out.println("[MasterPageRank]: POST /pageRanks");
    	final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        HashSet<String> queryIds = null;
        try {
        	queryIds = om.readValue(req.body(), HashSet.class);
		} catch (JsonParseException e1) {
			e1.printStackTrace();
		} catch (JsonMappingException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
  
        Config pageRanks = new Config();
        
        int workersNum = Integer.parseInt(master.resGet("workersNum"));
        
        List<HashMap<String, HashSet<String>>> queryIdList = new ArrayList<>();
        for (int i = 0; i < workersNum; i++) {
        	HashMap<String, HashSet<String>> hashToQueryIds = new HashMap<>();
        	queryIdList.add(hashToQueryIds);
        }
        //System.out.println("[MasterPageRank]: queryIdList size  = " + queryIdList.size());
        
        // assign queryIds to each worker node storage
        for (String queryId: queryIds) {
        	
    		int hash = queryId.hashCode();
    		hash = hash % this.bucketSize;
    		if(hash < 0) {
    			hash = hash + this.bucketSize; 
    		}
    		System.out.println("[MasterPageRank]: queryId  = " + queryId + ", hash = " + hash);
    		String hashStr = String.valueOf(hash);
        	int workerIdx = getWorkerIdx(hashStr);
        	System.out.println("[MasterPageRank]: workerIdx = " + workerIdx + ", hashStr = " + hashStr);
        	queryIdList.get(workerIdx).putIfAbsent(hashStr, new HashSet<>());
        	queryIdList.get(workerIdx).get(hashStr).add(queryId);
        }
        
        // retrieve pageRanks for each worker node
        for (int i = 0; i < workersNum; i++) {
        	HashMap<String, HashSet<String>> hashToQueryIds = queryIdList.get(i);
        	System.out.println("[MasterPageRank]: i = " + i);
        	Config ranksFromWorker = getRanksFromWorker(i, hashToQueryIds);
        	
        	pageRanks.putAll(ranksFromWorker);
        }
        
        // send back pageRanks to client.
        String parameters = "";
		try {
			parameters = om.writerWithDefaultPrettyPrinter().writeValueAsString(pageRanks);
			System.out.println("[MasterPageRank]:  POST /pageRanks parameters =" + parameters);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return parameters;
    }
    
	private Config getRanksFromWorker(int i, HashMap<String, HashSet<String>> hashToQueryIds) {
		
		Config res = new Config();

		for (String hash:  hashToQueryIds.keySet()) {
			
			long beforeOpenDB = System.currentTimeMillis();
			DBWrapper resDB = this.master.DBMap.get(hash);
			
			System.out.println("[MasterPageRank]:hash = " + hash + ", resDB.resKeySet() = " + resDB.resKeySet());
			for (String queryId: hashToQueryIds.get(hash)) {
				
				String rank = resDB.resGet(queryId);
				res.put(queryId, rank);
			}
			long dur =  System.currentTimeMillis() - beforeOpenDB;
			// System.out.println("[MasterPageRank]: query one url duration:" + dur + " ms"); 
		}
		return res;
	}
	
	private int getWorkerIdx(String hash) {
		BoltInfo boltInfo = MasterAppConfig.hashToBoltDB.hashToBoltGet(hash);
		if(boltInfo == null ) {
			//System.out.println("[MasterPageRank]: boltInfo is Null");
		}
		//System.out.println("[MasterPageRank]: getWorkerIdx(), hash = " + hash + ", hashToBolt.getWorkerIdx = " + boltInfo.getWorkerIdx()  );
		int workerIdx = 0;
		try {
			workerIdx = Integer.parseInt( boltInfo.myWorkerIdx );
		} catch(NullPointerException e) {
			workerIdx = 0;
			//System.out.println("[MasterPageRank]: parsed myWorkeridx failed = " + hash + ", hashToBolt.getBoltId = " + boltInfo.getBoldId() );
		}
		
		return workerIdx;
	}
}