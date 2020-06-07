package edu.upenn.cis455.mapreduce.master;


import edu.upenn.cis.stormlite.bolt.pagerank.DBWrapper;
import edu.upenn.cis455.mapreduce.master.routes.*;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static spark.Spark.*;


/*
 * Define master routes and memory storage for worker status.
 */
public class MasterAppConfig {
	
	private Boolean jobNotReady;
	 
    private HashMap<String, WorkerStatusInfo> workerStatus;
    private HashMap<String, List<String>> reduceResults;   
    private HashMap<String, Double> ranks;
    
    // hash-> (boltId, workerIdx, listIdx)
    // private HashMap<String, BoltInfo> reduceBoltMapping;
    private HashMap<String, String> senderBoltMapping;
    public int bucketSize = 2;
    public int workersNum = 1;
    public static DBWrapper hashToBoltDB = new DBWrapper(MasterApp.HashToBolt);
    public static DBWrapper workerAddrsDB = new DBWrapper(MasterApp.WorkersAddr);
    public  Map<String, DBWrapper> DBMap = new HashMap<>();
    
    
    
	public void reset() {
		
		workerStatus = new HashMap<>();
        reduceResults = new HashMap<>();
        // reduceBoltMapping = new HashMap<>();
        senderBoltMapping = new HashMap<>();
        ranks = new HashMap<>();
        
        this.jobNotReady = false;
	}
	
	public void resetWorkerResult(String addr) {
		reduceResults.put(addr, new ArrayList<>());
		
	}
	
	public void setupDBMap() {
		
		for (String hash:  hashToBoltDB.getHashToBoltMapping().keySet()) {
			System.out.println("[MasterAppConfig]: hash = " + hash);
			String boltId = hashToBoltDB.hashToBoltGet(hash).getBoldId();
			int workerIdx = Integer.parseInt(hashToBoltDB.hashToBoltGet(hash).getWorkerIdx());
			
			System.out.println("[MasterAppConfig]: hash = " + hash + ", boltId = " + boltId + ", workerIdx = " + workerIdx);
			
			//String path = WorkerAdmin.dbStore + "/" + WorkerAdmin.resPrefix + boltId;
			String path = WorkerAdmin.dbStore + "/" + "Worker" + workerIdx + "/" + WorkerAdmin.resPrefix + boltId;
			
			System.out.println("[MasterAppConfig]: path = " + path);
			try {
				DBWrapper db = new DBWrapper(path);
				DBMap.put(hash, db);
			} catch(Exception e) {
				
			}

		}
	}
	
    public MasterAppConfig(int portNum) {
    	reset();
        port(portNum);
        defineRoutes();
        setupDBMap();
        System.out.println("Master node startup on port: " + portNum);
    }
	

    private void defineRoutes() {
    	get("/", (request,response) -> {
            return "Please go to the <a href=\"/status\">status page</a>!";
         });
     	
    	// show status page
        get("/status", new StatusHandler(this));
        
        // update worker status
        get("/workerstatus", new WorkerStatusHandler(this));
        
        // send POST /definejob to all workers
        post("/submitjob", new SumitJobHandler(this));
        
        // send POST /runjob to start all workers
        get("/runjobs", new StartJobHandler(this));
        
        // shut down all workers
        get("/shutdown", new ShutDownHandler(this));
        
        
        // accumulate rank for all workers.
        get("/addrank", new AddRankHandler(this));
        
        get("/getrank", new GetRankHandler(this));
        
        // routes mapping
        get("/addBoltId", new AddBoltIdHandler(this));
        get("/getListIdx", new GetListIdxHandler(this));
        get("/workersNum", new Clean(this));
        
        // send hashToBoltId to worker
        post("/hashToBoltId", new HashToBoltId(this));
        
        // get pageRanks
        post("/pageRanks", new MasterPageRank(this));
               
        // for debugging, collect results from all workers
        post("/addresult", new ResultHandler(this));
        
        
    }
    
    /////////////////////////////////////// 
    ///////// Workers Address
    /////////////////////////////////////// 
    public List<WorkerStatusInfo> getWorkerStatus() {
        List<WorkerStatusInfo> status = new ArrayList<WorkerStatusInfo>();
        for (Object value : workerStatus.values()) {
            status.add((WorkerStatusInfo) value);
        }
        return status;
    }

    public String[] getWorkersArray() {
    	int workersNum = 1;
    	if (this.workerStatus.size() != 0) {
    		workersNum = workerStatus.size();
    	} else if (this.workerAddrsDB.resGet("workersNum") != null) {
    		workersNum = Integer.parseInt(this.workerAddrsDB.resGet("workersNum"));
    	}
    	System.out.println("\n[MasterAppConfig]: getWorkersArray(), workersNum = " + workersNum);
        String[] result = new String[workersNum];
        int i = 0;
        for (String key : workerStatus.keySet()) {
            System.out.println(key);
            result[i] = key;
            i++;
        }
        return result;
    }
    

	public String getAddrByIdx(int i) {
		
		String[] addrs = getWorkersArray();
		System.out.println("[MasterAppConfig]: getAddrByIdx(), addrs size  = " + addrs.length + ", i = " + i + ", addrs[i] = " + addrs[i]);
		return addrs[i];
	}
    

    
    public int getWorkersNum() {
        return workerStatus.size();
    }

    public String getWorkerIdxByAddr(String addr) {

        Integer i = 0;
        for (String key : workerStatus.keySet()) {
        	// System.out.println("[MasterAppConfig]: !getWorkerIdxByAddr() key  = " + key );
            if (key.equals(addr)) {
            	
                return i.toString();
            }
            i++;
        }
        return null;
    }

    public void refreshWorkerStatus(WorkerStatusInfo record) {
        synchronized (workerStatus) {
            workerStatus.put(record.ip, record);
        }
    }
    
    /////////////////////////////////////// 
    ///////// Rank Normalization
    ///////////////////////////////////////
    public void updateRank(String iter, String rank) {
        synchronized (ranks) {
        	double preRank = ranks.getOrDefault(iter, 0.0);
        	double newRank = preRank + Double.parseDouble(rank);
        	// System.out.println("[MasterAppConfig]: GET /addrank, rank = " + rank + ", newRank = " + newRank);
        	ranks.put(iter, newRank);
        	// System.out.println("[MasterAppConfig]:  ranks = " + ranks);
        }
    }

	public String getRankSum(String iter) {
		String rankSum = String.valueOf(ranks.get(iter));
		// System.out.println("[MasterAppConfig]:  GET /getRank, iter = " +iter+", ranks = " + ranks);
		return rankSum;
	}

    
    
    /////////////////////////////////////// 
    ///////// Add Result
    ///////////////////////////////////////
    public void addResult(String ipAndPort, String key, String value) {

    	//key = key.substring(key.indexOf("_") + 1);
    	//value = value.substring(0, value.indexOf("-"));
        String s = "(" + key + ", " + value + ")\n";
        if (!reduceResults.containsKey(ipAndPort)) {
            List<String> results = new ArrayList<>();
            results.add(s);
            reduceResults.put(ipAndPort, results);
        } else {
            reduceResults.get(ipAndPort).add(s);
        }
    }
    
    
    public String getResForWorker(String ipAndPort) {
        List<String> kvs = reduceResults.get(ipAndPort);
        if (kvs == null) { return ""; }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String kv : kvs) {
            sb.append(kv);
            i++;
            if(i > 3) {
            	sb.append("... [only show 3 results]");
            	break;
            }
        }
        return sb.toString();
    }

    public synchronized void setJobReady(Boolean jobNotReady) {
        this.jobNotReady = jobNotReady;
    }

    public Boolean getJobReady() {
        return jobNotReady;
    }

    
    /////////////////////////////////////// 
    ///////// HashToBoltMapping 
    ///////////////////////////////////////
    
	public synchronized void  addReduceBoltMapping(BoltInfo boltInfo) {
		Integer hash = MasterAppConfig.hashToBoltDB.hashToBoltSize();
        System.out.println("[MasterAppConfig]: Before: this.hashToBoltDB.getHashToBoltMapping(). keySet=  " +  MasterAppConfig.hashToBoltDB.getHashToBoltMapping().keySet() + ", size = " + this.hashToBoltDB.hashToBoltSize());
        // System.out.println("[MasterAppConfig]:  this.hashToBoltDB.getIdMap()=  " +  this.hashToBoltDB.getMap().keySet());
		String hashStr = hash.toString();
		MasterAppConfig.hashToBoltDB.hashToBoltPut(hashStr, boltInfo);
        System.out.println("[MasterAppConfig]: After: AddReduceBoltMapping(),  hashStr =  " + hashStr + ", BoltId = " + boltInfo.getBoldId() + ", keySet =  " + MasterAppConfig.hashToBoltDB.keySet()+ ", size = " + this.hashToBoltDB.hashToBoltSize());
        
        
	}
	
	public synchronized void  cleanHashToBolt() {
        MasterAppConfig.hashToBoltDB.getHashToBoltMapping().clear();
        System.out.println("[MasterAppConfig]: cleanHashToBolt(), addReduceBoltMapping(), size =  " + MasterAppConfig.hashToBoltDB.getHashToBoltMapping().size());
	}


//	public BoltInfo getReduceBoltMapping(String hash) {
//		// System.out.println("[MasterAppConfig]: getReduceBoltMapping("+hash+") = " + this.reduceBoltMapping.get(hash));
//		System.out.println("[MasterAppConfig]: getSenderBoltMapping(),reduceBoltMapping " + reduceBoltMapping);
//		return this.reduceBoltMapping.get(hash);
//	}
	
	public Map<String,BoltInfo> getReduceBoltMapping() {
		return this.hashToBoltDB.getHashToBoltMapping();
	}
	
	
	
	
	
    /////////////////////////////////////// 
    ///////// HashToSenderBoltMapping 
    ///////////////////////////////////////
	

	
	
	
	
	
	
    /////////////////////////////////////// 
    ///////// HashToSenderBoltMapping 
    ///////////////////////////////////////
	public synchronized void addSenderBoltMapping(String myWorkerIdx, String toWorkerIdx, String listIdx) {
		String key = myWorkerIdx + "_" + toWorkerIdx;
		this.senderBoltMapping.put(key, listIdx);
		// System.out.println("[MasterAppConfig]: addSenderBoltMapping(),senderBoltMapping " + senderBoltMapping);

	}

	public String getSenderBoltMapping(String myWorkerIdx, String toWorkerIdx) {
		System.out.println("[MasterAppConfig]: getSenderBoltMapping(), senderBoltMapping " + senderBoltMapping);
		String key = myWorkerIdx + "_" + toWorkerIdx;
		return this.senderBoltMapping.get(key);
	}
	

	public String getListIdx(String hash, String sourceWorkerIdx) {
		// System.out.println("[getListIdx]: hash = " + hash + ", sourceWorkerIdx = " + sourceWorkerIdx);
		// System.out.println("[getListIdx]: hash = " + hash + ", db.hashToBolt keyset = " + MasterAppConfig.hashToBoltDB.getHashToBoltMapping().keySet() + ", sourceWorkerIdx = " + sourceWorkerIdx);
		BoltInfo boltInfo = hashToBoltDB.hashToBoltGet(hash);
        String toWorkerIdx = boltInfo.getWorkerIdx();
        
		// System.out.println("[MasterAppConfig]: getListIdx(): sourceWorkerIdx = " + sourceWorkerIdx + ", toWorkerIdx = " + toWorkerIdx);
		String listIdx = null;
		if(sourceWorkerIdx.equals(toWorkerIdx)) {
			listIdx =  boltInfo.getListIdx();
			// System.out.println("[MasterAppConfig]: getListIdx(): sourceWorkerIdx.equals(toWorkerIdx), listIdx = " + listIdx);
		} else {
			listIdx = getSenderBoltMapping(sourceWorkerIdx, toWorkerIdx);
			// System.out.println("[MasterAppConfig]: getListIdx(): !sourceWorkerIdx.equals(toWorkerIdx), listIdx = " + listIdx + "\n");
		}
		return listIdx;
	}

	
    /////////////////////////////////////// 
    ///////// ResMapping 
    ///////////////////////////////////////
	public void resPut(String key, String val) {
		MasterAppConfig.workerAddrsDB.resPut(key, val);	
	}
	
	
	public String resGet(String key) {
		return MasterAppConfig.workerAddrsDB.resGet(key);
	}
	
	
	
    /////////////////////////////////////// 
    ///////// WorkerAddrs 
    ///////////////////////////////////////

	public void writeWorkerAddrs() {
		String[] addrs = this.getWorkersArray();
		for (Integer i = 0 ; i < addrs.length; i++) {
			MasterAppConfig.workerAddrsDB.resPut("workerIdx_"+i.toString(), addrs[i]);
		}
	}
	
	public void cleanWorkerAddrs() {
		MasterAppConfig.workerAddrsDB.getResMap().clear();	
	}

	public String getAddrByIdxFromDB(int workerIdx) {
		String workerIdxStr = String.valueOf(workerIdx);
		return MasterAppConfig.workerAddrsDB.resGet("workerIdx_"+ workerIdxStr);
	}




}
