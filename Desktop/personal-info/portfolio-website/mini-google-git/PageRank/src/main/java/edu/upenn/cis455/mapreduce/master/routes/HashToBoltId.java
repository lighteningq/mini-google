package edu.upenn.cis455.mapreduce.master.routes;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis455.mapreduce.master.MasterAppConfig;

import spark.Request;
import spark.Response;
import spark.Route;

public class HashToBoltId implements Route {
	
	private MasterAppConfig master;
	Config hashToBoltId;
	
	public HashToBoltId(MasterAppConfig master) {
		this.master = master;
		this.hashToBoltId = new Config();
		


	}

	@Override
	public Object handle(Request request, Response response) throws Exception {
		
		Map<String, BoltInfo> reduceBoltMapping = this.master.getReduceBoltMapping();
		if (reduceBoltMapping != null) {
			for (String hash:  reduceBoltMapping.keySet()) {
				BoltInfo boltInfo = reduceBoltMapping.get(hash);
				this.hashToBoltId.put(hash, boltInfo.getBoldId());
			}
		}
		
		
        // send hashToBoltId to worker.
    	final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        String hashToBoltIdStr = null;
        try {
        	hashToBoltIdStr = om.writerWithDefaultPrettyPrinter().writeValueAsString(hashToBoltId);
        	System.out.println("[WorkerPageRank]:hashToBoltIdStr = " + hashToBoltIdStr);
		} catch (JsonParseException e1) {
			e1.printStackTrace();
		} catch (JsonMappingException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return hashToBoltIdStr;
	}

}
