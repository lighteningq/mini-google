package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis.stormlite.bolt.pagerank.DBWrapper;
import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Master Handle GET /addBoltId
 */
public class AddBoltIdHandler implements Route {
    private MasterAppConfig master;

    public AddBoltIdHandler(MasterAppConfig master) {
        this.master = master;

    }

    @Override
    public Object handle(Request req, Response res) {
    	System.out.println("[AddBoltHandler]: GET /addBoltId");
    	
        String isSender = req.queryParams("isSenderBolt");
        boolean isSenderBolt = Boolean.parseBoolean(isSender);
        String listIdx = req.queryParams("listIdx");
        String myWorkerIdx = req.queryParams("myWorkerIdx");
        
        if (isSenderBolt) {
        	String toWorkerIdx = req.queryParams("toWorkerIdx");
        	// System.out.println("[AddBoltHandler]: is senderBolt: listIdx = " + listIdx + ", myWorkerIdx=" + myWorkerIdx + ", toWorkerIdx=" + toWorkerIdx  + ", isSenderBolt=" + isSenderBolt);
        	master.addSenderBoltMapping(myWorkerIdx, toWorkerIdx, listIdx);
        } else {
        	String boltId = req.queryParams("boltId");
        	System.out.println("[AddBoltHandler]: not senderBolt: listIdx=" + listIdx + ", myWorkerIdx=" + myWorkerIdx  + ", boltId=" + boltId + ", isSenderBolt=" + isSenderBolt);
        	BoltInfo boltInfo= new BoltInfo(boltId, myWorkerIdx, listIdx);
        	master.addReduceBoltMapping(boltInfo);
        }
        
        

       
        return "Rank received by master";
    }

}
