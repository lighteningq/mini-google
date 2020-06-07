package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Handle GET /getListIdx
 */
public class GetListIdxHandler implements Route {
    private MasterAppConfig master;

    public GetListIdxHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
    	
        String hash = req.queryParams("hash");
        String addr = req.ip() + ":" + req.queryParams("port");
       
        String sourceWorkerIdx = master.getWorkerIdxByAddr(addr);
        if (sourceWorkerIdx == null) {
        	sourceWorkerIdx = "0";
        }
        System.out.println("[GetListIdxHandler]: GET /getListIdx received. hash = " + hash + ", addr = " + addr + ", sourceWorkerIdx = " + sourceWorkerIdx);
        
        String listIdx = master.getListIdx(hash, sourceWorkerIdx);
        
        System.out.println("[GetListIdxHandler]: GET /getListIdx received. listIdx = " + listIdx);
        return listIdx;
    }

}
