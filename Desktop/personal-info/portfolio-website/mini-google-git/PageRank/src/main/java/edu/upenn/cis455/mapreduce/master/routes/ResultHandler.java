package edu.upenn.cis455.mapreduce.master.routes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import edu.upenn.cis455.mapreduce.master.MasterApp;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Handle GET /addresult
 */
public class ResultHandler implements Route {
    private MasterAppConfig master;

    public ResultHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        String ip = req.ip() + ":" + req.queryParams("port");
        String key = req.queryParams("key");
        String result = req.queryParams("result");

        System.out.println("[ResultHandler]:  " + ip + " : [ " + key + ", "+ result + " ]");
        
        // write to masterOutput.txt in master node.
        String kvpair = key + "," + result + "\n";       
        BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(MasterApp.masterOutputFile, true));
			writer.append(kvpair);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
        
		// write to user interface.
        if (key != null && result != null) {
        	master.addResult(ip, key, result);
        }
        return "Result added to master";
    }

}