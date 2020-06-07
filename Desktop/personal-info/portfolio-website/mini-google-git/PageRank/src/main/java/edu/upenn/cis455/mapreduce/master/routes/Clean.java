package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;


public class Clean implements Route {
	private MasterAppConfig master;
	
	public Clean(MasterAppConfig master) {
		this.master = master;
	}

	@Override
	public Object handle(Request request, Response response) throws Exception {

		String workersNum = request.queryParams("workersNum");
		System.out.println("[AddWorkerNum]: GET /workersNum" + workersNum);

		MasterAppConfig.workerAddrsDB.resPut("workersNum", workersNum);
        this.master.writeWorkerAddrs();

		System.out.println("[AddWorkerNum]:master.resGet(\"workersNum\") " + master.resGet("workersNum") + ", getResMap() = " + MasterAppConfig.workerAddrsDB.getResMap());
		return "WorersNum added.";
	}

}
