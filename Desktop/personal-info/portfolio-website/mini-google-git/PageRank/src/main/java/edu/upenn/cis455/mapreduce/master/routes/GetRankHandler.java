package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Handle GET /getrank
 */
public class GetRankHandler implements Route {
    private MasterAppConfig master;

    public GetRankHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        String iter = req.queryParams("iter");
        String rankSum = master.getRankSum(iter);
        return rankSum;
    }

}
