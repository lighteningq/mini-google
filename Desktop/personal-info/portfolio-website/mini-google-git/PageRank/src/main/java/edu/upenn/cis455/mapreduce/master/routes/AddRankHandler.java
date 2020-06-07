package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Handle GET /addrank
 */
public class AddRankHandler implements Route {
    private MasterAppConfig master;

    public AddRankHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        String iter = req.queryParams("iter");
        String sinkRank = req.queryParams("rank");
        master.updateRank(iter, sinkRank);
        return "Rank received by master";
    }

}
