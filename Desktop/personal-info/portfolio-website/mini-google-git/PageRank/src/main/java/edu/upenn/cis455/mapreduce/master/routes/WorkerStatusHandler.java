package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import edu.upenn.cis455.mapreduce.master.WorkerStatusInfo;
import spark.Request;
import spark.Response;
import spark.Route;

/*
 * Handle GET /workerstatus
 */
public class WorkerStatusHandler implements Route  {
    private MasterAppConfig master;

    public WorkerStatusHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        WorkerStatusInfo status = getStatus(req);
        master.refreshWorkerStatus(status);
        return "Worker status received by master";
    }

    /*
     * construct a worker status
     */
    private WorkerStatusInfo getStatus(Request req) {
        String ip = req.ip() + ":" + req.queryParams("port");
        String status = req.queryParams("status");
        String job = req.queryParams("job");
        String keysRead = req.queryParams("keysRead");
        String keysWritten = req.queryParams("keysWritten");
        //String runningTime = req.queryParams("runningTime");
        return new WorkerStatusInfo(ip, status, job, keysRead, keysWritten, System.currentTimeMillis());
    }
}
