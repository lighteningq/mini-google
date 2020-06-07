package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/*
 * Handle /runjobs, send a signal to each workers to run job
 */
public class StartJobHandler implements Route  {

    MasterAppConfig master;

    public StartJobHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
    	
        String[] workerLinks = master.getWorkersArray();
        master.setJobReady(false);
        try {
            for (String worker : workerLinks) {
                sendRequest(worker);
            }
            return "Job Started";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

	/*
	 * send a signal to each workers to start runing.
	 */
    private void sendRequest(String worker) throws MalformedURLException, IOException {
        String dest = "http://" + worker + "/runjob";
        URL url = new URL(dest.toString());
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.connect();
        printResponse(conn);
    }

    private void printResponse(HttpURLConnection httpURLConnection) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append(httpURLConnection.getResponseCode())
                .append(" ")
                .append(httpURLConnection.getResponseMessage())
                .append("\n");
        System.out.println(builder);
    }
}
