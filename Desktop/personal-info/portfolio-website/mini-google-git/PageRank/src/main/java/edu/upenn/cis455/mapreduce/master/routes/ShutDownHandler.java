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
 * Handle GET /shutdown
 */
public class ShutDownHandler implements Route  {

    MasterAppConfig master;

    public ShutDownHandler(MasterAppConfig master) {
        this.master = master;
    }

    /*
     * Handle GET /shutdown
     */
    @Override
    public Object handle(Request req, Response res) {
    	
        String[] workerAddrs = master.getWorkersArray();
        try {
            for (String workerAddr : workerAddrs) {
                sendShutDownRequest(workerAddr);
            }
            master.reset();
            return "Shuting down workers";
        } catch (Exception e) {
            return e.getMessage();
        }
        
        
    }
    
    /*
     * Send shutdown to request
     */
    private void sendShutDownRequest(String worker) throws MalformedURLException, IOException {
        String dest = "http://" + worker + "/shutdown";
        URL url = new URL(dest.toString());
        System.out.println(dest.toString());
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.connect();
        printResponse(conn);
    }
    
    /*
     * Print response
     */
    private void printResponse(HttpURLConnection httpURLConnection) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(httpURLConnection.getResponseCode())
                .append(" ")
                .append(httpURLConnection.getResponseMessage())
                .append("\n");
        System.out.println(sb);
    }
}
