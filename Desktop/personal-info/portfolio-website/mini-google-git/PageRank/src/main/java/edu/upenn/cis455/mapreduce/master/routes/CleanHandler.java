//package edu.upenn.cis455.mapreduce.master.routes;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.net.HttpURLConnection;
//import java.net.MalformedURLException;
//import java.net.URL;
//
//import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
//import edu.upenn.cis455.mapreduce.master.MasterApp;
//import spark.Request;
//import spark.Response;
//import spark.Route;
//
///*
// * Handle GET /clean
// */
//public class CleanHandler implements Route {
//    private MasterAppConfig master;
//
//    public CleanHandler(MasterAppConfig master) {
//        this.master = master;
//    }
//
//    @Override
//    public Object handle(Request req, Response res) {
//        System.out.println("[CleanRoute]: GET /clean");
//        
//        
//        String[] workerAddrs = master.getWorkersArray();
//        try {
//            for (String workerAddr : workerAddrs) {
//            	sendCleanRequest(workerAddr);
//            }      
//        } catch (Exception e) {
//            return e.getMessage();
//        }
//        // reset master
//        master.reset();
//        res.redirect("/status");
//        
//        return "Clean master and workers";
//    }
//    
//    /*
//     * send clean request to all workers. for debugging.
//     */
//    private void sendCleanRequest(String worker) throws MalformedURLException, IOException {
//        String dest = "http://" + worker + "/clean";
//        URL url = new URL(dest.toString());
//        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
//        conn.setRequestMethod("POST");
//        conn.connect();
//        printResponse(conn);
//    }
//    
//    /*
//     * print the response
//     */
//    private void printResponse(HttpURLConnection httpURLConnection) throws IOException {
//        StringBuilder builder = new StringBuilder();
//        builder.append(httpURLConnection.getResponseCode())
//                .append(" ")
//                .append(httpURLConnection.getResponseMessage())
//                .append("\n");
//       System.out.println(builder);
//    }
//
//}