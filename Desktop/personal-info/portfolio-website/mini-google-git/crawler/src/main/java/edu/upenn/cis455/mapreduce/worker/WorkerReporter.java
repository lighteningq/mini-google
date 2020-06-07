package edu.upenn.cis455.mapreduce.worker;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class WorkerReporter extends Thread{
	String port;
	String master;
	boolean stop = true;
	
	public WorkerReporter(String masterPort, String port){
		this.master = masterPort;
		this.port = port;
		stop = false;
		this.run();
	}
	



	@Override
	public void run(){
		while(!stop) {
			String url = "http://"+ master + "/workerstatus";
			String queryParams = "port=" + port 
							     +"&status=" + WorkerServer.ws.getS() 
							     +"&job=" + WorkerServer.ws.getJob() 
							     + "&keysRead=" + WorkerServer.ws.getKeysRead() 
							     +"&keysWritten=" + WorkerServer.ws.getKeysWritten();
			try {
				
				URL get_url = new URL(url + "?" + queryParams);
				
				System.out.println(get_url);
				
				HttpURLConnection urlConnection = (HttpURLConnection) get_url.openConnection();
				urlConnection.setRequestMethod("GET");
				if(urlConnection.getResponseCode()!=200) throw new RuntimeException("worker reporter error on worker port" + port);
				urlConnection.getResponseMessage();
			} catch (IOException e) {
				//e.printStackTrace();
				System.out.println("Worker Port: " + port + " Reporting status failed");
			}
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	

	
	public void shutdown(){
		stop = true;
		
	}
}
