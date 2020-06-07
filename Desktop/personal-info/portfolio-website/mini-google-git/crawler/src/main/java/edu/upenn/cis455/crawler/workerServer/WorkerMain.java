package edu.upenn.cis455.crawler.workerServer;

import java.util.Map;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.UnknownHostException;
public class WorkerMain {
    public static void main(String[] args) {

      if (args.length != 3) {
        System.err.println("You need to provide: 1) the master's IP:port, 2) the path to the storage direcotry, and 3) the port number on which to listen for commands from the master.");
        System.exit(1);
      }
       String ip="";
    	// TODO: Start WorkerServer from this class based on the command-line arguments provided
      	try {
			ip = InetAddress.getLocalHost().toString();
			System.out.println("ip is :"+ip);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Map<String, String> config = new HashMap<String, String>();
        config.put("masterIpPort", args[0]);
        config.put("storageDir", args[1]);
     //   config.put("workerList", "["+ip+":"+args[2]+"]");
        config.put("workerList", "[localhost:"+args[2]+"]");
        config.put("workerIndex", "0");
        WorkerServer.createWorker(config);

    }
}
