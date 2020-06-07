package edu.upenn.cis455.mapreduce.worker;

import java.util.Map;
import java.util.HashMap;

public class WorkerMain {
    public static void main(String[] args) {

      if (args.length != 3) {
        System.err.println("You need to provide: 1) the master's IP:port, 2) the path to the storage direcotry, and 3) the port number on which to listen for commands from the master.");
        System.exit(1);
      }

    	// TODO: Start WorkerServer from this class based on the command-line arguments provided

        Map<String, String> config = new HashMap<String, String>();
        String storage = args[1];
        if (!storage.endsWith("/")) {
        	storage+= "/";
        }
        config.put("masterIpPort", args[0]);
        config.put("storageDir", storage);
        config.put("workerList", "[localhost:"+args[2]+"]");
        config.put("workerIndex", "0");
        WorkerServer.createWorker(config);
    }
}
