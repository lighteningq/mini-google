package edu.upenn.cis455.mapreduce.master;

import java.io.File;
import java.io.IOException;

/*
 * Main entry for master node.
 */
public class MasterApp {

	static final long serialVersionUID = 455555001;
	public static final int masterPort = 8000;
	public static String masterOutputFile = "masterOutput.txt";
	public static String Store = "store/";
    public static String HashToBolt = "HashToBolt";
    public static String WorkersAddr = "WorkerAddrs";
    
    
	public static void registerStatusPage() {
	}

	/**
	 * The mainline for launching a MapReduce Master. This should handle at least
	 * the status and worker status routes, and optionally initialize a worker as
	 * well.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			File outputFile = new File(masterOutputFile);
			outputFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		new MasterAppConfig(masterPort);
	}

}
