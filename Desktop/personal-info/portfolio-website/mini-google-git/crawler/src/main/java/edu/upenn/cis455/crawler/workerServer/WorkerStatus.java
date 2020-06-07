package edu.upenn.cis455.crawler.workerServer;
import java.util.*;

public class WorkerStatus {
	public enum status{idle, waiting, mapping, reducing}
	
	private static int fileCrawled = 0;
	private static int urlProcessed = 0;
	private static int maxFile;
	private static status s = status.idle;
	private static String job = "crawler";
	private static List<String> results = new ArrayList<>();
	
	
	public synchronized List<String> getResults() {
		return results;
	}
	public synchronized void setResults(List<String> r) {
		WorkerStatus.results = r;
	}
	public synchronized int getFileProcessed() {
		return fileCrawled;
	}
	
	public synchronized void addFileProcessed() {
		WorkerStatus.fileCrawled++;
	}
	
	public synchronized void addKeysWrite() {
		WorkerStatus.urlProcessed++;
	}
	
	public synchronized long getAverageProcessTime() {
		if(fileCrawled==0) return 0;
		return (System.currentTimeMillis() - WorkerServer.startTime)/fileCrawled;
	}
	
	public synchronized void addResult(String res) {
		WorkerStatus.results.add(res);
	}
	public synchronized void setfileCrawled(int keysRead) {
		WorkerStatus.fileCrawled = keysRead;
	}
	public synchronized int getKeysWritten() {
		return urlProcessed;
	}
	public synchronized void setKeysWritten(int keysWritten) {
		WorkerStatus.urlProcessed = keysWritten;
	}
	public synchronized status getS() {
		return s;
	}
	public synchronized void setS(status s) {
		WorkerStatus.s = s;
	}
	public synchronized String getJob() {
		return job;
	}
	public synchronized void setJob(String job) {
		WorkerStatus.job = job;
	}
	
	
	
}
