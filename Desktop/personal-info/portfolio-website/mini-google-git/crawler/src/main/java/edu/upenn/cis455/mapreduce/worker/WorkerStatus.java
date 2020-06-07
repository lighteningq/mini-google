package edu.upenn.cis455.mapreduce.worker;
import java.util.*;

public class WorkerStatus {
	public enum status{idle, waiting, mapping, reducing}
	
	private static int keysRead = 0;
	private static int keysWritten = 0;
	private static status s = status.idle;
	private static String job = null;
	private static List<String> results = new ArrayList<>();
	
	
	public synchronized List<String> getResults() {
		return results;
	}
	public synchronized void setResults(List<String> r) {
		WorkerStatus.results = r;
	}
	public synchronized int getKeysRead() {
		return keysRead;
	}
	
	public synchronized void addKeysRead() {
		WorkerStatus.keysRead++;
	}
	
	public synchronized void addKeysWrite() {
		WorkerStatus.keysWritten++;
	}
	
	public synchronized void addResult(String res) {
		WorkerStatus.results.add(res);
	}
	public synchronized void setKeysRead(int keysRead) {
		WorkerStatus.keysRead = keysRead;
	}
	public synchronized int getKeysWritten() {
		return keysWritten;
	}
	public synchronized void setKeysWritten(int keysWritten) {
		WorkerStatus.keysWritten = keysWritten;
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
