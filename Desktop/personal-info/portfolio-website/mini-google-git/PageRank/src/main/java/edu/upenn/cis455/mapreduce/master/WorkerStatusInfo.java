package edu.upenn.cis455.mapreduce.master;

import java.io.Serializable;

import edu.upenn.cis455.mapreduce.Workerstatus;


/*
 * store the information about the worker status
 */
public class WorkerStatusInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	/*
	 * worker ip
	 */
    protected String ip, job;
    /*
     * worker status: mapping, waiting, reducing, idle
     */
    protected Workerstatus status;
    /*
     * keys written, keys read.
     */
    protected int keysWritten, keysRead;
    
    /*
     * last checked time for a worker node, in milliseconds.
     */
    protected long lastCheckedTime = -1;
    
    protected long startTime;
    
    protected long runningTime;

    public WorkerStatusInfo(String ip, String status, String job, String keysRead,  String keysWritten, long lastCheckedTime) {
        this.ip = ip;
        this.job = job;
        this.status = parseOrIdle(status);
        this.keysRead = parseOrZero(keysRead);
        this.keysWritten = parseOrZero(keysWritten);
        if (this.lastCheckedTime == -1) {
        	this.startTime = lastCheckedTime;
        }
        this.lastCheckedTime = lastCheckedTime;
        this.runningTime = lastCheckedTime - startTime;
    }

    private int parseOrZero(String s) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return 0;
        }
    }

    public Workerstatus parseOrIdle(String s) {	
        try {
            return Workerstatus.valueOf(s);
        } catch (IllegalArgumentException e) {
            return Workerstatus.IDLE;
        }
    }


    public String getIp() {
        return ip;
    }

    public int getKeysRead() {
        return keysRead;
    }
    
    public int getRunningTime() {
        return (int) (runningTime/1000);
    }
    
    public String getJobName() {
    	
        return job;
    }

    public int getKeysWritten() {
        return keysWritten;
    }

    public Workerstatus getStatus() {
        return status;
    }

	public long getLastChecked() {
		return this.lastCheckedTime;
	}
}
