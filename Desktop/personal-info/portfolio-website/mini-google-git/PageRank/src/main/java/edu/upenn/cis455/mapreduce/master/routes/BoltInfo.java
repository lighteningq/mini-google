package edu.upenn.cis455.mapreduce.master.routes;

import java.io.Serializable;

public class BoltInfo implements Serializable{
	String boltId;
	String myWorkerIdx;
	String listIdx;
	public BoltInfo(String boltId, String myWorkerIdx, String listIdx) {
		this.boltId = boltId;
		this.myWorkerIdx = myWorkerIdx;
		this.listIdx = listIdx;
	}
	
	public String getListIdx() {
		return this.listIdx;
	}
	
	public String getBoldId() {
		return this.boltId;
	}

	public String getWorkerIdx() {
		// TODO Auto-generated method stub
		return this.myWorkerIdx;
	}
	
}
