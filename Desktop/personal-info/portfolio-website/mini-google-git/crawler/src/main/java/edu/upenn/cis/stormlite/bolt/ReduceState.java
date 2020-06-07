package edu.upenn.cis.stormlite.bolt;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.util.*;


@Entity
public class ReduceState {
	
	@PrimaryKey
	private String key;
	
	private List<String> list = new ArrayList<>();
	

	
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public List<String> getList() {
		return list;
	}

	public void setList(List<String> list) {
		this.list = list;
	}
	
	public void addList(String element){
		this.list.add(element);
	}
	
	public void removeList(String element){
		this.list.remove(element);
	}
}