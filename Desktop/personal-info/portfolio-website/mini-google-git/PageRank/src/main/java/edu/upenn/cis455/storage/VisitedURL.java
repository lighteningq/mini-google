package edu.upenn.cis455.storage;
import java.util.UUID;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class VisitedURL {	
	@PrimaryKey
	private String url;
	private long lastAccessTime;
	
	public String getUrl() {
		return url;
	}
	
	
	public void addUrl(String url) {
		this.url = url;
	}
	
	public void setLastAccessTime(long time) {
		lastAccessTime = time;
	}
	public long getLastAccessTime() {
		return lastAccessTime;
	}
}
