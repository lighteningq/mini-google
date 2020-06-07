package edu.upenn.cis455.storage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.crawler.CrawlerUtil;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;

@Entity
public class RobotTxtEntry {
	@PrimaryKey
	private String uri;
	private byte[] info;
	
	public String getURI() {
		return uri;
	}
	
	public byte[] getRobotsInfo() {
		
		return info;
	}
	
	public void setURI(String uri) {
		this.uri = uri;
	}
	
	
	public void setRobotsTxtInfo(byte[] i) {
		
			info = i;
	}
	
	
	
}
