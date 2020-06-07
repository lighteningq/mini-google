package edu.upenn.cis455.storage;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.crawler.CrawlerUtil;



@Entity
public class URLHashEntry {
	@PrimaryKey
	private String id;
	private String url;
	
	public String getURL() {
		return url;
	}
	
	public void setURL(String s) {
		url = s;
		this.id = CrawlerUtil.generateURLId(s);
	}
	
	public String getId() {
		return id;
	}
}
