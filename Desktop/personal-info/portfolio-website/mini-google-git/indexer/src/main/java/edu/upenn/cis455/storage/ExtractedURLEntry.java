package edu.upenn.cis455.storage;

import java.util.*;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.storage.CrawlerUtil;

@Entity
public class ExtractedURLEntry {	
	@PrimaryKey
	private String id;
	private String url;
	private List<String> extractedURL;
	
	public String getUrl() {
		return url;
	}
	
	
	public void addUrl(String url) {
		this.id = CrawlerUtil.generateURLId(url);
		this.url = url;
	}
	
	
	
	public void setExtractedLinks(List<String> links) {
		extractedURL= links;
	}
	public List<String> getExtractedLinks() {
		return extractedURL;
	}
}
