package edu.upenn.cis455.storage;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.crawler.CrawlerUtil;
@Entity
public class DocEntry {
	@PrimaryKey
	private String id;
	
	private String url;
//	private long lastModified;
	private long lastAccessed = 0;
    private byte[] content;
    private String contentType;
    
    
    public String getId() {
    	return id;
    }
    
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.id = CrawlerUtil.generateURLId(url);
		this.url = url;
	}
	public long getLastAccessed() {
		return lastAccessed;
	}
	public void setLastAccessed(long lastAccessed) {
		this.lastAccessed = lastAccessed;
	}
//	public long getLastModifed() {
//		return lastModifed;
//	}
//	public void setLastModifed(long lastModifed) {
//		this.lastModifed = lastModifed;
//	}
	public byte[] getContent() {
		return content;
	}
	public void setContent(byte[] content) {
		this.content = content;
	}
    
    
}
