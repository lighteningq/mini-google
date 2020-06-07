package edu.upenn.cis455.storage;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.crawler.CrawlerUtil;
@Entity
public class MetaDataEntry {
	@PrimaryKey
	private String id;
	private String url;
	private String title;
    private byte[] chunkbody;
    
    
    public String getId() {
    	return id;
    }
    
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.id = CrawlerUtil.generateURLId(url);
		this.url = url;
	}

	public byte[] getChunkBody() {
		return chunkbody;
	}
	public void setContent(byte[] content) {
		this.chunkbody = content;
	}
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String t) {
		this.title = t;
	}
    
    
}
