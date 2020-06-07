package edu.upenn.cis455.storage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.storage.CrawlerUtil;

@Entity
public class RobotTxtEntry {
	@PrimaryKey
	private String uri;
	private byte[] info;
	
	public String getURI() {
		return uri;
	}
	
	public RobotsTxtInfo getRobotsInfo() {
		
		RobotsTxtInfo res = null;
		try {
			res = (RobotsTxtInfo)CrawlerUtil.byteToObj(info);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}
	
	public void setURI(String uri) {
		this.uri = uri;
	}
	
	public void setRobotsTxtInfo(RobotsTxtInfo i) {
		
		try {
			info = CrawlerUtil.objToByte(i);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
}
