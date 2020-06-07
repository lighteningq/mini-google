package edu.upenn.cis455.crawler.test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import edu.upenn.cis455.crawler.CrawlerUtil;
import edu.upenn.cis455.crawler.DistributedCrawler;
import edu.upenn.cis455.crawler.SearchEngineDB;
import edu.upenn.cis455.crawler.UploaderS3;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;

public class TestCrawler {
	static List<String> ids;
	static DBWrapper db;

	public static void main(String[] args) {
		
//		SearchEngineDB se = new SearchEngineDB(2);
//	
//		db = se.dbMap.get(0);
//		System.out.println(db);
//		getDBInfo();
//		if(ids!=null) {
//		//	for(List<String> data :se.getQueryMetaData(ids).values()) {
//				for(String s: data)
//				System.out.print(s);
//				
//				System.out.println();
//			}
//		}
//
//		//testHttp("https://www.nytimes.com/");
////		try {
////			Document d = Jsoup.connect("http://crawltest.cis.upenn.edu").execute().parse();
////			testJsoup(d);
////		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		 
		
		//testFindURL();
		
		//System.out.print(UploaderS3.getDBIndex(CrawlerUtil.generateURLId("https://www.cnn.com/specials/us/extreme-weather")));
	}
	
	public static void testDC() {
		String seedUrl = "https://www.cnn.com";
		String storeDir = "./test";
		String maxFileSize = "2";
		String maxFileCrawl = "30";
		String[] abc = {seedUrl, storeDir,maxFileSize, maxFileCrawl};
		DistributedCrawler d = new DistributedCrawler(abc);
		System.out.println("poll from queue"+ d.queue.isEmpty());
	}
	
	public static void testHttp(String url) {
		URLInfo curInfo = new URLInfo(url);
		HttpsURLConnection conn;
		int len = 0;
		String type = "";
		
		try {
			conn  = (HttpsURLConnection) (new URL(url)).openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("User-Agent", "cis455crawler");
			conn.setRequestProperty("Connection", "close");
//			conn.setConnectTimeout(3000);
//			conn.setReadTimeout(3000);
			conn.connect();
			String content = conn.getContent().toString();
			len = conn.getContentLength();
			type = conn.getContentType();
			System.out.println("response code: "+ conn.getResponseCode());
			System.out.println("response:"+conn.getHeaderField("Location"));
			System.out.println("content type is: "+type);
			System.out.println("content length is: "+len);
			conn.disconnect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void getDBInfo() {
	//	DBWrapper db = new DBWrapper("./crawler_001");
		ids = db.test();
		//db.shutdownDB();
		//db.retrieveQueue();
	}
	
	public static void testJsoup(Document d) {
		try {

			for (Element link : d.select("a")) {
				String absHref = link.attr("abs:href");
				if (absHref != null &&!absHref.equals("") && !absHref.equals("\n"))
					System.out.println(absHref);
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void testFindURL() {
		String[] urls = {"https://us.cnn.com", "https://www.cnn.com/travel/videos","https://www.cnn.com/health"
				,"https://www.cnn.com/travel/news"
				,"https://www.cnn.com/business"
				,"https://www.cnn.com/us"};
		String[] urls2 = {"https://en.wikipedia.org/wiki/Australia"
				,"https://en.wikipedia.org/wiki/New_Spain"
				,"https://www.espncricinfo.com/ci/content/video_audio/1222358.html"
				,"http://www.gnu.org/server/standards/README.translations.html"
				,"https://www.cnn.com/2020/01/17/cnn-underscored/phonesoap-pro-review/index.html"
				,"https://cnnespanol.cnn.com/2020/01/10/hbo-estrena-the-outsider-basada-en-el-libro-de-stephen-king/"};
		DBWrapper db0 = new DBWrapper("./crawler_000");
		DBWrapper db1 = new DBWrapper("./crawler_001");
		DBWrapper db2 = new DBWrapper("./crawler_002");
		DBWrapper db3 = new DBWrapper("./crawler_003");

		
		for(String s: urls2) {
			System.out.println(s);
			String id = CrawlerUtil.generateURLId(s);
			if(db0.containDoc(id)) {
				System.out.println("document is in db0");
				db0.shutdownDB();
			}else {
				
				if(db1.containDoc(CrawlerUtil.generateURLId(s))) {
					
					System.out.println("document is in db1");
					//db1.shutdownDB();
				}else 
					if(db2.containDoc(CrawlerUtil.generateURLId(s))) {
					System.out.println("document is in db2");
				}else if(db3.containDoc(CrawlerUtil.generateURLId(s))) {
					System.out.println("document is in db3");
				}
						else System.out.println("documentDNE");
			} 
		}
		
		
	}
	
	
//	public static boolean db1retrieve(String id) {
//		if(db1.containDoc(id)) {
//			return true;
//		}
//		return false;
//	}

}
