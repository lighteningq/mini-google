package test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class sendRequest implements Runnable{
//	private BlockingQueue<String> queue;
	private String url; 
	public sendRequest(String url) {
//		this.queue = url;
		this.url = url;
	}
/**
 * soup.connect(url).header("User-Agent", "cis455crawler").timeout(1500).execute().parse();
 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
			Document doc = null;
		try {
//			System.out.println(url);
			doc = Jsoup.connect(url).get();
//					.header("User-Agent", "cis455crawler")
//					.header("Connection", "close")
//					.method(Connection.Method.GET)
//					.timeout(200)
//					.execute().parse();
//			System.out.println(d.text().substring(0,100));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String title = doc.title();
		Elements metas = doc.head().select("meta");
		for (Element meta : metas) {  
			String content = meta.attr("content");  
			if ("keywords".equalsIgnoreCase(meta.attr("name"))) {  
                System.out.println(content);  
            } 
			if ("description".equalsIgnoreCase(meta.attr("name"))) {  
                System.out.println(content);  
            }  
			
		}
		Elements keywords = doc.getElementsByTag("meta");
        System.out.println(title);
		
	}

}
