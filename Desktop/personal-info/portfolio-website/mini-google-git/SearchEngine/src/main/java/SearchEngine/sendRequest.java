package SearchEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class sendRequest implements Runnable{
	ResultEntry resultentry;
	ArrayList<ResultEntry> topK;
	public sendRequest(ResultEntry entry, ArrayList<ResultEntry> topK) {
		this.resultentry = entry;
		this.topK = topK;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("start : " + this.resultentry.URL);
		Document doc = null;
		try {
			doc = Jsoup.connect(this.resultentry.URL).get();
			
			String title = doc.title();
			if(title != null) this.resultentry.title = title;
			Elements metas = doc.head().select("meta");
			for (Element meta : metas) {  
				String content = meta.attr("content");  
				if ("keywords".equalsIgnoreCase(meta.attr("name"))) {  
	                this.resultentry.digest = content;
	                
	            } 
				if ("description".equalsIgnoreCase(meta.attr("name"))) {  
					 this.resultentry.digest = content; 
	            }  
				
			}
			System.out.println(this.resultentry.digest);
			this.topK.add(this.resultentry);
		} catch (IOException e) {
			System.out.println("error");
			e.printStackTrace();
		}
	        
	}

//	@Override
//	public void run() {
//		Document d;
//		ResultEntry entry = null;
//		String url = null;
//		url = entry.URL;
//		try {
//			d = Jsoup.connect(url)
//					.header("User-Agent", "cis455crawler")
//					.header("Connection", "close")
//					.method(Connection.Method.GET)
//					.timeout(100)
//					.execute().parse();
//			this.resultentry.digest = d.text().substring(0,200);
//			this.topK.add(this.resultentry);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//	}

}
