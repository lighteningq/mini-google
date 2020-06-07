package SearchEngine;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class getURL implements Runnable {
	ResultEntry resultEntry;
	
	String crawlerHost;
	public getURL(String crawlerHost,ResultEntry resultEntry) {
		this.resultEntry = resultEntry;
		this.crawlerHost = crawlerHost;
	}


	@Override
	public void run() {
		  
		  HttpURLConnection conn;
		  int len = 0;
		  String type = "";
		  
		  String url = crawlerHost +"/geturl?id="+ this.resultEntry.URLId;
		  InputStream is = null;
		  try {
		   conn  = (HttpURLConnection) new URL(url).openConnection();
		   conn.setConnectTimeout(100);
		   conn.setRequestProperty("User-Agent", "cis455crawler");
		   conn.setRequestProperty("Connection", "close");
		   conn.setRequestMethod("GET");
		   conn.connect();
		   
		   BufferedReader br = null;
		   if(conn.getResponseCode() == 200) {
			   is = new BufferedInputStream(conn.getInputStream());
		        br = new BufferedReader(new InputStreamReader(is));
		        String inputLine = "";
		        StringBuilder sb = new StringBuilder();
		        while ((inputLine = br.readLine()) != null) {
		            sb.append(inputLine);
		        }
		        
			   this.resultEntry.URL = sb.toString();
//			   System.out.println(this.resultEntry.URL);
			   br.close();
			   is.close();
			   conn.disconnect();
			   
			   Document doc = null;
				try {
					doc = Jsoup.connect(this.resultEntry.URL).
							timeout(500).
							get();
					
					String title = doc.title();
					if(title != null) this.resultEntry.title = title;
					Elements metas = doc.head().select("meta");
					for (Element meta : metas) {  
						String content = meta.attr("content");  
						if ("keywords".equalsIgnoreCase(meta.attr("name"))) {  
			                this.resultEntry.digest = content;
			                
			            } 
						if ("description".equalsIgnoreCase(meta.attr("name"))) {  
							 this.resultEntry.digest = content; 
			            }  
						
					}
			        searchEngine.temp.add(this.resultEntry);
			        this.resultEntry.finishJOB = true;
				} catch (IOException e) {
					e.printStackTrace();
				}
		   }else {
			   conn.disconnect();
		   }
		   
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  
		  

	 }
		
}
	
