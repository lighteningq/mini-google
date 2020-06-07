package edu.upenn.cis455.crawler.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.crawler.workerServer.WorkerServer;

/**
 * 
 * This class processes URL and get back the document
 * content
 * 1. send HTTP/HTTPS request
 * 2. check different status and return document if is valid
 * @author Jingwen Qiang
 *
 */
public class URLProcessHandler {
	static Logger log = Logger.getLogger(URLProcessHandler.class);
	
	String type = "";
	int len;
	BufferedReader content;
	String location = "";
	int status;
	
	
	
	
	public Document processURL(String url) {
		Document d;
		if (url.startsWith("https://")) { // https
			d = processHTTPS(url);
		} else { // process http
			d = processHTTP(url);
			
		}
	 return d;
	}
	
	
	
	public String getDocType() {
		return type;
	}
	
	/** Process HTTP request for each URL
	 * @param url
	 * @return true if a new doc is put into DB, false otherwise
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	private Document processHTTP(String url) {

		// 1. send get request
		//System.out.println("HTTP: Sending Get Request to " + url + "...............");
		URLInfo cur = new URLInfo(url);
		HttpURLConnection conn = null;

	//	log.debug("HostName is: "+ curInfo.getHostName()+ " | Port is "+ curInfo.getPortNo());

		try {
			
			conn  = (HttpURLConnection) (new URL(url)).openConnection();
			conn.setRequestProperty("User-Agent", "cis455crawler");
			conn.setRequestProperty("Connection", "close");
			conn.setRequestMethod("GET");
			conn.setConnectTimeout(2000);
			conn.setReadTimeout(2000);
			conn.connect();
			len = conn.getContentLength();
			type = conn.getContentType();
			status = conn.getResponseCode();
			location = conn.getHeaderField("Location");
		//	content = new BufferedReader(new InputStreamReader((InputStream)conn.getContent()));
			
		}catch (IOException e1) {
			// TODO Auto-generated catch block
			//log.info(ExceptionUtils.getStackTrace(e1));
			//e1.printStackTrace();
		}

		
		// 2. read response and generate string
		log.debug("processing sending http:....."+url);

		// 2.1 process redirect , download if necessary
		if (status == 301 || status == 302) {
			
					log.debug("Redirecting location is: "+ location);
					if (!location.equals(" ")) {
					//	location = cur.getProtocol() + cur.getHostName() + location;
						WorkerServer.getCrawler().queue.pushURLToQueue(location);
						log.debug(location + " : Adding Redirecting Location:......."+location);
						return null;
					}
				
			
			// 2.2 process 400 and 500 response, skipping
		} else if (status / 100 == 4 || status / 100 == 5) {
			conn.disconnect();
			log.debug("400/500 response, skipping.......");
			return null;
		} // 2.3 else read in document and put into db
		else if (status == 200 || status == 304) { // 2 cases

			

			if (status == 200) {
				
				//System.out.println("number left:" +maxNumFile);
//				String line ="";
//				String doc ="";
				Document d;
				try {
//					while((line=content.readLine())!=null) {
//						doc+=line;
//					}
					d = Jsoup.connect(url).header("User-Agent", "cis455crawler").timeout(1500).execute().parse();
					conn.disconnect();
					return d;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//Document d = Jsoup.parse(doc, cur.getProtocol() + cur.getHostName() + cur.getFilePath());
				
				//collector.emit(new Values<Object>(url, content, status,type));

			} else {
				log.debug(url + ": Not Modified ........");
			}

		} else {
			log.debug("Invalid Response... Skipping");
		}
		conn.disconnect();
		return null;
	}
	
	
	/** Process HTTPS request and put documents into DB
	 *
	 * @param url
	 * @return true if a new document is generated, false otherwise
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private Document processHTTPS(String url) {
		int status=0;
		String redirectLink="";
		URLInfo cur = new URLInfo(url);
		log.debug("processing sending https:....."+url);
		HttpsURLConnection conn = null;
		try {
			conn = (HttpsURLConnection) (new URL(url)).openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("User-Agent", "cis455crawler");
			conn.setConnectTimeout(2000);
			conn.setReadTimeout(3000);
			type = conn.getContentType();
			status = conn.getResponseCode();
			redirectLink = conn.getHeaderField("Location");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (status == 301 || status == 302) {

			
			if (redirectLink != null) {
				log.debug(url + " : Redirecting.....");
				//WorkerServer.getCrawler().addToFrontierQueue(cur.getProtocol() + cur.getHostName() + redirectLink);
				conn.disconnect();
				return null;
			}
			conn.disconnect();
			return null;

		} else if (status / 100 == 4 || status / 100 == 5) {
			log.debug(url + " : Invalid Response 4xx or 5xx .....");
			conn.disconnect();
			return null;

		} else if (status == 200 || status == 304) {

//			
//			try {
//				content = new BufferedReader(new InputStreamReader((InputStream) conn.getContent()));
//			} catch (IOException e1) {
//				log.error("content reading error",e1);
//				e1.printStackTrace();
//			}
//			String line;
//			String doc="";
//			try {
//				while ((line = content.readLine()) != null) {
//					doc += line + "\n";
//				}
//				
//			} catch (IOException e) {
//				log.error("reading document error|"+url+"|  ",e);
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			conn.disconnect();



			if (status == 200) {
				Document d = null;
				try {
					d = Jsoup.connect(url).header("User-Agent", "cis455crawler").timeout(1500).execute().parse();
					return d;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//				Document d = Jsoup.parse(doc, cur.getProtocol() + cur.getHostName() + "/");
			
				//System.out.println("number left:" +maxNumFile);
				
			} else {
				log.debug(url + " : Not Modified.....");
				return null;
			}
			
					

		} else {
			System.out.println(url + " : Other Invalid Response... Skipping....");
		}
		
		conn.disconnect();
		return null;
	}
	
	
	
	

	
	

}


