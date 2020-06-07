package edu.upenn.cis455.crawler.handler;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.crawler.workerServer.WorkerServer;
import edu.upenn.cis455.storage.DBWrapper;

public class RobotHandler {

	
	static Logger log = Logger.getLogger(RobotHandler.class);
	DBWrapper db = WorkerServer.getCrawler().getDB();
	InputStream in;
	RobotsTxtInfo rbt;

	/**
	 * Send a header request to for politeness check
	 * 
	 * @param url
	 * @return true if pass, false otherwise
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean headerCheck(String url) {
		log.debug("Header Check for URL: "+ url);
		URLInfo curInfo = new URLInfo(url);
		if (url.startsWith("https://")) {
			HttpsURLConnection conn;
			int len;
			String type;
			try {
				conn = (HttpsURLConnection) (new URL(curInfo.getProtocol()+curInfo.getHostName()+curInfo.getFilePath())).openConnection();
				conn.setRequestMethod("HEAD");
				conn.setRequestProperty("User-Agent", "cis455crawler");
				conn.setRequestProperty("Connection", "close");
				conn.setConnectTimeout(WorkerServer.HTTP_TIMEOUT);
				conn.setReadTimeout(WorkerServer.READFILE_TIMEOUT);
				
				/** handle redirection **/
				int status = conn.getResponseCode();
				if(status==301) {
					log.info("redirect in header "+ url);
					String location = conn.getHeaderField("Location");
					if(location!=null) {
						WorkerServer.getCrawler().queue.pushURLToQueue(location);
					}else {
						location = conn.getHeaderField("location");
						if(location!=null)WorkerServer.getCrawler().queue.pushURLToQueue(location);
					}
					conn.disconnect();
					return false;
				}
				
				len = conn.getContentLength();
				type = conn.getContentType();
				conn.disconnect();
			} catch (MalformedURLException e) {
				log.debug("URL Format Problem\n"+ExceptionUtils.getStackTrace(e));
				e.printStackTrace();
				return false;
			} catch (IOException e) {
				log.debug("IO exception in HTTP Request\n"+ExceptionUtils.getStackTrace(e));
				e.printStackTrace();
				return false;
			}

			// check len and type
			if (len > WorkerServer.getCrawler().maxDocSize || !isValidType(type)) {
				log.debug("larger than doc size or is not a valid type ... failed header check" + "  || size is:"+ len+ " || type is : "+type + "|| url is: "+ url);
				return false;
			}
				


		} 
		
		
		else { // http
			HttpURLConnection conn;
			log.debug("HostName is: "+ curInfo.getHostName()+ " | Port is "+ curInfo.getPortNo());
 
			int len=0;
			String type="";
			try {
				conn  = (HttpURLConnection) (new URL(curInfo.getProtocol()+curInfo.getHostName()+curInfo.getFilePath())).openConnection();
				conn.setRequestMethod("HEAD");
				conn.setRequestProperty("User-Agent", "cis455crawler");
				conn.setRequestProperty("Connection", "close");
				conn.setConnectTimeout(WorkerServer.HTTP_TIMEOUT);
				conn.setReadTimeout(WorkerServer.READFILE_TIMEOUT);
				conn.connect();
				
				
				/** handle redirection **/
				int status = conn.getResponseCode();
				if(status==301) {
					log.info("redirect in header check "+ url);
					String location = conn.getHeaderField("Location");
					if(location!=null) {
						WorkerServer.getCrawler().queue.pushURLToQueue(location);
					}else {
						location = conn.getHeaderField("location");
						if(location!=null)WorkerServer.getCrawler().queue.pushURLToQueue(location);
					}
					conn.disconnect();
					return false;
				}
				len = conn.getContentLength();
				type = conn.getContentType();
				conn.disconnect();
			}catch (IOException e1) {
				// TODO Auto-generated catch block
				log.info(ExceptionUtils.getStackTrace(e1));
				e1.printStackTrace();
			}


			log.debug("successfully sending head request......."+url);
			if (len > WorkerServer.getCrawler().maxDocSize || !isValidType(type)) {
				log.debug("larger than doc size or is not a valid type ... failed header check" + " || type is : "+type + "|| url is: "+ url);
				return false;
			}
				
			
			return true;
		}

		return true;

	}
	
	
//	/**
//	 * Parse the robots.txt file info
//	 * @throws IOException
//	 */
//	private void parseInputStream() throws IOException{
//		if(in != null){
//			rbt = new RobotsTxtInfo();
//			BufferedReader br = new BufferedReader(new InputStreamReader(in));
//			String s;
//			while((s = br.readLine()) != null){
//				s = s.toLowerCase();
//				if(s.toLowerCase().equals("user-agent: *")) break;
//				else if(s.toLowerCase().equals("user-agent: cis455crawler")){
//					break;
//				}
//			}
//			
//			while((s = br.readLine()) != null){
//				s = s.toLowerCase();
//			    if(s.startsWith("disallow: ")){
//			    	rbt.addUserAgent("cis455crawler");
//			    	rbt.addDisallowedLink("cis455crawler", s.substring(10));
//			    }
//			    else if(s.startsWith("allow: ")){
//			    	rbt.addAllowedLink("cis455crawler", s.substring(7));
//			    }
//			    else if(s.startsWith("crawl-delay")){
//			    	rbt.addCrawlDelay("cis455crawler", Integer.parseInt(s.substring(13)));
//			    }
//			    else if(s.startsWith("user-agent")) break;    // reach the end of this agent info
//			}
//			
//			while(s != null){
//				s = s.toLowerCase();
//				if( s.startsWith("user-agent: cis455crawler") ) {
//					disallowList.clear();
//					allowList.clear();
//					crawlDelay = 0;
//					while((s = br.readLine()) != null){
//						s = s.toLowerCase();
//						if(s.startsWith("disallow: ")){
//					    	disallowList.add(s.substring(10));
//					    }
//					    else if(s.startsWith("allow: ")){
//					    	allowList.add(s.substring(7));
//					    }
//					    else if(s.startsWith("crawl-delay")){
//					    	crawlDelay = Integer.parseInt(s.substring(13));
//					    }
//					    else if(s.startsWith("user-agent")) break;
//					}
//				}
//				if(s == null) break;
//				s = br.readLine();
//			}
//
//		}
//	}

	
	
	private void parseInputStream() throws IOException{
		
		String text = null;
		StringBuilder sb = new StringBuilder();
		int ret;
		while ((ret = in.read()) != -1)
			sb.append((char) ret);
		text = sb.toString();
		
		rbt = new RobotsTxtInfo();
		String[] robotRaw = text.split("\n");
		
		String agent ="";
		for (int i = 0; i<robotRaw.length; i++) {
			String s = robotRaw[i].toLowerCase();
				if (s.startsWith("user-agent: " )) {
					agent = s.split(" ")[1];
					rbt.addUserAgent(agent);
				}
				else if (s.startsWith("disallow: ") )
							rbt.addDisallowedLink(agent, s.substring(10));
				else if (s.startsWith("crawl-delay: ")) {
					int delay = 0;
					try {
						delay = (int) Double.parseDouble(s.substring(13).split(" #\",.'")[0].trim());
					}catch(Exception e) {
						e.printStackTrace();
					}
					
					rbt.addCrawlDelay(agent, delay);
				}else if(s.startsWith("allow: ")) {
					rbt.addAllowedLink(agent, s.substring(7));
				}
					

		}
		
		
	}

	/**
	 * parse and check robot.txt for politeness
	 * 
	 * @param url
	 * @return true => can proceed, false otherwise
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public boolean robotTxtCheck(String url) throws MalformedURLException{
		URLInfo curURL = new URLInfo(url);
		
		String cur = "https://" + curURL.getHostName() + "/robots.txt";
		
		if (!db.containsRobot(cur)) {
			log.debug("(NEW) ROBOT CHECK URL is: "+ cur);
			HttpsURLConnection conn;
			try {
				conn = (HttpsURLConnection) new URL(cur).openConnection();
				conn.setRequestMethod("GET");
				in = conn.getInputStream();
				if(in!=null)parseInputStream();
				

			} catch (MalformedURLException e) {
				rbt = new RobotsTxtInfo();
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.debug("Robot File not Found\n"+ExceptionUtils.getStackTrace(e));
				rbt = new RobotsTxtInfo();
				e.printStackTrace();
			}
			
			

			db.putRobotTxt(rbt, curURL.getHostName());
		    

		} else {
			//System.out.println("Robot Check has already been done for: "+ curURL.getHostName());
			rbt = db.getRobotFromURL(url);
		}

		if(rbt!=null) {
			ArrayList<String> disAllowed;
			ArrayList<String> allowed;
			Integer delay = 0;
			// check for cis455crawler always
			if (rbt.containsUserAgent("cis455crawler")) {
				disAllowed = rbt.getDisallowedLinks("cis455crawler");
				allowed = rbt.getAllowedLinks("cis455crawler");
				System.out.println("allowed: "+allowed+" and disallowed: "+disAllowed);
				if(disAllowed!=null) {
				for (String s : disAllowed) {
					if (curURL.getFilePath().startsWith(s)) {
						return false;
					}
				}
				}
				if(allowed!=null) {
					for(String s: allowed) {
						if (curURL.getFilePath().startsWith(s)) return true;
					}
				}
				if (rbt.crawlContainAgent("cis455crawler") && delay!=null) {
					delay = rbt.getCrawlDelay("cis455crawler");
					System.out.println("last accessed time is : " + db.getDocLastAccessTime(url));
					if(db.getDocLastAccessTime(url)==-1) return true;
					else if (db.getDocLastAccessTime(url) - System.currentTimeMillis() < (long) delay * 1000)
						return false;
				}
				return true;

			} else if (rbt.containsUserAgent("*")) {
				disAllowed = rbt.getDisallowedLinks("*");
				allowed = rbt.getAllowedLinks("*");
				if(disAllowed!=null) {
					for (String s : disAllowed) {
						if (curURL.getFilePath().startsWith(s)) {
							return false;
						}
					}
				}
				if(allowed!=null) {
					for(String s: allowed) {
						if (curURL.getFilePath().startsWith(s))  return true;
					}
				}

				
				
				return checkDelay(url);

			}
		}


		return true;

	}
	
// ONLY UPDATED with hostName delay time
	public boolean checkDelay(String url) {
		URLInfo urlinfo = new URLInfo(url);
		String hostName = urlinfo.getHostName();
		if(hostName == null) return true;
		
		if(db.containsRobot(hostName)) {
			if(db.getCrawlDelay(hostName)==0) return true;
			long lastAccessTime = db.getDocLastAccessTime(url);
			if(lastAccessTime ==-1) return true;
			if(System.currentTimeMillis() - lastAccessTime >= db.getCrawlDelay(hostName)* 1000) {
				db.updateVisitTime(hostName);
				return true;
			}else {
				return false;
			}
		}
		return true;
	}

	
	
	private boolean isValidType(String type) {
		return isHTML(type) || isXML(type);
	}

	public boolean isHTML(String type) {
		if(type==null) return false;
		type = type.split(";")[0].trim();
		if (type.startsWith("text/html"))
			return true;
		else
			return false;
	}

	public boolean isXML(String type) {
		if(type==null) return false;
		type = type.split(";")[0];
		if (type.endsWith("+xml") || type.equals("application/xml") || type.endsWith("text/xml"))
			return true;
		else
			return false;
	}
}
