package edu.upenn.cis455.crawler.handler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class MonitorClient {
	static Logger log = Logger.getLogger(MonitorClient.class);
	private static String monitorHost;
	public static InetAddress host;
	
	public MonitorClient(String hostName) {
		monitorHost = hostName;
		try {
			host = InetAddress.getByName(monitorHost);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			log.error("sending monitor error", e);
		}
	
	}
	
	/** Send UDP monitor for each HTTP request
	 * @param url
	 * @throws IOException
	 */
	public void sendUDPMonitor(String url) {
		DatagramSocket s;
		try {
			s = new DatagramSocket();
			byte[] data = ("jingwenq;" + url).getBytes();
			DatagramPacket packet = new DatagramPacket(data, data.length, host, 10455);
			s.send(packet);
			log.debug("sending monitoring packet......"+url);
			s.close();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			log.error("sending UDP monitor error", e2);
		}

	}
	
}
