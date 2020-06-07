package edu.upenn.cis455.storage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This Util Class Provides the Utility Functions to Crawler
 * 
 * @author Jingwen Qiang
 *
 */
public class CrawlerUtil {
	
	
	
	/**This function hash url to a unique ID
	 * @param url 
	 * @return id for URL
	 */
	public static String generateURLId(String url) {
		  MessageDigest md;
		        String signature = null;
		  try {
		   md = MessageDigest.getInstance("MD5");
		   md.update(url.getBytes(),0, url.length());
		   signature = new BigInteger(1, md.digest()).toString(16);
		   /*System.out.println("[CrawlerUtils]: Signature = "+ signature) ;*/
		  } catch (NoSuchAlgorithmException e) {
		   // TODO Auto-generated catch block
		   // logger.catching(Level.DEBUG, e);
		   //halt(500);
		  }
		  
		  return signature;
		 }
	
	
	public static byte[] objToByte(Object obj) throws IOException {
	    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	    ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
	    objStream.writeObject(obj);

	    return byteStream.toByteArray();
	}

	public static Object byteToObj(byte[] bytes) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
	    ObjectInputStream objStream = new ObjectInputStream(byteStream);

	    return objStream.readObject();
	}
}
