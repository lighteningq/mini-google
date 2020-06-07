package edu.upenn.cis455.crawler.handler;


/**
 * Filter Out Invalid URLs and
 * @author Jingwen Qiang
 *
 */
public class URLFilterHandler {
	
	
	public static String filter(String url) {
		String res = "";
		if( isMailTo(url) || noOtherType(url))return null;
		res = removeQueryString(url);
		res = removeHashTag(res);
		return res;
	}
	
	private static String removeQueryString(String url) {
		String[] split = url.split("\\?");
		return split[0];
	}
	
	private static String removeHashTag(String url) {
		String[] split = url.split("#");
		return split[0];
	}
	
	private static boolean isMailTo(String url) {
		if(url.startsWith("mailto")) return true;
		return false;
	}
	
	private static boolean noOtherType(String url) {
		if(url.endsWith(".gif") || url.endsWith(".jpg") || url.endsWith(".png")|| url.endsWith(".pdf") )return true;
		else return false;
	}
	
	
}
