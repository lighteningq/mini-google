//package edu.upenn.cis.stormlite.spout;
//
//import java.util.*;
//
//public class TestSelfLoop {
//
//	public String removeSelfLoop(String url, String outUrls) {
//
//
//		
//		outUrls = outUrls.replace(url, "");
//		String[] splits = outUrls.split(spliter, -1);
//		StringBuilder sb = new StringBuilder();
//		for (int i = 0; i < splits.length; i++) {
//			if(splits[i].length() > 0) {
//				sb.append(splits[i]).append(spliter);;
//			}	
//		}
//		return sb.substring(0, sb.length()-3));
//	}
//		
//		
////		
////		int urlIdx = outUrls.indexOf(url);
////		String[] arr = outUrls.split(spliter);
////		Set<String> outUrlsSet = new HashSet<String>(Arrays.asList(arr));
////		System.out.println("outUrlsSet = " + outUrlsSet);
////		outUrlsSet.remove(url);
////		String[] outUrlsArr = outUrlsSet.toArray(new String[outUrlsSet.size()]);
////		if(urlIdx != -1) {
////			outUrls = outUrls.replace(url, "");
////			outUrls = outUrls.replace(spliter+spliter, "");
////			System.out.println("outUrls = " + outUrls);
////			
////		}
//
//}
