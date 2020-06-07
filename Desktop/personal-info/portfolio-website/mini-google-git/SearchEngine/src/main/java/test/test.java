package test;

import java.util.HashMap;

public class test {
	private static HashMap<String, Double> pagerankParser(String pagerank) {
		HashMap<String, Double> URLIDtoPageRank = new HashMap<>();
		String[] URLPageRankPairs = pagerank.split(";");
		for(String kvPairs : URLPageRankPairs) {
			String url = kvPairs.substring(0,kvPairs.indexOf(':')).trim();
			String score = kvPairs.substring(kvPairs.indexOf('-')+1,kvPairs.length()).trim();
			Double pagerankScore = Double.valueOf(score);
			URLIDtoPageRank.put(url,pagerankScore);
		}
		return URLIDtoPageRank;
		
	}
	
	public static void main(String[] args) {
		String pagerankStr = "urlA: urlA-0.00012; urlB: urlB-0.11134;  urlC: urlC-0.000000056";
		HashMap<String,Double> mapping = pagerankParser(pagerankStr);
		for(String key : mapping.keySet()) {
			System.out.println(key);
			System.out.println(mapping.get(key));
		}
		
	}
}
