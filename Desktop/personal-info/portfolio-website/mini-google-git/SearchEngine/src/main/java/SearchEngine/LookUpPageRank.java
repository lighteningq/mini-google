package SearchEngine;

import java.util.HashMap;

import edu.upenn.cis.stormlite.bolt.pagerank.DBWrapper;

public class LookUpPageRank {

	static String boltId = "123";

	public LookUpPageRank() {

	}

	public static Double getPageRank(String id) {

//		int hash = id.hashCode() % 10;
//		if(hash < 0) {
//			hash += 10;
//		}
//		DBWrapper db = new DBWrapper("./Worker0/res_7602c048-8bcf-4a26-b497-7e996ba337fd");
//		String rankStr = db.resGet(id);
		Double rank = 0.019;
//		if(rankStr != null) {
//			rank = Double.parseDouble(rankStr);
//		}
		
		return rank;
	}
}
