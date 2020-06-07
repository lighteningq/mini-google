package edu.upenn.cis.stormlite.bolt.pagerank;

import java.util.HashMap;

public class LookUpPageRank {

	static String boltId = "123";

	public LookUpPageRank() {

	}
	
	public static void main(String[] args) {
		getPageRank("123");
	}
	public static Double getPageRank(String id) {

		DBWrapper db = new DBWrapper("/Worker0");
		String rankStr = db.resGet(id);
		Double rank = Double.parseDouble(rankStr);
		return rank;
	}
}
