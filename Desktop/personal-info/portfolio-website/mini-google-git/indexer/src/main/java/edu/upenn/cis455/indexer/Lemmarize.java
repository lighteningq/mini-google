package edu.upenn.cis455.indexer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Parse the query using Standford NLP simple API If stoplist words or numbers
 * are found, append them to extra list Enable searching on stoplist
 */
public class Lemmarize {

	public static String removeStopWords(String query) {
		StringBuilder queryList = new StringBuilder("");
		Pattern pan2 = Pattern.compile("[a-zA-Z]+");
		Pattern pan = Pattern.compile("^[a-zA-Z0-9]+[.@&-]*[a-zA-Z0-9]+");

		Matcher m, m2;
		int querySize = 0;

//		List<String> lemmas = sen.lemmas();
//		String[] lemmas = 
//		for (String w : lemmas) {
//			w = w.trim();
//			m = pan.matcher(w);
//			m2 = pan2.matcher(w);
//			if (m.matches()) {
//				if (m2.find()) {
//					if (!w.equalsIgnoreCase("-rsb-") && !w.equalsIgnoreCase("-lsb-") && !w.equalsIgnoreCase("-lrb-")
//							&& !w.equalsIgnoreCase("-rrb-") && !w.equalsIgnoreCase("-lcb-")
//							&& !w.equalsIgnoreCase("-rcb-")) {
//						w = w.toLowerCase();
//						if (!IndexerTest.STOPWORDS.contains(w)) {
//							// not stop word
//							w = w.toLowerCase();
//							queryList.append(w).append(" ") ;
//							querySize++;
//						}
//					}
//				}
//			}
//		}
//  System.out.println(queryList);
		return queryList.toString();
	}


}