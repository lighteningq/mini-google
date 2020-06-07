package SearchEngine;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import edu.stanford.nlp.simple.Sentence;

/**
 * Parse the query using Standford NLP simple API
 * If stoplist words or numbers are found, append them to extra list
 * Enable searching on stoplist
 */ 
public class removeStopWords {
	final static String STOPLIST = "./src/main/resources/public/stopwords.txt";
//	private static Hashtable<String, Integer> stops = new Hashtable<>();
	private static HashSet<String> stops = new HashSet<>();
	public removeStopWords() {
		init();
	}
	public void init(){
        File stop = new File(STOPLIST);
        try {
        	Scanner sc = new Scanner(stop);
        	while (sc.hasNext()) {
        		String s = sc.nextLine().trim();
        		stops.add(s);
        	}
        	sc.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
	}
	public static String removeStopWords(String query) {
		String queryList = "";
		Pattern pan2 = Pattern.compile("[a-zA-Z]+");
		Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");
		Pattern pan = Pattern.compile("^[a-zA-Z0-9]+[.@&-]*[a-zA-Z0-9]+");
		
		Matcher m, m2, m3;
		int querySize = 0;	
		
//		Sentence sen = new Sentence(query);
//		List<String> lemmas = sen.lemmas();
		String[] splitwords = query.split("\\s+");
		for (String w: splitwords) {
			w = w.trim();	
			m = pan.matcher(w);
			m2 = pan2.matcher(w);
			m3 = pan3.matcher(w);
			if (m.matches()) {
				if (m2.find()){			
					if (!w.equalsIgnoreCase("-rsb-")&&!w.equalsIgnoreCase("-lsb-")
							&&!w.equalsIgnoreCase("-lrb-")&&!w.equalsIgnoreCase("-rrb-")
							&&!w.equalsIgnoreCase("-lcb-")&&!w.equalsIgnoreCase("-rcb-")){
						w = w.toLowerCase();
						if ( !stops.contains(w)) {
							// not stop word
							w = w.toLowerCase();
							queryList += w + " ";
							querySize++;
						} 
					}			
				}
			}
		}
		System.out.println(queryList);
		return queryList;
	}
	
	// tester
	public static void main(String[] args) {
//		String query = "test no";
		String query = "?? we'll Gridspot can link up idle computers instances across the world to provide large scale efforts with the computing power they require at affordable prices 0103 centsCPU hour These Linux instances run Ubuntu inside a virtual machine You are able to bid on access to these instances and specify the requirements of your tasks or jobs When your bid is fulfilled you can start running the instances using SSH anywhere youd like There are grant options available to defray costs for researchers and nonprofits The Gridspot API allows you to manage instances and identify new ones You can list available instances access them and stop the instances if you so choose Each API call requires an API key that can be generated from your account page";
//		String query = "I'll love you forever, babe";
		System.out.println(query);
		removeStopWords remove = new removeStopWords();
		String newquery =  remove.removeStopWords(query);
		System.out.println(newquery);
	}
	
	
}
