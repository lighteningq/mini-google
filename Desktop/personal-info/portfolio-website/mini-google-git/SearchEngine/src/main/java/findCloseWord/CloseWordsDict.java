package findCloseWord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import SearchEngine.searchEngine;



public class CloseWordsDict {
	static Logger log = Logger.getLogger(CloseWordsDict.class);
	public static Trie dict;
	
	/**
	 * load all the corresponding file into TrieTree
	 * suggestwords.txt
	 * cached queries
	 * indexer words
	 * stopwords
	 */
	public CloseWordsDict() {
		dict = new Trie();
		
		File suggestWordsFile = new File("./src/main/resources/public/suggestwords.txt");
		FileReader fr = null;
		BufferedReader bf = null;
    	try {
    		fr = new FileReader(suggestWordsFile);
    		bf = new BufferedReader(fr);
        	String oneline = null;
        	int counter = 0;
			while((oneline = bf.readLine()) != null) {   
				String s = oneline.trim().toLowerCase();
				dict.insert(s);
				searchEngine.frequencyMapping.put(s,1);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}  finally {
			try {
				fr.close();
				bf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info("finish loading dict for suggest words");
		}
    	
    	File stopwords = new File("./src/main/resources/public/stopwords.txt");
    	FileReader freader = null;
		BufferedReader br = null;
    	try {
    		freader = new FileReader(stopwords);
    		br = new BufferedReader(freader);
        	String oneline = null;
			while((oneline = br.readLine()) != null) { 
				String s = oneline.trim().toLowerCase();
				if(searchEngine.frequencyMapping.containsKey(s)) {
					searchEngine.frequencyMapping.put(s,2);
					continue;
				}
				dict.insert(s);
				searchEngine.frequencyMapping.put(s,1);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}  finally {
			try {
				freader.close();
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info("finish loading dict for stop words");
		}
    	
    	File cities = new File("./src/main/resources/public/city.txt");
		FileReader f = null;
		BufferedReader b = null;
    	try {
    		f = new FileReader(cities);
    		b = new BufferedReader(f);
        	String oneline = null;
			while((oneline = b.readLine()) != null) {   
				String s = oneline.trim().toLowerCase();
				if(searchEngine.frequencyMapping.containsKey(s)) {
					searchEngine.frequencyMapping.put(s,3);
					continue;
				}
				dict.insert(s);
				searchEngine.frequencyMapping.put(s,1);
				searchEngine.cities.add(s);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}  finally {
			try {
				f.close();
				b.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info("finish loading city ajax for weather api, size " + searchEngine.cities.size());
		}
    	
    	
    	File company = new File("./src/main/resources/public/company.txt");
    	FileReader fread = null;
		BufferedReader bread = null;
    	try {
    		fread = new FileReader(company);
    		bread = new BufferedReader(fread);
        	String oneline = null;
			while((oneline = bread.readLine()) != null) { 
				String s = oneline.trim().toLowerCase();
				if(searchEngine.frequencyMapping.containsKey(s)) {
					searchEngine.frequencyMapping.put(s,4);
					continue;
				}
				dict.insert(s);
				searchEngine.frequencyMapping.put(s,1);
				searchEngine.companies.add(s);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}  finally {
			try {
				fread.close();
				bread.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info("finish loading dict for yelp api" );
		}
    	
    	
    	
    	
	}
	public static Trie getTrie() {
		return dict;
	}
	
}
