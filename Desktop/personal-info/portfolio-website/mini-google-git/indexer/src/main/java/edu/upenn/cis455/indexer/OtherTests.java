package edu.upenn.cis455.indexer;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class OtherTests {

	public static void main(String[] args) {
		
		Map<String, Integer> map = new TreeMap<String, Integer>();
		map.put("abc", 1);
		map.put("bcd", 2);
		map.put("efg", 3);
		map.put("123", 4);
		map.put("cde", 5);
		Iterator it = map.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			System.out.println(key);
		}
	}

}
