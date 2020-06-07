package edu.upenn.cis455.indexer.storage;

import java.awt.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.management.loading.PrivateClassLoader;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class IndexEntry {
	@PrimaryKey
	private String word;
	private Map<String, ArrayList<String>> meta;
	// 0: title TF; 
	// 1: title locations "2 5"; 
	// 2: body normalized TF;
	// 3: body locations "2 5 16"
	
	public IndexEntry() {
		this.word = "";
	}
	
	public IndexEntry(String word) {
		this.word = word;
		// meta: <docID, docMeta>
		this.meta = new TreeMap<String, ArrayList<String>>();
	}
	
	/**
	 * add id and meta mapping into the index
	 * @param id
	 * @param idMeta
	 * @return
	 */
	public boolean putID(String id, ArrayList<String> docMeta) {
		try {
			meta.put(id, docMeta);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * get all meta data according to this word
	 * @return
	 */
	public Map<String, ArrayList<String>> getMeta(){
		return this.meta;
	}
	
	public String getWord() {
		return this.word;
	}
	
	
	
	/***********************************************
	 * API for retreive data
	 ***********************************************/
	
	public String getTitleTF(String docID) {
		if (docID==null||!meta.containsKey(docID)) {
			return null;
		}
		else return meta.get(docID).get(0);
	}
	
	public String getTitleLocations(String docID) {
		if (docID==null||!meta.containsKey(docID)) {
			return null;
		}
		else return meta.get(docID).get(1);
	}
	
	
	public String getBodyTF(String docID) {
		if (docID==null||!meta.containsKey(docID)) {
			return null;
		}
		else return meta.get(docID).get(2);
	}
	
	public String getBodyLocations(String docID) {
		if (docID==null||!meta.containsKey(docID)) {
			return null;
		}
		else {
			return meta.get(docID).get(3);
		}
	}
	
	public String[] getTitleLocationStrings(String docID){
		if (docID==null||!meta.containsKey(docID)) {
			return null;
		}
		else {
			return meta.get(docID).get(1).split(" ");
		}
	}
	
	public String[] getBodyLocationStrings(String docID){
		if (docID==null||!meta.containsKey(docID)) {
			return null;
		}
		else {
			return meta.get(docID).get(3).split(" ");
		}
	}
	public Iterator getDocIDIter() {
		return this.meta.keySet().iterator();
	}
	
}
