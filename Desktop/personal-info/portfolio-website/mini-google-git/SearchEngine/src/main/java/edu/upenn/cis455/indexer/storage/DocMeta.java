package edu.upenn.cis455.indexer.storage;

import java.util.ArrayList;


/**
 * A class to record the meta data according to a word in a specific doc.
 * @param Word
 */
public class DocMeta {
	
	private String word;
	private String docID;
	private double TF;
	private ArrayList<Integer> positions = new ArrayList<>();
	
	public DocMeta() {
	}
	
	public DocMeta(String word, String docID) {
		this.word = word;
		this.docID = docID;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getDocID() {
		return docID;
	}

	public void setDocID(String docID) {
		this.docID = docID;
	}

	public double getTF() {
		return TF;
	}

	public void setTF(double tF) {
		this.TF = tF;
	}

	public ArrayList<Integer> getPositions() {
		return positions;
	}

	public boolean putPosition(int pos) {
		this.positions.add(pos);
		return true;
	}
	
	
	
}
