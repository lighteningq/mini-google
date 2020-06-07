package edu.upenn.cis.stormlite.mapreduce;

import edu.upenn.cis.stormlite.spout.FileSpout;

public class WordFileSpout extends FileSpout {

	@Override
	public String getFilename() {
		System.out.println("[WordFileSpout]: getFilename()");
		return  "words.txt";
	}

}
