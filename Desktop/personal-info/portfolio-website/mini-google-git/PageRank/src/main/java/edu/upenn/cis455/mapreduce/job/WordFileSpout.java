package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis.stormlite.spout.FileSpout;

public class WordFileSpout extends FileSpout {

	@Override
	public String getFilename() {
		// will not be used anymore
		return  "words.txt";
	}


}
