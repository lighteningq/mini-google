package edu.upenn.cis455.mapreduce.job;

import java.util.Iterator;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	@Override
	public void map(String key, String value, Context context) {
		context.write(value, value);
	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		int i = 0;
		while (values.hasNext()) {
			i++;
			values.next();
		}
		context.write(key, String.valueOf(i));

	}

}
