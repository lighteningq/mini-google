//package edu.upenn.cis455.mapreduce.job;
//
//import java.util.Iterator;
//
//import edu.upenn.cis455.mapreduce.Context;
//import edu.upenn.cis455.mapreduce.Job;
//
//public class WordCount implements Job {
//
//	/**
//	 * This is a method that lets us call map while recording the StormLite source executor ID.
//	 * 
//	 */
//	@Override
//	public void map(String key, String value, Context context)
//	{
//		context.write(value, "1");
//	}
//
//	/**
//	 * This is a method that lets us call map while recording the StormLite source executor ID.
//	 * 
//	 */
//	@Override
//	public void reduce(int iteration, String key, Iterator<String> values, Context context)
//	{
//		
//		int counter = 0;
//		while (values.hasNext()) {
//			values.next();
//			counter ++;
//		}
//		context.write(key, String.valueOf(counter));
//	}
//
//}
