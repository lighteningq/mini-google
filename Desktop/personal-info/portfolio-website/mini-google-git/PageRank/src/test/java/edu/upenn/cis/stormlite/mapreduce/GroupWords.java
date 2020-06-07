//package edu.upenn.cis.stormlite.mapreduce;
//
//import java.util.Iterator;
//
//import edu.upenn.cis455.mapreduce.Context;
//import edu.upenn.cis455.mapreduce.Job;
//
///**
// * Sample class to implement both mapper and reducer.
// * The reducer just returns the key as key and value.
// * 
// * @author ZacharyIves
// *
// */
//public class GroupWords implements Job {
//	@Override
//	public void map(String key, String value, Context context) {
//		context.write(value, value);
//	}
//
//	@Override
//	public void reduce(String key, Iterator<String> values, Context context) {
//		context.write(key, key);
//	}
//
//}
