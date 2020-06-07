package edu.upenn.cis455.mapreduce;

/**
 * Context class as per Hadoop MapReduce -- used
 * to write output from the mapper / reducer
 * 
 * @author ZacharyIves
 *
 */
public interface Context {

	void write(String key, String value);
}
