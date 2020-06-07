package edu.upenn.cis455.mapreduce.job;

import java.util.Iterator;


import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;


public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
    // Your map function for WordCount goes here
//	  String[] words = value.split(" ");
//	  for(String word : words){
		  String word = key.trim();
		  if(!word.equals("")){
				context.write(key, "1");
				WorkerServer.ws.addKeysWrite();
		  }
//	  }

	
  }
  
  public void reduce(String key, Iterator<String> values, Context context)
  {
    // Your reduce function for WordCount goes here
	
	Integer count = 0;
	while(values.hasNext()) {
		count+=Integer.parseInt(values.next());
	}
	context.write(key, count.toString());
	WorkerServer.ws.addKeysWrite();
	WorkerServer.ws.addResult(key + ":"+ Integer.toString(count));

  }
  
}
