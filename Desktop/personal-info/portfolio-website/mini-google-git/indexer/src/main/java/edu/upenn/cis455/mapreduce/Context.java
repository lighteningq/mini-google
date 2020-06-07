package edu.upenn.cis455.mapreduce;

import java.util.ArrayList;

public interface Context {

  void write(String key, String value);
  
  void write(String key, ArrayList<String> value);
  
}
