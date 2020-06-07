package edu.upenn.cis455.mapreduce;

import java.io.IOException;

public interface PRJob extends Job {
  
  void addRank(double rank, String iter) throws IOException;

  Double getRank(Integer iter) throws IOException;
  
}
