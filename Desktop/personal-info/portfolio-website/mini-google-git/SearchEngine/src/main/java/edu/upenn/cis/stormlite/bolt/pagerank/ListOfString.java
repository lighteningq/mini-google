package edu.upenn.cis.stormlite.bolt.pagerank;

import java.util.ArrayList;

/*
 * Define a class that are same as ArrayList<String>, since BerkeryDB cannot use ArrayList<String> directly. 
 */
public class ListOfString extends ArrayList<String> {

 private static final long serialVersionUID = 1L;

 public ListOfString() {
  super();
 }
}