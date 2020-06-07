//package edu.upenn.cis.stormlite.spout;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import edu.upenn.cis.stormlite.tuple.Values;
//
//public class testFileSpout {
//    // For default, read all files under "inputDir" 
//	
//    String inputDir = "Worker1/inputdir";
//	Map<String, BufferedReader> readersMap = new HashMap<>();
//    BufferedReader currReader = null;
//    String currFileName = null;
//    Iterator<Entry<String, BufferedReader>> itr;
//    boolean sentEos = false;
//    
//    public static void main(String[] args) throws FileNotFoundException {
//    	testFileSpout test=  new testFileSpout();
//    	
//    	test.prepare();
//    	
//    	int i = 0;
//    	while(test.nextTuple()) {
//    		i++;
//    	}
//    	System.out.println("i = " + i);
//    }
//    
//    
//    public void prepare() throws FileNotFoundException {
//    	System.out.println("[Spout]: Starting spout from diretory:  " + this.inputDir);
//		File directory = new File(this.inputDir);
//		for (File f : directory.listFiles()) {
//			BufferedReader reader = new BufferedReader(new FileReader(f));
//			this.readersMap.put(f.getName(), reader);
//		}
//		this.itr = this.readersMap.entrySet().iterator();
//		if(itr.hasNext()) {
//			Map.Entry<String,BufferedReader> entry = itr.next();
//			this.currFileName = entry.getKey();
//			this.currReader = entry.getValue();
//		}
//		System.out.println("[FileSpout]: readersMap.size() = " + readersMap.size());
//    	
//    	
//    }
//    
//    public synchronized boolean nextTuple() {
//    		
//		// For default, emit(currFileName, each line), only support one spout
//
//    	if (this.currReader != null  && !sentEos) {
//	    	try {
//		    	String line = this.currReader.readLine();
//		    	
//		    	if(line == null && this.itr.hasNext()) {
//		    		
//		    			Map.Entry<String,BufferedReader> entry = this.itr.next();
//		    			this.currFileName = entry.getKey();
//		    			this.currReader = entry.getValue();
//		    			
//		    			line = currReader.readLine();
//		    			System.out.println("update: currFileName = " + currFileName + ", line  = " + line);
//
//		    	} else if (line == null && !this.itr.hasNext()) {
//		    		return false;
//		    	}
//		    		
//		    
//		    	
//		    	
//		    	if (line != null) {
//		        	 System.out.println("[Spout]: read from file : {" + currFileName + ", " + line + "}");
//		    	     //this.collector.emit(new Values<Object>(currFileName, line));
//
//		    	} else if (!this.readersMap.entrySet().iterator().hasNext() && !sentEos) {
//		        	
//		        	System.out.println("[Spout]: emit EOS");
//	    	        sentEos = true;
//		    	}
//		    	
//		    	try {
//		    		Thread.sleep(100);
//				} catch (Exception e) {
//		    		e.printStackTrace();
//				}
//	    	} catch (IOException e) {
//	    		e.printStackTrace();
//	    	}
//	        Thread.yield();
//	        return true;
//    	} else {
//    		System.out.println("return false");
//    		return false;
//    	}
//    }
//
//}
