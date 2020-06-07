/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.stormlite.routers;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

/**
 * Does hash partitioning on the tuple to determine
 * a destination
 * 
 * @author zives
 *
 */
public class FieldBased extends StreamRouter {
	List<Integer> fieldsToHash;
	List<String> shardFields;
	private int myPort = WorkerAdmin.getInstance().myPort();
	
	public FieldBased() {
		fieldsToHash = new ArrayList<Integer>();
	}
	
	public FieldBased(List<String> shardFields) {
		fieldsToHash = new ArrayList<Integer>();
		this.shardFields = shardFields;
	}
	
	/**
	 * Adds an index field of an attribute that's used to shard the data
	 * @param field
	 */
	public void addField(Integer field) {
		fieldsToHash.add(field);
	}
	
	/**
	 * Determines which bolt to route tuples to
	 */
	
	public List<IRichBolt> getBoltsFor(List<Object> tuple) {

		int hash = 0;
		if (fieldsToHash.isEmpty()){
			throw new IllegalArgumentException("Field-based grouping without a shard attribute");
		}
			
		for (Integer i: fieldsToHash){
			Object filedData = tuple.get(i);
			if (filedData instanceof String) {
				String filedStr = (String) filedData;
				int underscoreIdx = filedStr.indexOf("_");
				if(underscoreIdx != -1) {
					filedStr = filedStr.substring(underscoreIdx+1);
				} 
				hash ^= filedStr.hashCode();

			} else {
				hash ^= tuple.get(i).hashCode();
			}
		}
		
		
		
		hash = hash % getBolts().size();
		if(hash < 0) {
			hash = hash + getBolts().size(); 
		}
		int listIdx = 0;
		try {
			listIdx = Integer.parseInt( getListIdxForReduceBolt(hash) );
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/*
		 * 	if (hash < 0)
			hash = hash + getBolts().size();
		    System.out.println("[FileldBased]: getBoltsFor(tuple = " + tuple + "),  will be sent to Bolt No." + hash + ", getBolts().size() = " + getBolts().size());

		 */

		
		System.out.println("[FileldBased]: getBoltsFor(tuple = " + tuple + "),  will be sent to Bolt No." + listIdx + ", getBolts().size() = " + getBolts().size());
		List<IRichBolt> ret = new ArrayList<>();
		ret.add(getBolts().get(listIdx));

		return ret;
	}
	
	
	public String getListIdxForReduceBolt(Integer hash) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(WorkerAdmin.masterLocation + "/getListIdx?");
        sb.append("hash=" + hash);
        sb.append("&port=" + myPort);
        System.out.println("[FiledBased]:/getListIdx?hash=" + hash + "&port=" + myPort);
        URL url = new URL(sb.toString());

        HttpURLConnection conn;
        conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("GET");
        conn.connect();
        
        InputStream in = conn.getInputStream();
        String encoding = conn.getContentEncoding();
        encoding = encoding == null ? "UTF-8" : encoding;
        String listidx = IOUtils.toString(in, encoding);
        System.out.println("[FieldBased]: getListIdx =  " + listidx);
        return listidx;

	}
	
	
	// public List<IRichBolt> getBoltsFor(List<Object> tuple) {

	// 	int hash = 0;
	// 	if (fieldsToHash.isEmpty()){
	// 		throw new IllegalArgumentException("Field-based grouping without a shard attribute");
	// 	}
			
	// 	for (Integer i: fieldsToHash){
	// 		hash ^= tuple.get(i).hashCode();
	// 	}
			
	// 	hash = hash % getBolts().size();

	// 	if (hash < 0)
	// 		hash = hash + getBolts().size();
		
	// 	System.out.println("[FileldBased]: getBoltsFor(tuple = " + tuple + "),  will be sent to Bolt No." + hash);
    //     List<IRichBolt> ret = new ArrayList<>();
	// 	ret.add(getBolts().get(hash));

	// 	return ret;
	// }

	/**
	 * Handler that, given a schema, looks up the index positions used
	 * for sharding fields
	 */
	@Override
	public void declare(Fields fields) {
		super.declare(fields);

		if (shardFields != null) {
			for (String name: shardFields) {
				Integer pos = fields.indexOf(name);
				if (pos < 0)
					throw new IllegalArgumentException("Shard field " + name + " was not found in " + fields);
				if (!fieldsToHash.contains(pos))
					fieldsToHash.add(pos);
			}
		}
	}

	public List<Integer> getFieldsToHash() {
		return fieldsToHash;
	}
	
	public String getKey(List<Object> input) {
		StringBuilder sb = new StringBuilder();
		
		int inx = 0;
		for (Integer i: fieldsToHash) {
			if (inx > 0)
				sb.append(',');
			else
				inx++;
			sb.append(input.get(i));
		}
		return sb.toString();
	}

	public void setFieldsToHash(List<Integer> fieldsToHash) {
		this.fieldsToHash = fieldsToHash;
	}

	public List<String> getShardFields() {
		return shardFields;
	}

	public void setShardFields(List<String> shardFields) {
		this.shardFields = shardFields;
	}

	
}
