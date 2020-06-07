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
package edu.upenn.cis.stormlite;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolt.BoltDeclarer;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.bolt.pagerank.PRLastReduceBolt;

import edu.upenn.cis.stormlite.distributed.SenderBolt;
import edu.upenn.cis.stormlite.distributed.WorkerUtils;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tasks.ITask;
import edu.upenn.cis.stormlite.tasks.SpoutTask;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

/**
 * Use multiple threads to simulate a cluster of worker nodes.
 * Hooks to other nodes in a distributed environment.
 * 
 * A thread pool (the executor) executes runnable tasks.  Each
 * task involves calling a nextTuple() or execute() method in
 * a spout or bolt, then routing its tuple to the router. 
 * 
 * @author zives
 *
 */
public class DistributedCluster implements Runnable {

	
	static AtomicBoolean quit = new AtomicBoolean(false);
	
	String theTopology;
	
	Map<String, List<IRichBolt>> boltStreams = new HashMap<>();
	Map<String, List<IRichSpout>> spoutStreams = new HashMap<>();
	
	Map<String, StreamRouter> streams = new HashMap<>();
	
	TopologyContext context;
	
	ObjectMapper mapper = new ObjectMapper();
	
	ExecutorService executor = Executors.newFixedThreadPool(1);
	
	Queue<ITask> taskQueue = new ConcurrentLinkedQueue<ITask>();
	
	Map<Integer, String> idxToBoltId = new HashMap<>();
	Map<String, String> BoltIdToWorkerAddr = new HashMap<>();
	private int workersNum;

	/**
	 * Sets up the topology, instantiating objects (executors)
	 * on the local machine.  (Needs to be run on each worker in
	 * a distributed cluster.)
	 * 
	 * Does not start worker threads; that is in startTopology. 
	 * 
	 * 
	 * @param name
	 * @param config
	 * @param topo
	 * @return
	 * @throws ClassNotFoundException
	 */
	public TopologyContext submitTopology(String name, Config config, 
			Topology topo) throws ClassNotFoundException {
		theTopology = name;
		
		context = new TopologyContext(topo, taskQueue);
		
		boltStreams.clear();
		spoutStreams.clear();
		streams.clear();
		// System.out.println("[DistributedCluster]: topo =" + topo.toString());
		createSpoutInstances(topo, config);
		
		createBoltInstances(topo, config);
		
		createRoutes(topo, config);
		scheduleSpouts();
		// System.out.println("[DistributedCluster]: context =" + context.toString());
		return context;
	}
	
	/**
	 * Starts the worker thread to process events, starting with the spouts.
	 */
	public void startTopology() {
		// Put the run method in a background thread
		new Thread(this).start();
		
	}
	
	/**
	 * The main executor loop uses Java's ExecutorService to schedule tasks.
	 */
	public void run() {
		while (!quit.get()) {
			ITask task = taskQueue.poll();
			if (task == null)
				Thread.yield();
			else {
				executor.execute(task);
			}
		}
	}
	
	/**
	 * Allocate units of work in the task queue, for each spout
	 */
	private void scheduleSpouts() {
		for (String key: spoutStreams.keySet())
			for (IRichSpout spout: spoutStreams.get(key)) {
				taskQueue.add(new SpoutTask(spout, taskQueue));
			}
	}
	
	/**
	 * For each spout in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 * @throws ClassNotFoundException 
	 */
	private void createSpoutInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String key: topo.getSpouts().keySet()) {
			StringIntPair spout = topo.getSpout(key);
			
			spoutStreams.put(key, new ArrayList<IRichSpout>());
			for (int i = 0; i < spout.getRight(); i++)
				try {
					IRichSpout newSpout = (IRichSpout)Class.forName(spout.getLeft()).newInstance();
					
					SpoutOutputCollector collector = new SpoutOutputCollector(context);
					
					newSpout.open(config, context, collector);
					spoutStreams.get(key).add(newSpout);
					//log.debug("Created a spout executor " + key + "/" + newSpout.getExecutorId() + " of type " + spout.getLeft());
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}


	/**
	 * For each bolt in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 * @throws ClassNotFoundException 
	 */
	private void createBoltInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String key: topo.getBolts().keySet()) {
			StringIntPair bolt = topo.getBolt(key);
			
			boltStreams.put(key, new ArrayList<IRichBolt>());
			int localExecutors = bolt.getRight();
			for (int i = 0; i < localExecutors; i++)
				try {
					OutputCollector collector = new OutputCollector(context);
					
					IRichBolt newBolt = (IRichBolt)Class.forName(bolt.getLeft()).newInstance();
					newBolt.prepare(config, context, collector);
					boltStreams.get(key).add(newBolt);
					//log.debug("Created a bolt executor " + key + "/" + newBolt.getExecutorId() + " of type " + bolt.getLeft());
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	/**
	 * Link the output streams to input streams, ensuring that the right kinds
	 * of grouping + routing are accomplished
	 * 
	 * @param topo
	 * @param config
	 */
	private void createRoutes(Topology topo, Config config) {
		// Add destination streams to the appropriate bolts
		System.out.println("[DistributedCluster]: topo.getBolts().keySet() = " + topo.getBolts().keySet());
		
		String[] workerAddrs = WorkerUtils.getWorkerAddrs(config);
		this.workersNum = workerAddrs.length;
		
		try {
			addWorkersNum(workersNum);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for (String stream: topo.getBolts().keySet()) {

			BoltDeclarer decl = topo.getBoltDeclarer(stream);
			
			StreamRouter router = decl.getRouter();
			
			streams.put(stream, router);
			
			int count = boltStreams.get(stream).size();
			// System.out.println("\n\n[DistributedCluster]:Declarer = "+decl.getStream()+", stream = " + stream + " boltStreams = " + boltStreams + "boltStreams.get(stream) = " + boltStreams.get(stream));
			// System.out.println("[DistributedCluster]: boltStreams.get(stream).size() = " + count);

			// Create a bolt for each remote worker, give it the same # of entries
			// as we had locally so round-robin and partitioning will be consistent
			int workerId = 0;

			for (String worker: workerAddrs) {
				
				// System.out.println("[DistributedCluster]:worker = " + worker + ", workerId = " + workerId + ", config.get(workerIndex) = "  + config.get("workerIndex"));
				// Create one sender bolt for each node aside from us!
				int myWorkerIdx = Integer.valueOf(config.get("workerIndex"));
				if (workerId++ != myWorkerIdx) {
					SenderBolt sender = new SenderBolt(worker, stream);
					sender.prepare(config, context, null);
					
					for (int i = 0; i < count; i++) {
						// System.out.println("[DistributedCluster]:bouta addRemoteBolt(), i = " + i);
						
						
						if(decl.getStream().equals("MAP_BOLT" + config.get("iterations"))) {
							int listIdx = router.getBolts().size();
							String boltId = sender.getExecutorId();
							int toWorkerIdx = workerId-1;
							try {
								addBoltIdToMaster(listIdx, myWorkerIdx, toWorkerIdx, null, true);
							} catch (IOException e) {
								e.printStackTrace();
							}
							// System.out.println("[DistributedCluster]: If: Adding a remote route from " + stream + " to " + worker);
						}
						
						
						
						router.addRemoteBolt(sender);
						
						
					}
					
					
				// Create one local executor for each node for us!
				} else {
					// System.out.println("[DistributedCluster]:bouta add local bolt");
					for (IRichBolt bolt: boltStreams.get(stream)) {
						// System.out.println("[DistributedCluster]:bolt type " + bolt.getClass().getName());
						
						
						if (bolt instanceof PRLastReduceBolt) {
							// System.out.println("[DistributedCluster]:bolt type is PRLastReduceBolt ");
							int listIdx = router.getBolts().size();
							String boltId = bolt.getExecutorId();
							// System.out.println("[DistributedCluster]:  listIdx = " + listIdx + ", myWorkerIdx = " + myWorkerIdx + ", bolt id  = " + boltId);
							try {
								addBoltIdToMaster(listIdx, myWorkerIdx, -1, boltId, false);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						

						router.addBolt(bolt);
						// System.out.println("[DistributedCluster]: Else: Adding a route from Del:" + decl.getStream() + " to " + bolt);
						
					}
				}
				
				//System.out.println("[DistributedCluster]: router.getBolt().size() = " +router.getBolts().size());
				
				// For debugging:
           
			}
			
			if (topo.getBolts().containsKey(decl.getStream())) {
				for (IRichBolt bolt: boltStreams.get(decl.getStream())) {
					bolt.setRouter(router);
					bolt.declareOutputFields(router);
				}
			} else {
				for (IRichSpout spout: spoutStreams.get(decl.getStream())) {
					spout.setRouter(router);
					spout.declareOutputFields(router);
				}
			}
			
		}
	}
	
	public synchronized void addWorkersNum(int workersNum) throws IOException{
			
		// System.out.println("[DistributedCluster]: addWorkersNum() ");
        StringBuilder sb = new StringBuilder();
        sb.append(WorkerAdmin.masterLocation + "/workersNum?");
        sb.append("workersNum=" + workersNum);
        // System.out.println("[DistributedCluster]: addWorkersNum =  "+sb.toString());
        
        URL url = new URL(sb.toString());
        HttpURLConnection conn;
        conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("GET");
        conn.connect();
        conn.getResponseCode();
}
	public synchronized void addBoltIdToMaster(int listIdx, int myWorkerIdx, int toWorkerIdx, String boltId, boolean isSenderBolt) throws IOException{

			// System.out.println("[DistributedCluster]: addBoltIdToMaster() ");
	        StringBuilder sb = new StringBuilder();
	        sb.append(WorkerAdmin.masterLocation + "/addBoltId?");
	        if (isSenderBolt) {
	        	sb.append("&isSenderBolt=" + true);
	        } else {
	        	sb.append("&isSenderBolt=" + false);
	        }
	        
	        sb.append("&listIdx=" + listIdx);
	        sb.append("&myWorkerIdx=" + myWorkerIdx);
	        if(isSenderBolt) {
	        	sb.append("&toWorkerIdx=" + toWorkerIdx);
	        } else {
	        	sb.append("&boltId=" + boltId);
	        }
	        
	        // System.out.println("[DistributedCluster]: "+sb.toString());
	        
	        URL url = new URL(sb.toString());
	        HttpURLConnection conn;
	        conn = (HttpURLConnection)url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.connect();
	        conn.getResponseCode();
	}
	

	
	/**
	 * For each bolt in the topology, clean up objects
	 * 
	 * @param topo Topology
	 */
	private void closeBoltInstances() {
		for (List<IRichBolt> boltSet: boltStreams.values())
			for (IRichBolt bolt: boltSet)
				bolt.cleanup();
	}

	/**
	 * For each spout in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 */
	private void closeSpoutInstances() {
		for (List<IRichSpout> spoutSet: spoutStreams.values())
			for (IRichSpout spout: spoutSet)
				spout.close();
	}

	/**
	 * Shut down the cluster
	 * 
	 * @param string
	 */
	public void killTopology(String string) {
		if (quit.getAndSet(true) == false) {
			while (!quit.get())
				Thread.yield();
		}
		System.out.println(context.getMapOutputs() + " local map outputs and " + 
		context.getReduceOutputs() + " local reduce outputs.");

		for (String key: context.getSendOutputs().keySet())
			System.out.println("Sent " + context.getSendOutputs().get(key) + " to " + key);
		}

	/**
	 * Shut down the bolts and spouts
	 */
	public void shutdown() {
		closeSpoutInstances();
		closeBoltInstances();

		System.out.println("Shutting down distributed cluster.");
	}

	public StreamRouter getStreamRouter(String stream) {
		return streams.get(stream);
	}
}
