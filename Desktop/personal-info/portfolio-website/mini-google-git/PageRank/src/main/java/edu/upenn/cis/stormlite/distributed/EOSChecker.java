package edu.upenn.cis.stormlite.distributed;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Coordinator for tracking and reaching consensus
 * 
 * Tracks both who voted and how many votes were recorded.
 * If someone votes twice, there is an exception.  If enough
 * votes are received to pass a specified threshold, we assume consensus
 * is reached.
 *
 */
public class EOSChecker {
	
	/**
	 * Track the set of voters
	 */

	Set<String> voted = ConcurrentHashMap.newKeySet();
	int votesNeeded;
	
	public EOSChecker(int votesNeeded) {
		this.votesNeeded = votesNeeded;
	}
	
	/**
	 * Add another vote towards consensus.
	 * @param voter Optional ID of the node / executor that voted, for tracking
	 * if anyone is voting more than once!
	 * 
	 * @return true == we have enough votes for consensus end-of-stream.
	 *         false == we don't yet have enough votes.
	 */
	public synchronized boolean addVoterAndCheckEos(String voter, String voterType) {

		
		if (voter != null && !voter.isEmpty()) {
			if (voted.contains(voter)) {
				//System.out.println(" [Checker]: Executor " + voter.substring(0,4) + " already voted EOS!");
			} else{
				voted.add(voter);
			}
		}
		System.out.println("[Checker]: voter = " + voter.substring(0,4) + ", who is checking? " + voterType + ", voted size() = " + voted.size());
		if (voted.size() >= votesNeeded)
			return true;
		else
			return false;
	}
}
