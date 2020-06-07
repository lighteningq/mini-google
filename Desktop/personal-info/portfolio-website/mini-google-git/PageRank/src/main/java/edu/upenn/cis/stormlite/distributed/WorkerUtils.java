package edu.upenn.cis.stormlite.distributed;

import java.util.Map;

public class WorkerUtils {
	
	/**
	 * Get the list of workers
	 * 
	 * @param config
	 * @return
	 */
	public static String[] getWorkerAddrs(Map<String, String> config) {
		String list = getServerList(config);
		String[] servers = list.split(",");
		String[] ret = new String[servers.length];
		int i = 0;
		for (String item: servers)
			if (!item.startsWith("http"))
				ret[i++] = "http://" + item;
			else
				ret[i++] = item;	
		return ret;
	}
	
	public static String getServerList(Map<String, String> config) {
		String list = config.get("workerList");
		if (list.startsWith("["))
			list = list.substring(1);
		if (list.endsWith("]"))
			list = list.substring(0, list.length() - 1);
		return list;
	}
	
}
