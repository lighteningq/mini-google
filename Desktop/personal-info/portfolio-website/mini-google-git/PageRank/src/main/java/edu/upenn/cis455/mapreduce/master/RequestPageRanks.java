package edu.upenn.cis455.mapreduce.master;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

public class RequestPageRanks {
	
	//private static final String MasterLocation = "http://18.212.246.36:8000";
	private static final String MasterLocation = "http://127.0.0.1:8000";

	public static void main(String[] args) throws IOException {
		
		// input urlIds:
		HashSet<String> queryIds = new HashSet<>();
		//queryIds.addAll(Arrays.asList("154137d90f8d311d18bee3d4e126e6b", "163e891b75b18f1a22f2205868dee31a", "16acb859ce33204b29a22ff2d8b1dedb", "16b38c26814ba98acfa88d5a6cde458f"));
		//queryIds.addAll(Arrays.asList("A", "B", "C", "D"));
		queryIds.addAll(Arrays.asList("10ec84957604d7b42d11bd33ec58091e", "10e857db6d96f4db763236920703e50b"));
		// send request
        RequestPageRanks r = new RequestPageRanks();
        
        // get pageRanks
        Config pageRanks = r.sendRequest(queryIds);
        System.out.println("[RequestPageRank]: POST /pageRanks, pageRanks =  " + pageRanks);
	}
	
	
	public Config sendRequest(Set<String> queryIdSet) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        
        String queryIdsStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(queryIdSet);
		byte[] postData  = queryIdsStr.getBytes();
		
		StringBuilder sb = new StringBuilder();
        sb.append(RequestPageRanks.MasterLocation + "/pageRanks");
        URL url = new URL(sb.toString());

        // send request to master server(master.routes: MasterPageRank)
        HttpURLConnection conn;
        conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput( true );
        conn.setInstanceFollowRedirects( false );
        conn.setRequestMethod("POST");
        conn.setRequestProperty( "Content-Type", "application/x-www-form-urlencoded"); 
        conn.setRequestProperty( "charset", "utf-8");
        conn.setRequestProperty( "Content-Length", Integer.toString( postData.length ));
    
        try( DataOutputStream wr = new DataOutputStream( conn.getOutputStream())) {
        	wr.write( postData );
        }
        
        // read response from master server
        InputStream in = conn.getInputStream();
        String encoding = conn.getContentEncoding();
        encoding = encoding == null ? "UTF-8" : encoding;
        String pageRanksRes = IOUtils.toString(in, encoding);
        Config pageRanks = mapper.readValue(pageRanksRes, Config.class);
        return pageRanks;
	}
}
