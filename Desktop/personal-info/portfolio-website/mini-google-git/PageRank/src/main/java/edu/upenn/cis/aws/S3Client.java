package edu.upenn.cis.aws;

import java.util.List;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

//import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;

import com.amazonaws.services.s3.model.GetObjectRequest;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.upenn.cis.stormlite.bolt.pagerank.DBWrapper;
import edu.upenn.cis455.mapreduce.worker.WorkerAdmin;

public class S3Client {

	public static String AWS_ACCESS_KEY = "AKIAIACZLFZKWR6WYZEQ";
	public static String AWS_SECRET_KEY = "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";
	public static String AWS_TOKEN = "FwoGZXIvYXdzEFgaDK7n5AvYKg4IzLXlWCLDAUOrwGjF+N+qPa2y1sq1f0DCzHnkHRZkHpT9k366d1muk5GwY90fK0POuTPflDbNFhXKDKg/0GxNsMrxv0KYUAAo/KrLE2+9hkUMQtMcbdT2H2T/qWN1+B5BjH8xUpK5asiygwfHQMqRlpHNn7uuzraVmr+ElNq77Fwv5v4Xi2haQqrSuimkJeyPhaAtJtN/Nvy1F8FTTUqI556iuKDpE6h02ELQn6HyNHI54xLVS4OLHRavV7/T5SmLvbiHrRAbVZjIuyjkosL1BTIt/3k8JLCqVwwskutcK37R+wN8dmv+l+pjnpC/bRJW5TJRd7ExX5+dqU6egW3E";

	public final static String bucketName = "extractedurls2k20";
	static AWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);
	static AmazonS3 s3client = AmazonS3ClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();

	public static String workerStorage;
	public static DBWrapper urlIDSet;

	public S3Client() {

	}
	
	
    public static void createDir(String dir) {
    	if(!Files.exists(Paths.get(dir))) {
    		try {
				Files.createDirectories(Paths.get(dir));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
	
	public static void main(String args[]) {
		// getN();  
		System.out.println("New S3 prepare~~~!");

		String path = args[0] + "/" + WorkerAdmin.urlIDSet;
		createDir(path);
		urlIDSet = new DBWrapper(args[0] + "/" + WorkerAdmin.urlIDSet);
		urlIDSet.clearUrlId();
		
		
		int cntInOneFile = 25;
		int totalCnt = 100;
		int workerNum = -1;
		try {
			cntInOneFile = Integer.parseInt(args[1]);
			totalCnt = Integer.parseInt(args[2]);
			workerNum = Integer.parseInt(args[3]);
		}catch(Exception e) {
			
		}
		
 
		try {
			boolean needBDB = true;
			boolean needTXT = true;
			// writeTxtAndBDB(cntInOneFile, totalCnt, "/home/ec2-user/G06/G06/HW3/store/urlIds", needBDB, needTXT);
			String thepath = "";
			if (args[3].equals("ec2")) {
				thepath = "/home/ec2-user/G06/HW3/";
				
			} else {
				thepath = "/home/cis455/git/G06/HW3/";
			}
			String totalPath = thepath + args[0]+"/PRInputDir/S3";
			
			writeTxtAndBDB(cntInOneFile, totalCnt, totalPath, workerNum, needBDB, needTXT);
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	/**
	 * The API to get files from S3 in InputStream type. Please remember to close it
	 * when it's done
	 *
	 * @param credentials Related S3 credential information, can be read in
	 *                    ./conf/config.properties
	 * @param url         The requested URL from S3
	 * @return The InputStream for the requested File. Null if the request failed.
	 */
	public static  InputStream downloadfileS3(String urlId) {
		AmazonS3 s3client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();

		try {
			S3Object object = s3client.getObject(new GetObjectRequest(bucketName, urlId));
			InputStream objectData = object.getObjectContent();
			return objectData;
		} catch (AmazonServiceException ase) {
			System.out.println("URLID: " + urlId);
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			return null;
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
			return null;
		}
	}

	public static Object byteToObj(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
		ObjectInputStream objStream = new ObjectInputStream(byteStream);
		
		return objStream.readObject();
	}

	public static long getN() {
		int cnt = 0;
		for (S3ObjectSummary summary : S3Objects.withPrefix(s3client, bucketName, "")) {
			String urlid = summary.getKey();
			cnt++;
		}
		System.out.println("N = " + cnt);
		return cnt;

	}




	public static BufferedWriter cleanWrite(String path) throws IOException {
		System.out.println("path = " + path);
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		} else {
			file.delete();
			file.createNewFile();
		}
		return new BufferedWriter(new FileWriter(file));
	}

	public static void writeTxtAndBDB(int maxInFile, int maxNum, String dirPath, int workerNum, boolean needBDB, boolean needTXT)
			throws IOException {

		int fileCount = 0;
		int IDCount = 0;
		int IDCountInFile = 0;

		BufferedWriter bw = cleanWrite(dirPath + "/urlid_" + fileCount + ".txt");

		for (S3ObjectSummary summary : S3Objects.withPrefix(s3client, bucketName, "")) {

			String urlid = summary.getKey();


			if (IDCountInFile >= maxInFile) {
				bw.flush();
				bw.close();
				// reset
				IDCountInFile = 0;
				fileCount++;

				System.out.println(fileCount + " file written.");
				if (IDCount >= maxNum) {
					break;
				}
				bw = cleanWrite(dirPath + "/urlid_" + fileCount + ".txt");
			}

			if (needTXT) {
				bw.write(urlid + "\r\n");
			}
			if (needBDB) {
				urlIDSet.addUrlId(urlid);
			}
			IDCount++;
			IDCountInFile++;

		}

	}

	public static byte[] readInputStream(InputStream inputStream) throws IOException {
		byte[] buffer = new byte[1024];
		int len = 0;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		while ((len = inputStream.read(buffer)) != -1) {
			bos.write(buffer, 0, len);
		}
		bos.close();
		return bos.toByteArray();
	}

	public static AmazonS3 getClient() {
		// TODO Auto-generated method stub
		return s3client;
	}

	public static List<String> getExtractedIds(String urlId) {
		List<String> outUrlList = null;
		try {
			long start = System.currentTimeMillis();
			InputStream inputStream = downloadfileS3(urlId);
			byte[] content = readInputStream(inputStream);

			try {
				Object obj = byteToObj(content);
				outUrlList = (List<String>) obj;
				System.out.println("outUrlList size = " + outUrlList.size());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// String firstOutLink = outUrlList.get(0);
			long end = System.currentTimeMillis();
			//System.out.println("FirstOutLink = " + firstOutLink);
			//System.out.println(String.format("[read content]: %.2f s", ((end - start) * 1.0 / 1000.0))); //
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return outUrlList;
	}

}
