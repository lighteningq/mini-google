package edu.upenn.cis455.storage;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.http.util.Args;
import org.apache.log4j.Logger;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sleepycat.je.rep.monitor.NewMasterEvent;

//Access Key ID:
//AKIAIACZLFZKWR6WYZEQ
//Secret Access Key:
//7HfrZWWj3EQDDyQd4Cy3YkSomCME9d4m2dzqsCCQ
/**
 * https://www.baeldung.com/aws-s3-java
 * 
 * @author cis455
 *
 */

public class UploaderS3 {
	static Logger log = Logger.getLogger(UploaderS3.class);
	public static String AWS_ACCESS_KEY = "AKIAIACZLFZKWR6WYZEQ";
	public static String AWS_SECRET_KEY = "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";
	public static String AWS_TOKEN = "FwoGZXIvYXdzEFgaDK7n5AvYKg4IzLXlWCLDAUOrwGjF+N+qPa2y1sq1f0DCzHnkHRZkHpT9k366d1muk5GwY90fK0POuTPflDbNFhXKDKg/0GxNsMrxv0KYUAAo/KrLE2+9hkUMQtMcbdT2H2T/qWN1+B5BjH8xUpK5asiygwfHQMqRlpHNn7uuzraVmr+ElNq77Fwv5v4Xi2haQqrSuimkJeyPhaAtJtN/Nvy1F8FTTUqI556iuKDpE6h02ELQn6HyNHI54xLVS4OLHRavV7/T5SmLvbiHrRAbVZjIuyjkosL1BTIt/3k8JLCqVwwskutcK37R+wN8dmv+l+pjnpC/bRJW5TJRd7ExX5+dqU6egW3E";
	public final static String bucketName = "minigoogle2k20";
	static AWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);

	public UploaderS3() {
	}

	public static void uploadfileS3(String url, byte[] content) {
		AmazonS3 s3client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
		try {
			ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(content);
			ObjectMetadata md = new ObjectMetadata();
			md.setContentLength(content.length);

			String keyName = CrawlerUtil.generateURLId(url);
			s3client.putObject(new PutObjectRequest(bucketName, keyName, contentsAsStream, md));
		} catch (AmazonServiceException ase) {
			log.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			log.error("Error Message:    " + ase.getMessage());
			log.error("HTTP Status Code: " + ase.getStatusCode());
			log.error("AWS Error Code:   " + ase.getErrorCode());
			log.error("Error Type:       " + ase.getErrorType());
			log.error("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			log.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			log.error("Error Message: " + ace.getMessage());
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
	public static InputStream downloadfileS3(String urlId) {
		AmazonS3 s3client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();

		try {
			S3Object object = s3client.getObject(new GetObjectRequest(bucketName, urlId));
			InputStream objectData = object.getObjectContent();
			return objectData;
		} catch (AmazonServiceException ase) {
			log.error("URLID: " + urlId);
			log.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			log.error("Error Message:    " + ase.getMessage());
			log.error("HTTP Status Code: " + ase.getStatusCode());
			log.error("AWS Error Code:   " + ase.getErrorCode());
			log.error("Error Type:       " + ase.getErrorType());
			log.error("Request ID:       " + ase.getRequestId());
			return null;
		} catch (AmazonClientException ace) {
			log.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			log.error("Error Message: " + ace.getMessage());
			return null;
		}
	}

	public static void main(String args[]) {

//		try {
//			writeTXTEveryNID(300, 9000, "/home/cis455/project_crawled/urlid");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		seeEntryCount();
	}
	
	/**
	 * get a S3 client
	 * @return
	 */
	public static AmazonS3 getS3() {
		AmazonS3 s3client = AmazonS3ClientBuilder.standard()
		.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
		return s3client;
	}
	
	public static String extractContentWithS3(String urlId, AmazonS3 s3client) throws IOException {
		try {
			S3Object object = s3client.getObject(new GetObjectRequest(bucketName, urlId));
			InputStream objectData = object.getObjectContent();
			byte[] content = readInputStream(objectData);
			objectData.read(content);
			return new String(content);
			
		} catch (AmazonServiceException ase) {
			log.error("URLID: " + urlId);
			log.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			log.error("Error Message:    " + ase.getMessage());
			log.error("HTTP Status Code: " + ase.getStatusCode());
			log.error("AWS Error Code:   " + ase.getErrorCode());
			log.error("Error Type:       " + ase.getErrorType());
			log.error("Request ID:       " + ase.getRequestId());
			return null;
		} catch (AmazonClientException ace) {
			log.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			log.error("Error Message: " + ace.getMessage());
			return null;
		}
	}
	
	public static String extractContent(String urlId) throws IOException {
		try {
			AmazonS3 s3client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
			S3Object object = s3client.getObject(new GetObjectRequest(bucketName, urlId));
			InputStream objectData = object.getObjectContent();
			byte[] content = readInputStream(objectData);
			objectData.read(content);
			return new String(content);
			
		} catch (AmazonServiceException ase) {
			log.error("URLID: " + urlId);
			log.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			log.error("Error Message:    " + ase.getMessage());
			log.error("HTTP Status Code: " + ase.getStatusCode());
			log.error("AWS Error Code:   " + ase.getErrorCode());
			log.error("Error Type:       " + ase.getErrorType());
			log.error("Request ID:       " + ase.getRequestId());
			return null;
		} catch (AmazonClientException ace) {
			log.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			log.error("Error Message: " + ace.getMessage());
			return null;
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
	
	public static void seeEntryCount() {
		int count = 0;
		@SuppressWarnings("deprecation")
		AmazonS3Client s3 = new AmazonS3Client(credentials);
		for (S3ObjectSummary summary : S3Objects.withPrefix(s3, bucketName, "")) {
			count++;
			if (count%5000==0) {
				System.out.println("More than "+count);
			}
		}
		System.out.println("Number of entries: "+count);
	}

	public static void writeTXTEveryNID(int maxInFile, int maxNum, String dirPath) throws IOException {

		int fileCount = 0;
		int IDCount = 0;
		int IDCountInFile = 0;

		BufferedWriter bw = cleanWrite(dirPath + "/urlid_" + fileCount + ".txt");

		/* loop */

		@SuppressWarnings("deprecation")
		AmazonS3Client s3 = new AmazonS3Client(credentials);
		for (S3ObjectSummary summary : S3Objects.withPrefix(s3, bucketName, "")) {

			String urlid = summary.getKey();

			if (IDCountInFile >= maxInFile) {
				bw.flush();
				bw.close();
				IDCountInFile = 0;
				fileCount++;
				System.out.println(fileCount + " file written.");
				if (IDCount >= maxNum) {
					break;
				}
				bw = cleanWrite(dirPath + "/urlid_" + fileCount + ".txt");
			}
			bw.write(urlid + "\r\n");

			IDCount++;
			IDCountInFile++;

		}

	}

	public static BufferedWriter cleanWrite(String path) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		} else {
			file.delete();
			file.createNewFile();
		}
		return new BufferedWriter(new FileWriter(file));
	}

}
