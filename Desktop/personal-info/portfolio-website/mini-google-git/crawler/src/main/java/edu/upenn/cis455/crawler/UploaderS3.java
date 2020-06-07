package edu.upenn.cis455.crawler;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

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
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import edu.upenn.cis455.crawler.workerServer.WorkerServer;
import edu.upenn.cis455.storage.DBWrapper;

//Access Key ID:
//AKIAIACZLFZKWR6WYZEQ
//Secret Access Key:
//7HfrZWWj3EQDDyQd4Cy3YkSomCME9d4m2dzqsCCQ
/**
 * https://www.baeldung.com/aws-s3-java
 * @author cis455
 *
 */

public class UploaderS3 {
	static Logger log = Logger.getLogger(UploaderS3.class);
	public static String AWS_ACCESS_KEY = "AKIAIACZLFZKWR6WYZEQ";
	 public static String AWS_SECRET_KEY = "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";
	 public static String AWS_TOKEN = "FwoGZXIvYXdzEFgaDK7n5AvYKg4IzLXlWCLDAUOrwGjF+N+qPa2y1sq1f0DCzHnkHRZkHpT9k366d1muk5GwY90fK0POuTPflDbNFhXKDKg/0GxNsMrxv0KYUAAo/KrLE2+9hkUMQtMcbdT2H2T/qWN1+B5BjH8xUpK5asiygwfHQMqRlpHNn7uuzraVmr+ElNq77Fwv5v4Xi2haQqrSuimkJeyPhaAtJtN/Nvy1F8FTTUqI556iuKDpE6h02ELQn6HyNHI54xLVS4OLHRavV7/T5SmLvbiHrRAbVZjIuyjkosL1BTIt/3k8JLCqVwwskutcK37R+wN8dmv+l+pjnpC/bRJW5TJRd7ExX5+dqU6egW3E";
	 public final static String docBucket = "minigoogle2k20";
	 public final static String urlBucket = "extractedurls2k20";
     static AWSCredentials  credentials = new BasicAWSCredentials(
    	       AWS_ACCESS_KEY, 
    	       AWS_SECRET_KEY
    	     );
	 

	 public UploaderS3() {
	 }
	 

	 public static void uploadfileS3(String url, byte[] content, List<String> extractedIds) {
			AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
				    .withRegion(Regions.US_EAST_1).build();
			try {
		         ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(content);
		         ObjectMetadata md = new ObjectMetadata();
		         md.setContentLength(content.length);
		         
		         byte[] extracted = CrawlerUtil.objToByte(extractedIds);
		         ByteArrayInputStream extractedIdAsStream = new ByteArrayInputStream(extracted);
		         ObjectMetadata mdextracted = new ObjectMetadata();
		         mdextracted.setContentLength(extracted.length);
		        
		         
		         String keyName = CrawlerUtil.generateURLId(url);
		         s3client.putObject(new PutObjectRequest(docBucket, keyName, contentsAsStream, md));  
		         s3client.putObject(new PutObjectRequest(urlBucket, keyName, extractedIdAsStream, mdextracted));
	         } catch (AmazonServiceException ase) {
	            log.error("Caught an AmazonServiceException, which " +
	            		"means your request made it " +
	                    "to Amazon S3, but was rejected with an error response" +
	                    " for some reason.");
	            log.error("Error Message:    " + ase.getMessage());
	            log.error("HTTP Status Code: " + ase.getStatusCode());
	            log.error("AWS Error Code:   " + ase.getErrorCode());
	            log.error("Error Type:       " + ase.getErrorType());
	            log.error("Request ID:       " + ase.getRequestId());
	        } catch (AmazonClientException ace) {
	            log.error("Caught an AmazonClientException, which " +
	            		"means the client encountered " +
	                    "an internal error while trying to " +
	                    "communicate with S3, " +
	                    "such as not being able to access the network.");
	            log.error("Error Message: " + ace.getMessage());
	        } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	 
	 
	 
	 
	 
	 
	 /**
		 * The API to get files from S3 in InputStream type. Please remember to close it when it's done
		 * @param credentials  Related S3 credential information, can be read in ./conf/config.properties
		 * @param url   The requested URL from S3
		 * @return The InputStream for the requested File. Null if the request failed.
		 */
		public static InputStream downloadfileS3(String urlId){
			AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
			
			try{ 
				S3Object object = s3client.getObject(new GetObjectRequest(docBucket, urlId));
				InputStream objectData = object.getObjectContent();
				return objectData;
			} catch (AmazonServiceException ase) {
				log.error("URLID: "+urlId);
	            log.error("Caught an AmazonServiceException, which " +
	            		"means your request made it " +
	                    "to Amazon S3, but was rejected with an error response" +
	                    " for some reason.");
	            log.error("Error Message:    " + ase.getMessage());
	            log.error("HTTP Status Code: " + ase.getStatusCode());
	            log.error("AWS Error Code:   " + ase.getErrorCode());
	            log.error("Error Type:       " + ase.getErrorType());
	            log.error("Request ID:       " + ase.getRequestId());
	            return null;
	        } catch (AmazonClientException ace) {
	            log.error("Caught an AmazonClientException, which " +
	            		"means the client encountered " +
	                    "an internal error while trying to " +
	                    "communicate with S3, " +
	                    "such as not being able to access the network.");
	            log.error("Error Message: " + ace.getMessage());
	            return null;
	        }
	}
		
		
		
		
		public static InputStream downloadExtractedS3(String urlId){
			AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
			
			try{ 
				S3Object object = s3client.getObject(new GetObjectRequest(urlBucket, urlId));
				InputStream objectData = object.getObjectContent();
				return objectData;
			} catch (AmazonServiceException ase) {
				log.error("URLID: "+urlId);
	            log.error("Caught an AmazonServiceException, which " +
	            		"means your request made it " +
	                    "to Amazon S3, but was rejected with an error response" +
	                    " for some reason.");
	            log.error("Error Message:    " + ase.getMessage());
	            log.error("HTTP Status Code: " + ase.getStatusCode());
	            log.error("AWS Error Code:   " + ase.getErrorCode());
	            log.error("Error Type:       " + ase.getErrorType());
	            log.error("Request ID:       " + ase.getRequestId());
	            return null;
	        } catch (AmazonClientException ace) {
	            log.error("Caught an AmazonClientException, which " +
	            		"means the client encountered " +
	                    "an internal error while trying to " +
	                    "communicate with S3, " +
	                    "such as not being able to access the network.");
	            log.error("Error Message: " + ace.getMessage());
	            return null;
	        }
	}
		
		
	public static int getDBIndex(String urlId) {
		int hash = 0;
		hash^= (urlId).hashCode();
		hash = hash % 20; // assume 5 urlDistributeBolt per worker
		if (hash<0) hash = hash + 20;
		
		return hash / 5;
	}
	
	
		
}









