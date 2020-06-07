package amazonS3Test;


import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
/**
 * https://www.baeldung.com/aws-s3-java
 * @author cis455
 *
 */

public class connectS3Test {
  public static String AWS_ACCESS_KEY = "ASIA27KQ7VRKCFF3KMUY"; 
  public static String AWS_SECRET_KEY = "BIG4SxAZtrgWhRMCvgiprMBXbA1qQm595Q5vF4CF"; 
  public static String AWS_TOKEN = "FwoGZXIvYXdzEFgaDK7n5AvYKg4IzLXlWCLDAUOrwGjF+N+qPa2y1sq1f0DCzHnkHRZkHpT9k366d1muk5GwY90fK0POuTPflDbNFhXKDKg/0GxNsMrxv0KYUAAo/KrLE2+9hkUMQtMcbdT2H2T/qWN1+B5BjH8xUpK5asiygwfHQMqRlpHNn7uuzraVmr+ElNq77Fwv5v4Xi2haQqrSuimkJeyPhaAtJtN/Nvy1F8FTTUqI556iuKDpE6h02ELQn6HyNHI54xLVS4OLHRavV7/T5SmLvbiHrRAbVZjIuyjkosL1BTIt/3k8JLCqVwwskutcK37R+wN8dmv+l+pjnpC/bRJW5TJRd7ExX5+dqU6egW3E";

     public final  String bucketName = "trycis455spring";

    
        

   
    public static void main(String[] args){
     AWSCredentials  credentials = new BasicSessionCredentials(
       AWS_ACCESS_KEY, 
       AWS_SECRET_KEY,
       AWS_TOKEN
     );
     AmazonS3 s3client = AmazonS3ClientBuilder
       .standard()
       .withCredentials(new AWSStaticCredentialsProvider(credentials))
       .withRegion(Regions.US_EAST_1)
       .build();
     
     List<Bucket> buckets = s3client.listBuckets();
     for(Bucket bucket : buckets) {
         System.out.println(bucket.getName());
     }
      
//        s3client.putObject(
//            bucketName, 
//            "./interface/test.txt", 
//            new File("./interface/test.txt")
//          );
    }


}