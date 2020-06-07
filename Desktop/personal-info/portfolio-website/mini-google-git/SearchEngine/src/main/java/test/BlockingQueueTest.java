package test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BlockingQueueTest {

    public static void main(String[] args) throws InterruptedException {
        
        String[] urls = {"https://www.cdc.gov","https://www.google.com","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.yahoo.com","https://www.cdc.gov","https://www.cdc.gov","https://www.google.com","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.cdc.gov","https://www.cdc.gov","https://www.baidu.com","https://www.cdc.gov","https://www.cdc.gov",
        		"https://www.baidu.com","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov","https://www.cdc.gov"
				};


        // use Executors
        ExecutorService service = Executors.newCachedThreadPool();
        
        long starttime = System.currentTimeMillis();
        // start task
        for(String url : urls) {
        	 service.execute(new sendRequest(url));
        }
        System.out.println("whole url " + urls.length);
        System.out.println( System.currentTimeMillis() - starttime);
        // exit Executor
        service.shutdown();
    }
}