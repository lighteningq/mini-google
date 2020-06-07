package test;
 
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
 
public class FutureExample {
 
    static class MyCallable implements Callable<String> {
    	private String url;
        public MyCallable(String url) {
			this.url = url;
		}

		@Override
        public String call() throws Exception {
			 Document doc = Jsoup.connect(url).get();
			 String responseText = doc.text();
			 return responseText;
        }
    }
 
    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
		String[] urls = {"https://www.baidu.com", "https://www.google.com","https://www.cdc.gov","https://www.weblistingsolutions.com",
				"https://www.amazon.com","https://www.baidu.com", "https://www.google.com","https://www.cdc.gov","https://www.weblistingsolutions.com",
				"https://www.amazon.com","https://www.unscramblerer.com/unscramble-word","https://www.baidu.com", "https://www.google.com","https://www.cdc.gov","https://www.weblistingsolutions.com",
				"https://www.amazon.com","https://www.unscramblerer.com/unscramble-word","https://www.baidu.com", "https://www.google.com","https://www.cdc.gov","https://www.weblistingsolutions.com",
				"https://www.amazon.com","https://www.unscramblerer.com/unscramble-word"};
		long starttime = System.currentTimeMillis();
		for(String url : urls) {
			 Future<String> future = executorService.submit(new MyCallable(url));
			 System.out.println("这里不阻塞，可以继续异步执行");
		     String result = future.get(); //get方法会发生阻塞，如果判断任务是否执行完成使用isDone()方法
		     System.out.println("result：" + result);
		}
		long cur =  System.currentTimeMillis();
		System.out.println(cur - starttime);
    }
}