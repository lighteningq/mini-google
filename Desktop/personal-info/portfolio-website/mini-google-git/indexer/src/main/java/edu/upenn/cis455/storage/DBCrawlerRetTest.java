package edu.upenn.cis455.storage;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis455.indexer.Lemmarize;

public class DBCrawlerRetTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String directory = "/home/cis455/git/proj_Index/crawler_000";
		DBCrawler db = new DBCrawler(directory);
		DBCrawler.retrieveURL();
		
		EntityCursor<DocEntry> cursor = DBCrawler.getDocEntryCursor();
		int count = 0;
		for(DocEntry docEntry: cursor) {
			count++;
			if (count!=2) {
				continue;
			}
			
			byte[] contentBytes = docEntry.getContent();
			if (contentBytes!=null) {
				String contentString = new String(contentBytes);
				System.out.println(contentString);
				
				Document doc = Jsoup.parse(contentString);

				String title = doc.title();
				String body = doc.body().text();

				System.out.println("After parsing, Title : " + title);
				System.out.println("Afte parsing, Body : " + body);

				title = Lemmarize.removeStopWords(title);
				body = Lemmarize.removeStopWords(body);

				System.out.println("After lemmarization, Title : " + title);
				System.out.println("Afte lemmarization, Body : " + body);
				
				
			}
			
			break;
		}
		cursor.close();
		DBCrawler.shutdownDB();
	}

}
