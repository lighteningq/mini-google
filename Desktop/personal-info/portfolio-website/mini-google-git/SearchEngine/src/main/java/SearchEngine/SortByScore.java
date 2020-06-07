package SearchEngine;

import java.util.Comparator;

public class SortByScore implements Comparator<ResultEntry>{
	@Override
	public int compare(ResultEntry entry1, ResultEntry entry2) {
		if(entry1.score - entry2.score > 0 ) return -1;
		else if(entry1.score - entry2.score < 0 ) return 1;
		else return 0;
	}

}