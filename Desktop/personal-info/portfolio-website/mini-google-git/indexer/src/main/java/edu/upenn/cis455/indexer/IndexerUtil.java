package edu.upenn.cis455.indexer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.upenn.cis455.mapreduce.job.WordCount;

public class IndexerUtil {
	
	

	public static final Pattern WORD_PATTERN = Pattern.compile("\\b[a-zA-Z]+\\b");
	
	/**
	 * segment and count words for the given sentence. return max word count
	 * @param sentence
	 * @param wordCountMap
	 * @param wordLocationMap
	 * @return max word count
	 */
	public static int segmentAndCountWords(String sentence,
			Map<String, Integer> wordCountMap, Map<String, String> wordLocationMap) {
		
		int wordCount = 0; // location
		int maxCount = 0;
		
		/*  word count  */
        
        // title must not be empty
        if (sentence!=null) {
        	Matcher matcher = WORD_PATTERN.matcher(sentence);
        	while(matcher.find()){
        		
        		// ignore case
        		String word = matcher.group(0).toLowerCase();
        		
        		// ignore stopwords
        		if (STOPWORDS.contains(word))
        			continue;
        		
				int thisWordCount;
				if (wordCountMap.containsKey(word)) {
					// count
					int lastCount = wordCountMap.get(word);
					thisWordCount = lastCount + 1;
					wordCountMap.replace(word, thisWordCount);
					// location
					wordLocationMap.replace(word, wordLocationMap.get(word) + " " + (String.valueOf(wordCount)));
				} else {
					// count
					thisWordCount = 1;
					wordCountMap.put(word, thisWordCount);
					// location
					wordLocationMap.put(word, String.valueOf(wordCount));
				}
				maxCount = (thisWordCount > maxCount ? thisWordCount : maxCount);
				wordCount++;
			}
        }
        return maxCount;
	}
	
	/**
	 * helper function to calculate the TF value of a certain word.
	 * 
	 * @param count
	 * @param maxCount
	 * @return
	 */
	public static Double calculateNormalizedTF(int count, int maxCount) {
		Double ret = TF_PARAM + (1.0 - TF_PARAM) * Double.valueOf(count) / Double.valueOf(maxCount);
		return ret;
	}
	public static final Double TF_PARAM = 0.2;
	
	/**
	 * count/ length
	 * @param count
	 * @param length
	 * @return
	 */
	public static Double calculateFractionalTf(int count, int length) {
		Double ret = Double.valueOf(count) / Double.valueOf(length);
		return ret;
	}
	/**
	 * stop words list
	 */
	public static final Set<String> STOPWORDS = Collections.unmodifiableSet(new HashSet<String>() {
		{
			add("d");
			add("ll");
			add("re");
			add("s");
			add("aren");
			add("ain");
			add("isn");
			add("hadn");
			add("t");
			add("ve");
			add("don");
			add("nt");
			add("a");
			add("about");
			add("above");
			add("after");
			add("again");
			add("against");
			add("all");
			add("am");
			add("an");
			add("and");
			add("any");
			add("are");
			add("as");
			add("at");
			add("be");
			add("because");
			add("been");
			add("before");
			add("being");
			add("below");
			add("between");
			add("both");
			add("but");
			add("by");
			add("cannot");
			add("could");
			add("did");
			add("do");
			add("does");
			add("doing");
			add("down");
			add("during");
			add("each");
			add("few");
			add("for");
			add("from");
			add("further");
			add("had");
			add("has");
			add("have");
			add("having");
			add("he");
			add("her");
			add("here");
			add("hers");
			add("herself");
			add("him");
			add("himself");
			add("his");
			add("how");
			add("i");
			add("if");
			add("in");
			add("into");
			add("is");
			add("it");
			add("its");
			add("itself");
			add("me");
			add("more");
			add("most");
			add("my");
			add("myself");
			add("no");
			add("nor");
			add("not");
			add("of");
			add("off");
			add("on");
			add("once");
			add("only");
			add("or");
			add("other");
			add("ought");
			add("our");
			add("ours");
			add("ourselves");
			add("out");
			add("over");
			add("own");
			add("same");
			add("she");
			add("should");
			add("so");
			add("some");
			add("such");
			add("than");
			add("their");
			add("theirs");
			add("them");
			add("themselves");
			add("the");
			add("then");
			add("there");
			add("these");
			add("they");
			add("this");
			add("those");
			add("through");
			add("to");
			add("too");
			add("under");
			add("until");
			add("up");
			add("very");
			add("was");
			add("we");
			add("were");
			add("what");
			add("when");
			add("where");
			add("which");
			add("while");
			add("who");
			add("whom");
			add("why");
			add("with");
			add("would");
			add("you");
			add("your");
			add("yours");
			add("yourself");
			add("yourselves");
			add("de");
			add("del");
			add("di");
			add("y");
			add("corporation");
			add("corp");
			add("corp.");
			add("co");
			add("llc");
			add("inc");
			add("inc.");
			add("ltd");
			add("ltd.");
			add("llp");
			add("llp.");
			add("plc");
			add("plc.");
			add("&");
			add(",");
			add("-");
		}
	});
	
	
}
