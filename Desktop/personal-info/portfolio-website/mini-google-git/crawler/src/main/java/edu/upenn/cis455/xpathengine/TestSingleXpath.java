package edu.upenn.cis455.xpathengine;
import java.util.*;

public class TestSingleXpath {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String s1 = "/foo/bar/this";
		String s2 ="/foo/bar[@att=\"123\"]";
		String s3 = "/xyz/abc[contains(text(),           \"someSubstring\")]";
		String s4 = "/a/b/c[text()=\"theEntireText\"]";
		String s5 = "/blah[anotherElement]";
		String s6 = "/this/that[something/else]";
		String s7 = "/d/e/f[foo[text()=       \"    something\"]][bar]";
		String s8 = "/a/b/c[text() =                \"whiteSpace   sShouldNotMatter\"]";
		String s9 = "/a[@attr=\"123\"]/b/c[foo][bar]";
		String s10 = "/a/b/c/d[e/f][g[h]]/i/j[k]]/l";
		String[] arr = new String[] {s1,s2,s3,s4,s5,s6,s7,s8,s9,s10};
		XPathEngineImpl x= new XPathEngineImpl();
		x.setXPaths(arr);
		System.out.println(x.isValid(0));
		for(boolean b: x.validArr) System.out.println("Is Valid? "+b);
		//System.out.println(x.cleanUpWhiteSpaces("I am \"a    hero""));
		
//		for(String s: arr) {
//			SingleXPath x = new SingleXPath(s);
//			System.out.println("Trimmed XPath is: ||"+ x.xpath+"||");
//			System.out.println("Is it Valid? "+ x.isValid());
//			x.traverse(x.root);
//			
//		}
	}

}
