package edu.ucr.cs242;

import org.junit.Test;

public class UnitTest {

	@Test
	public void test() {
		String token = "you,ll!";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
		
		token = "you'll";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
		
		token = "you’ll";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
		
		token = "input.txt!";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
		
		token = "input.txt.";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
		
	
		token = token.replaceAll(WordCount.CLEAR_TRAILING_PERIODS,"");
		System.out.println("after="+token);
		
		token = "target/wordcount-1.0-SNAPSHOT.jar";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
		
		token = "target\\wordcount-1.0-SNAPSHOT.jar";
		System.out.println("before="+token);
		token = token.replaceAll(WordCount.CLEAR_PUNCTUATION_REGEX,"");
		System.out.println("after="+token);
	}
}
