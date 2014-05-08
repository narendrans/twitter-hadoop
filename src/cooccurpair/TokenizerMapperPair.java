package cooccurpair;

import java.io.IOException;
import java.util.ArrayList;

import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.UtilityClass;

public class TokenizerMapperPair extends Mapper<Object, Text, Text, DoubleWritable>{

	private final static DoubleWritable one = new DoubleWritable(1);
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			StringTokenizer itr = new StringTokenizer(value.toString());		
			String currentToken = "";
			ArrayList<String> tokenList = new ArrayList<String>(); 
			System.out.println("Value in Mapper : " + value.toString());
			while (itr.hasMoreTokens()) 
			{				
				currentToken = itr.nextToken().trim();
				if(!UtilityClass.isStopWord(currentToken))			
					tokenList.add(UtilityClass.cleanText(currentToken));
				
			}
			
			// Forming co-occurring token from all possible combinations
			// using 'Pairs approach' 
			int j = 0;
			String nextToken = "", cooccurToken = "";
			for (int i = 0; i< tokenList.size(); i++) 
			{		
				j = 0;
				currentToken = tokenList.get(i);
				System.out.println("current token - " + currentToken + " i : "+ i);
				// Count for current token before it's pair
				word.set(currentToken);				
				context.write(word, one);
				
				while(j != tokenList.size()) 
				{
					if(j != i)
					{
						nextToken = tokenList.get(j);
						//System.out.println("next token - " + nextToken + " j : "+ j);
						// To sort based to ignore the order across pairs, eg A,B = B,A
						/*if(currentToken.compareTo(nextToken) < 0)
							cooccurToken = currentToken + "," + nextToken;				
						else
							cooccurToken = nextToken + "," + currentToken;
						*/
						cooccurToken = currentToken + "," + nextToken;
						word.set(cooccurToken);				
						System.out.println("Word Context to set in HDFS - " + word);			
						context.write(word, one);					
					}
					j++;
				}		
			}
		} catch (Exception e) {
			System.out.println("ERROR - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
}