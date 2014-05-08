package cooccurpair;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DoubleFrequencyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
	private static DoubleWritable result = new DoubleWritable();
	private static MapWritable singleKeyMap = new MapWritable(); 
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {		
		try {
			int sum = 0;
			
			System.out.println("*****************************");
			System.out.println("Reducer called - with key "+ key + " Value" + values.toString());
			
			for (DoubleWritable val : values) {
				
				sum += val.get();
			}
			result.set(sum);
			
			
			
			//Check whether we are single key, in that case add to local hashmap
			System.out.println("Reducer pair - " + key);
			String[] keyPair = key.toString().trim().split(",");
			if(keyPair.length == 1)
			{
				System.out.println("key length 1 - " + keyPair[0]);
				System.out.println("and result sum - " + result);
				singleKeyMap.put(new Text(keyPair[0].trim()), result);
				System.out.println("after getting it : " + singleKeyMap.get(new Text(keyPair[0])));
			}
			
			// Calculate Relative frequency of A,B with respect to A
			else 
			{	
				System.out.println("retrieving single key : " + keyPair[0]);
				DoubleWritable singleKeyFreq = (DoubleWritable) singleKeyMap.get(new Text(keyPair[0].trim()));
				System.out.println(" freq : " + singleKeyFreq);
				
//				System.out.println("retrieving double key : " + key.toString());
//				DoubleWritable doubleKeyFreq = (DoubleWritable) singleKeyMap.get(new Text(key.toString()));
//				System.out.println(" freq : " + doubleKeyFreq);
				
				System.out.println("result before RF - " +result);
				
				if(singleKeyFreq != null)
				{
					System.out.println("Frequency Reducer - Numerator 		   : " + result.get());
					System.out.println("Frequency Reducer - Denomerator		   : " + singleKeyFreq);
					System.out.println("Frequency Reducer - Relative Frequency : " + result.get()/singleKeyFreq.get());
					result.set(result.get()/singleKeyFreq.get());
				}
				System.out.println("result after RF - " +result);
				context.write(key, result);
			}
			
		} catch (Exception e) {
			System.out.println("ERROR - " + e.getMessage());
			e.printStackTrace();
		}
	}
}
