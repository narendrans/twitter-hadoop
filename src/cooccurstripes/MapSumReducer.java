package cooccurstripes;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class MapSumReducer extends Reducer<Text,MapWritable,Text,DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	private MapWritable mapResult = new MapWritable();
	private IntWritable mapCount = new IntWritable(1);	

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		//String keyString = ""; 
		System.out.println("Reduce Values  : " + values);
		for (MapWritable valueMap : values) {
			Set<Writable> keySet =  valueMap.keySet();			
			for (Writable keyMap : keySet) 
			{
				System.out.println("Key Map : " + keyMap);
				if(!mapResult.containsKey(keyMap))
				{	
					System.out.println("Key doesnt exist. so initiating 1 for keymap : " + keyMap);
					mapCount.set(1);
					mapResult.put(keyMap, mapCount);
				}
				else
				{					
					mapCount = (IntWritable) mapResult.get(keyMap);
					mapCount.set(mapCount.get() + 1);
					mapResult.put(keyMap, mapCount);
					System.out.println("Key exist. so incremented map count for Keymap : " + keyMap + " cnt : "+mapCount);
				}				
			}
		}
		
		// Calculating relative frequency by getting the pairs frequency and single key frequency from above mapResult HashMap
		Text cooccurKeyText = new Text();
		for (Entry<Writable, Writable> mapResultEntry : mapResult.entrySet()) 
		{			
			if(mapResultEntry.getKey().toString().compareTo(key.toString()) != 0)
			{
				double singleKeyFrequency = ((DoubleWritable) mapResult.get(key)).get();
				double pairsFrequency = ((DoubleWritable) mapResultEntry.getValue()).get();
				result.set(pairsFrequency / singleKeyFrequency);
				cooccurKeyText.set(key.toString() + "," + mapResultEntry.getKey().toString());				
				context.write(cooccurKeyText, result);
			}
			
			
			
			// To sort based to ignore the order across pairs, eg A,B =B,A  
			/*if(key.toString().compareTo(mapResultEntry.getKey().toString()) < 0)
			{				
				result.set(((IntWritable) mapResultEntry.getValue()).get());
				cooccurKeyText.set(key.toString() + "-" +mapResultEntry.getKey().toString()); 
				context.write(cooccurKeyText, result);
			}
			else
			{
				result.set(((IntWritable) mapResultEntry.getValue()).get());
				cooccurKeyText.set(mapResultEntry.getKey().toString() + "-" +key.toString()); 
				context.write(cooccurKeyText, result);
			}*/				
		}		
	}
}
