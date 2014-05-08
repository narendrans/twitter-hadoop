package graph;

import graph.ShortestPath.DISTANCE;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ShortestDistanceReducer 
extends Reducer<Text,Text,Text,Text> {
//	private IntWritable result = new IntWritable();
	//private static final transient Logger LOG = LoggerFactory.getLogger(ShortestPath.class);
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try
		{
			int minDistance = 10000;
			String minDistanceNode = "";
			
			String node = "";		
			String path = "";
			for (Text val : values) 
			{
				System.out.println("reduce value: " + val.toString());
				String[] valArray = val.toString().split(",");				
				if(valArray[0].compareTo("node") == 0)
				{
					node = valArray[1];				
				}				
				
				else if(valArray[0].compareTo("distance") == 0)
				{				
					if(Integer.parseInt(valArray[1]) < minDistance)
					{
						minDistance = Integer.parseInt(valArray[1]);
						minDistanceNode = valArray[2];
					}
					path = valArray[2];
				}
				else
					System.out.println("ERROR - Invalid key type");
			}			
			
			String[] nodeArray = node.split(" ");
			if(!ShortestPath.isShortestPathFound)
			{
				System.out.println(" Deleting input directory as it becomes out output directory...");
				ShortestPath.deleteFolder(context.getConfiguration(), ShortestPath.inputPath);
				FileOutputFormat.setOutputPath(Job.getInstance(context.getConfiguration()), new Path(ShortestPath.inputPath));
			}
			
			context.getCounter(DISTANCE.MIN).increment(minDistance);
			node = nodeArray[0] + " " + minDistance + " " + nodeArray[2] + " " + path + "-" + minDistanceNode;
			FileOutputFormat.setOutputPath(Job.getInstance(context.getConfiguration()), new Path(ShortestPath.inputPath));		
			context.write(key, new Text(node));
		}
		catch(Exception e)
		{
			System.out.println("ERROR - " + e.getMessage());
			e.printStackTrace();
		}
	}
}
