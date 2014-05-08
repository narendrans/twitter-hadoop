package cooccurpair;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerWordCountPair extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int count) {
		int partition = key.hashCode() % count;		
		return partition;
	}

}

