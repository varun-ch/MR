package com.cloudwick.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartioner extends Partitioner<TextPair, Text> {

	//key is the textpair
		@Override
		public int getPartition(TextPair key, Text value, int numberOfPartioners) {
			// TODO Auto-generated method stub
			
			int hashValue = key.getDeptId().hashCode()%numberOfPartioners;
			
			
			return hashValue;
			
			
	}

}
