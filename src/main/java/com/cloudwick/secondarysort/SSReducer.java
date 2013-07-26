package com.cloudwick.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SSReducer extends Reducer<TextPair, Text, NullWritable, Text> {

	@Override
	public void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String deptName = null;
		boolean set= false;
		
		for(Text row: values){
			if (set){
				context.write(null, new Text(row.toString()+","+deptName));
			}
			else{
				deptName= row.toString().split(",")[0];
				set= true;
			}
		}
		
	}

}
