package com.cloudwick.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DeptMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String[] dept = value.toString().split(",");
		
		String deptId =dept[0];
		String deptName = dept[1];
		
		context.write(new TextPair(new Text(deptId), new IntWritable(0)) , new Text(deptName));
		
	}

}
