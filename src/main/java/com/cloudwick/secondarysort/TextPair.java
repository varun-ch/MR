package com.cloudwick.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{
	
	private Text deptId;
	private IntWritable identifier;

	public TextPair() {
		this.deptId=new Text();
		this.identifier=new IntWritable();
	}
	
	public TextPair(Text text, IntWritable intWritable) {
		// TODO Auto-generated constructor stub
		
		this.deptId=text;
		this.identifier=intWritable;
		
		
	}

	public Text getDeptId() {
		return deptId;
	}

	public void setDeptId(Text deptId) {
		this.deptId = deptId;
	}

	public IntWritable getIdentifier() {
		return identifier;
	}

	public void setIdentifier(IntWritable identifier) {
		this.identifier = identifier;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		deptId.readFields(arg0);
		identifier.readFields(arg0);
		
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		deptId.write(arg0);
		identifier.write(arg0);
		
	}

	public int compareTo(TextPair obj) {
		// TODO Auto-generated method stub
		
		int result = this.deptId.compareTo(obj.deptId);
		if(result !=0){
			return result;
		}
		else{
			return this.identifier.compareTo(obj.identifier);
		}
		
		
	
		
		
	}

	public int compareTo2(TextPair tp2) {
		// TODO Auto-generated method stub
		
		return this.deptId.compareTo(tp2.deptId);
		
	} 
	
	

}
