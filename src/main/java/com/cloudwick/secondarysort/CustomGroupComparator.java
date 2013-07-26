package com.cloudwick.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupComparator extends WritableComparator {

	protected CustomGroupComparator() {
		super(TextPair.class,true);
		// TODO Auto-generated constructor stub
	}
	
	public int compare(WritableComparable keyA, WritableComparable keyB){
		
		TextPair tp1 = (TextPair)keyA;
		TextPair tp2 = (TextPair)keyB;
		int result = tp1.compareTo2(tp2);
		return result;
		
	}

}
