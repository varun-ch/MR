package com.cloudwick.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyComparator extends WritableComparator {

	protected CustomKeyComparator() {
		super(TextPair.class, true);
		// TODO Auto-generated constructor stub
		
	}
	public int compare(WritableComparable obj1, WritableComparable obj2){
			TextPair tp1 = (TextPair)obj1;
			TextPair tp2 = (TextPair)obj2;
			int result = tp1.compareTo(tp2);
			return result;
			
			
			
		}
		
	

}
