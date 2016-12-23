package com.Parser;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// KeyComparator class to sort values in descending order of page rank
public  class Sort100 extends WritableComparator
{
	public Sort100()
	{
		super(DoubleWritable.class,true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		DoubleWritable key1 = (DoubleWritable) w1;
		DoubleWritable key2 = (DoubleWritable) w2;
		int result  = Double.compare(key2.get(),key1.get());
		return result;

	}
}