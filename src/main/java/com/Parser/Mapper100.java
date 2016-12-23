package com.Parser;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper100 extends Mapper<Object, Text, DoubleWritable, Text>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Split the records on : and emit only the Node and the PageRank value
		String line[] = value.toString().split(":");
		context.write(new DoubleWritable(Double.parseDouble(line[2])), new Text(line[0]));
		
	}
}