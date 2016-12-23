package com.Parser;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.Parser.PageRank_Driver.DANGLING_NODE;

public class Reducer100 extends Reducer<DoubleWritable,Text,Text,Text> {
	
	// Override the run() to endure that reduce runs only 100 times
	  @Override
	  public void run(Context context) throws IOException, InterruptedException {
	    setup(context);

	    while (context.nextKey()) {
	        if (context.getCounter(DANGLING_NODE.top100).getValue()<100) {
	        reduce(context.getCurrentKey(), context.getValues(), context);
	        } else {
	        	
	            break;
	        }
	    }
	    cleanup(context);
	  }

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// convert scientific notation to decimal format
		NumberFormat formatter = new DecimalFormat("0.00000000000000000000");
		Text pageRank = new Text(formatter.format(key.get()));
			for (Text t : values) {
				
				// emit the node:pagerank pair and increment global counter to keep count of values emitted
				context.write(t, pageRank);
				context.getCounter(DANGLING_NODE.top100).increment(1);
			}
	}
}