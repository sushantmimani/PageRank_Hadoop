package com.Parser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.Parser.Bz2WikiParser.ParserMapper;
import com.Parser.Bz2WikiParser.numOfNodes;


public class PageRank_Driver {


	static long numNodes;
	
	// Global counters for the Hadoop program
	public  enum DANGLING_NODE {
		  massLoss,
		  top100;
	}
	
	// Map Reduce job for the parser
	public static void parserjob(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":");
		Job job = Job.getInstance(conf, "wiki parser");
		job.setJarByClass(PageRank_Driver.class);
		job.setMapperClass(ParserMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
    	FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	    // Store the number of nodes to calculate the initial pagerank value
	    numNodes = job.getCounters().findCounter(numOfNodes.COUNT).getValue();
	}
	
	// the processor job which executes 10 times to compute pageranks
	public static void wikiprocessorjob(String in) throws IOException, ClassNotFoundException, InterruptedException {
		int counter = 0;
		String input, output = null;
		Configuration conf = new Configuration();
		while(counter<10){ // run loop 10 times
			if(counter==0)
				// set input and output paths based on number of iteration
				input = in;
			else
				input = in+counter;
			output = in+(counter+1);
			conf.set("mapred.textoutputformat.separator", ":");
			// create a variable that stores number of nodes to be accesssed in mapper and reducer
			conf.setLong("node_count", numNodes);
			Job job = Job.getInstance(conf, "wiki parser");
			job.setJarByClass(PageRank_Driver.class);
		    job.setMapperClass(PageRank_Mapper.class);
		    job.setReducerClass(PageRank_Reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Node.class);
	    	FileInputFormat.addInputPath(job, new Path(input));
		    FileOutputFormat.setOutputPath(job, new Path(output));
		    job.waitForCompletion(true);
		    // update variable to reflect latest pagerank loss at the end of every iteration
		    conf.setLong("delta",job.getCounters().findCounter(DANGLING_NODE.massLoss).getValue());
		    counter++;
	    }
	}
	
	// Map reduce job to display the top 100 pageranks
	public static void gettop100(String in) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":");
		Job job = Job.getInstance(conf, "wiki parser");
		job.setJarByClass(PageRank_Driver.class);
	    job.setMapperClass(Mapper100.class);
	    job.setReducerClass(Reducer100.class);
	    job.setSortComparatorClass(Sort100.class);
	    job.setNumReduceTasks(1);
	    job.setMapOutputKeyClass(DoubleWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(in+10));
	    FileOutputFormat.setOutputPath(job, new Path(in+"Top100"));
	    job.waitForCompletion(true);
	}
	
public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		parserjob(args[0],args[1]);
		wikiprocessorjob(args[1]);
	    gettop100(args[1]);
	}
}

