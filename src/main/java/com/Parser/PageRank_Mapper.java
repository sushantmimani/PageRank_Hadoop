package com.Parser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PageRank_Mapper extends Mapper<Object, Text, Text, Node>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		double pageRank = 0;
		long dangling_delta;
		int numOfNodes = Integer.parseInt(conf.get("node_count"));
		String line[] = value.toString().split(":");
		String nid = line[0].trim();
		String temp = line[1].trim();
		String[] adj = temp.substring(1, line[1].length()-1).split(",");
		// Emit the adjacency list for each node
		context.write(new Text(nid), new Node(adj));
		// get the current dangling nodes loss that needs to be added to the page rank of each node
		if(conf.get("delta")!=null){
			 dangling_delta = Long.parseLong(conf.get("delta"));
		}
		else {
			dangling_delta = 0;
		}
		if(line.length==2){
			// set initial page rank in the first execution of the mapper
			pageRank = (1/numOfNodes);
		}
		else{
			// calculate pagerank by taking into account dangling nodes
			pageRank = (Double.parseDouble(line[2])/adj.length)+(dangling_delta/1000000000);
		}
		if(line[1].equals("[]")){
			// for dangling nodes, emit contrbution along with a dummy key
			context.write(new Text("~DummyKey|"),new Node(pageRank));
		}
			
		else{
			
			// if node is not a dangling node, emit contribution to all nodes in adjacency list
			for(String node : adj){
				if(!node.isEmpty())
					context.write(new Text(node.trim()), new Node(pageRank));
			}
		}
		
	}
}
