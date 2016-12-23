package com.Parser;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.Parser.PageRank_Driver.DANGLING_NODE;



public class PageRank_Reducer extends Reducer<Text,Node,Text,Node> {
	final double alpha = 0.85;
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int numOfNodes = Integer.parseInt(conf.get("node_count"));
		double pr = 0, dangling =0;
		
		// if we find the dummy key, calculate total dangling node loss and write it to a global counter. this value will be used 
		// in the next mapper to account for the dangling node loss
		if(key.toString().equals("~DummyKey|")){
			for(Node n : values){
				if(n.getAdj().isEmpty())
					dangling += n.getPageRank();
			}
			double perNode = dangling / numOfNodes;
			context.getCounter(DANGLING_NODE.massLoss).increment((long) (perNode*1000000000));
				}
		else{
			LinkedList<Text> adj = new LinkedList<Text>();
			for(Node n : values){
				
				// if adj. list is empty, add to running sum of pagerank values
				if(n.getAdj().isEmpty()){
					pr += n.getPageRank();
				}
				else
					// else we have received the adj list for the node
					adj.addAll(n.getAdj());	
			}
		 
			// calculate the page rank based on the formula
				double newpageRank = ((1-alpha)/numOfNodes)+(alpha*pr);
				context.write(key,new Node(adj,newpageRank));
		}
		
	}

}
