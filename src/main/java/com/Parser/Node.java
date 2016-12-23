package com.Parser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Node implements Writable {

	private LinkedList<Text> adj = new LinkedList<Text>(); // Data structure to save the adjacency list
	private double pageRank;
	
	public Node(){
		// Needed for serialization
	}
	
	public Node(String[] list){
		// Convert String array to ArrayList<Text>
		for(String node : list){
			Text temp = new Text(node.trim());
			adj.add(temp);
		}
	}
	
	public Node(List<String> list, double rank){
		for(String node : list){
			Text temp = new Text(node.trim());
			adj.add(temp);
		}
		this.pageRank = rank;
	}
	public Node(double rank){	
		this.pageRank = rank;
	}

	public Node(LinkedList<Text> list, double rank){
		this.adj = list;
		this.pageRank = rank;
	}
	
	public LinkedList<Text> getAdj() {
		return adj;
	}

	public void setAdj(LinkedList<Text> adj) {
		this.adj = adj;
	}

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		adj.clear();
		IntWritable size = new IntWritable();
        size.readFields(in);
        int n = size.get();
        while(n-- > 0) {
            Text atom = new Text();
            atom.readFields(in);
            adj.add(atom);
        }
        this.pageRank = in.readDouble();
	    }
	

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		IntWritable size = new IntWritable(adj.size());
        size.write(out);
        for (Text node : adj)
            node.write(out);
        out.writeDouble(pageRank);
	}
	
	
	@Override
	public String toString() {
		// Override this function to control how the value is displayed
		if (this.pageRank!=Double.MIN_VALUE)
			return (adj.toString()+":"+pageRank);
		else
			return (adj.toString());
	}

	
}
