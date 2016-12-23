# PageRank_Hadoop

Goal: Implement PageRank in MapReduce to explore the behavior of an iterative graph algorithm.

Overall Workflow Summary
1. Pre-processing Job: Turns the input Wikipedia data into a graph represented as adjacency lists. 
2. PageRank Job: 10 iterations of PageRank. 
3. Top-k Job: From the output of the last PageRank iteration, get the 100 pages with the highest PageRank and output them, along with their ranks, from highest to lowest.
