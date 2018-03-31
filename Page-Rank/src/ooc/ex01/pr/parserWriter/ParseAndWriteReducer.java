package ooc.ex01.pr.parserWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import ooc.ex01.pr.myWritable.MapEntryKeyComparator;
public class ParseAndWriteReducer extends Reducer<FloatWritable, IntWritable, FloatWritable, Text> {



	private PriorityQueue<Map.Entry<Float, Integer>> topN;
	private static HashMap<Integer, String> myDic;


	private static  HashMap<Integer,String>readDic(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		HashMap<Integer,String> dic =new HashMap<Integer,String>();
		Path titlesFile = new Path(conf.get("outFileUrl"));
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(titlesFile)));
		String line;
		line=br.readLine();
		String[] splitStr;
		while (line != null){
			splitStr = line.split("\\s");
			dic.put(Integer.parseInt(splitStr[0]),splitStr[1]);
			line=br.readLine();
		}
		br.close();
		return dic;
	}

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("topRes"));

		// This queue keeps the top N elements by PageRank.
		topN = new PriorityQueue<Map.Entry<Float, Integer>>(topResults,
				new MapEntryKeyComparator<Float, Integer>());
		myDic = readDic(context);
	}

	@Override
	protected void reduce(FloatWritable inKey,
			Iterable<IntWritable> inValues, Context context)
					throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("topRes"));

		for (IntWritable inValue : inValues) {
			int page = inValue.get();
			float pageRank = inKey.get();

			// The elements in the queue are sorted (in non-decreasing order) by
			// PageRank. The queue is filled up until it contains topResults
			// elements. Then, a new element will be added only if its PageRank
			// is greater than the lowest PageRank in the queue. If the queue is
			// full and a new element is added, the one with the lowest PageRank
			// is removed from the queue.
			if (topN.size() < topResults || pageRank >= topN.peek().getKey()) {
				topN.add(new AbstractMap.SimpleEntry<Float, Integer>(pageRank, page));
				if (topN.size() > topResults) {
					topN.poll();
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {

		float[] pageRanks = new float[topN.size()];
		String[] urls  = new String[topN.size()];		
		for (int i = 0; i <pageRanks.length ; i++) {
			Map.Entry<Float, Integer> entry = topN.poll();
			pageRanks[i] = entry.getKey();
			urls[i] =myDic.get(entry.getValue());

		}
		for (int i = pageRanks.length - 1; i >= 0; i--) {
			context.write(new FloatWritable(pageRanks[i]), new Text(urls[i]));
		}
	}
}