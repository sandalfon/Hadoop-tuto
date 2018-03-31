package ooc.ex01.pr.parserWriter;
import ooc.ex01.pr.myWritable.ShortArrayWritable;
import ooc.ex01.pr.myWritable.MatrixBlockWritable;
import ooc.ex01.pr.myWritable.FloatArrayWritable;
import ooc.ex01.pr.myWritable.MapEntryKeyComparator;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.Map;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public class ParseAndWriteMapper extends Mapper<ShortWritable, FloatArrayWritable, FloatWritable, IntWritable> {
	private Map countMap = new HashMap<>();	
	private PriorityQueue<Map.Entry<Float, Integer>> topN;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("topRes"));

		// This queue keeps the top N elements by PageRank.
		topN = new PriorityQueue<Map.Entry<Float, Integer>>(topResults,
				new MapEntryKeyComparator<Float, Integer>());
	}
	
	
	
	
	public void map(ShortWritable key, FloatArrayWritable value,
			Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		short blockSize = Short.parseShort(conf.get("blockSize"));
		int topResults = Integer.parseInt(conf.get("topRes"));

		Writable[] vStripe = value.get();
		for (int i = 0; i < vStripe.length; i++) {
			int page = 1 + (key.get() - 1) * blockSize + i;
			float pageRank = ((FloatWritable) vStripe[i]).get();

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

		// The mapper outputs the top N pages by PageRank in the partition.
		for (Map.Entry<Float, Integer> entry : topN) {
			context.write(new FloatWritable(entry.getKey()),
					new IntWritable(entry.getValue()));
		}
	}
}
