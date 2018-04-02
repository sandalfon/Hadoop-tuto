package ooc.ex01.pr.parserWriter;
import ooc.ex01.pr.myWritable.FloatArrayWritable;
import ooc.ex01.pr.myWritable.MapEntryKeyComparator;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.AbstractMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public class ParseAndWriteMapper extends Mapper<ShortWritable, FloatArrayWritable, FloatWritable, IntWritable> {

	private PriorityQueue<Map.Entry<Float, Integer>> topRes;
	/*
	 * Todo regarder plus en detail priorityQeue
	 * 
	 * */
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {

		Configuration conf = context.getConfiguration();
		int nbTopRes = Integer.parseInt(conf.get("topRes"));

		topRes = new PriorityQueue<Map.Entry<Float, Integer>>(nbTopRes,
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
			/* la queue est ordonnée de façon croissante, elle est rempli tant qu'elle n'est pas pleine
			 * et un ajout sera fait ssi le score du nouvelle element est > au dernier 
			 * alors il est ajoute et celui du rang le plus faible est retire
			 * */
			if (topRes.size() < topResults || pageRank >= topRes.peek().getKey()) {
				topRes.add(new AbstractMap.SimpleEntry<Float, Integer>(pageRank, page));
				if (topRes.size() > topResults) {
					topRes.poll();
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {

		for (Map.Entry<Float, Integer> entry : topRes) {
			context.write(new FloatWritable(entry.getKey()),
					new IntWritable(entry.getValue()));
		}
	}
}
