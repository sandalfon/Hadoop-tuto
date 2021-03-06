package ooc.ex1.tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TfIdfMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	private Text doc = new Text();
	private IntWritable count = new IntWritable(0);
	private IntWritable perDoc = new IntWritable(0);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), "\t");
		StringTokenizer tokens2 = new StringTokenizer(tokens.nextToken(), "::");
		StringTokenizer tokens3 = new StringTokenizer(tokens.nextToken(), "::");
		doc = new Text(tokens2.nextToken());
		word = new Text(tokens2.nextToken());
		count = new IntWritable(Integer.parseInt(tokens3.nextToken()));
		perDoc = new IntWritable(Integer.parseInt(tokens3.nextToken())); 
		context.write(word, new Text(doc+"::"+count+"::"+perDoc));

	}
}
