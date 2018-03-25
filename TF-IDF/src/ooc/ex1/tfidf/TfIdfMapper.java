package ooc.ex1.tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ooc.ex1.myWritable.WordCountWordPerDocWritable;
import ooc.ex1.myWritable.WordDocWritable;

public class TfIdfMapper extends Mapper<LongWritable, Text, WordDocWritable, WordCountWordPerDocWritable> {
	private WordDocWritable wordDocWritable; //= new WordDocWritable();
	private WordCountWordPerDocWritable countWordPerDocWritable;// = new WordCountWordPerDocWritable();
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
		wordDocWritable = new WordDocWritable(word, doc);
		countWordPerDocWritable = new WordCountWordPerDocWritable(count, perDoc);
		context.write(wordDocWritable, countWordPerDocWritable);
		
	}
}
