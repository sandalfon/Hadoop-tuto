package ooc.ex1.wordperdoc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ooc.ex1.myWritable.WordCountWritable;
import ooc.ex1.myWritable.WordDocWritable;

public class WordPerDocMapper extends Mapper<LongWritable, Text, Text, WordCountWritable> {

	private Text keyOut = new Text();
	private WordCountWritable valueOut = new WordCountWritable();
	private WordDocWritable wordDoc = new WordDocWritable();
	
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), "\t");
		wordDoc = new WordDocWritable(tokens.nextToken());
		int count = Integer.parseInt(tokens.nextToken());
		valueOut.setWord(new Text(wordDoc.getWord()));
		valueOut.setCount(new IntWritable(count));
		keyOut = new Text(wordDoc.getDocId());
		context.write(keyOut, valueOut);
	}
	
}
