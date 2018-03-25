package ooc.ex1.wordperdoc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ooc.ex1.myWritable.WordCountWordPerDocWritable;
import ooc.ex1.myWritable.WordCountWritable;
import ooc.ex1.myWritable.WordDocWritable;

public class WordPerDocMapper extends Mapper<LongWritable, Text, Text, WordCountWritable> {

	//private WordDocWritable wordDocWritable = new WordDocWritable();
	//private WordCountWordPerDocWritable wordCountWordPerDocWritable = new WordCountWordPerDocWritable();
	//IntWritable valueOut = new IntWritable(1);
	Text keyOut = new Text();
	WordCountWritable valueOut = new WordCountWritable();
	
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), "\t");
		String docIdWord = tokens.nextToken();
		
		int wordCount = Integer.parseInt(tokens.nextToken());
		tokens = new StringTokenizer(docIdWord, "::");
		keyOut = new Text(tokens.nextToken());
		valueOut.setWord(new Text(tokens.nextToken()));
		valueOut.setCount(new IntWritable(wordCount));

		
		context.write(keyOut, valueOut);
	}
	
}
