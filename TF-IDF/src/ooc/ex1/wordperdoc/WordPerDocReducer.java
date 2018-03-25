package ooc.ex1.wordperdoc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex1.myWritable.WordCountWordPerDocWritable;
import ooc.ex1.myWritable.WordCountWritable;
import ooc.ex1.myWritable.WordDocWritable;

public class WordPerDocReducer  extends Reducer<Text, WordCountWritable, Text, Text> {
	private WordCountWordPerDocWritable wordCountWordPerDocWritable; 
	private WordDocWritable wordDocWritable; 


	public void reduce(final Text key, final  Iterable<WordCountWritable> values,
			final Context context) throws IOException, InterruptedException {
		int sum = 0;
		HashMap<WordDocWritable, Integer> memWordDocWodCount = new HashMap<WordDocWritable, Integer>();

		for(WordCountWritable value : values) {
			wordDocWritable = new WordDocWritable(value.getWord(), key);
			memWordDocWodCount.put(wordDocWritable, value.getCount().get());
			sum += value.getCount().get();

		}

		for(WordDocWritable wordDoc : memWordDocWodCount.keySet()) {

			wordCountWordPerDocWritable = new WordCountWordPerDocWritable(new IntWritable(memWordDocWodCount.get(wordDoc)), new IntWritable(sum));
			context.write(wordDoc.toText(), wordCountWordPerDocWritable.toText());
		}



	}
}
