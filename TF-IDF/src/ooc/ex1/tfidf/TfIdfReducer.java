package ooc.ex1.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import ooc.ex1.myWritable.WordCountWordPerDocWritable;
import ooc.ex1.myWritable.WordCountWritable;
import ooc.ex1.myWritable.WordDocWritable;

public class TfIdfReducer extends Reducer<WordDocWritable, WordCountWordPerDocWritable, Text, Text> {

	public void reduce(final Text key, final  Iterable<WordCountWordPerDocWritable> values,
			final Context context) throws IOException, InterruptedException {
		for(WordCountWordPerDocWritable value : values) {
			

		}
	}
}
