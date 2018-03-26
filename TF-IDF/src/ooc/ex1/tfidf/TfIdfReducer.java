package ooc.ex1.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex1.myWritable.WordDocWritable;

public class TfIdfReducer extends Reducer<Text, Text, Text, Text> {
	private WordDocWritable wordDocWritable = new WordDocWritable();
	public void reduce(final Text key, final  Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		HashMap<WordDocWritable, Double> tfs = new HashMap<WordDocWritable, Double>(); 
		int n = context.getConfiguration().getInt("nbInputDoc", 0);
		int nbWordInDoc = 0;
		int count;
		int perDoc;
		double idf =0.0;
		double tf;
		Text docId;

		for(Text value : values) {
			nbWordInDoc ++;
			StringTokenizer tokens = new StringTokenizer(value.toString(), "::");
			docId = new Text(tokens.nextToken());
			count = Integer.parseInt(tokens.nextToken());
			perDoc = Integer.parseInt(tokens.nextToken());
			wordDocWritable = new WordDocWritable(new Text(key), new Text(docId));
			tf = (count*1.0)/(perDoc*1.0);
			tfs.put(wordDocWritable, tf);
		}

		idf=Math.log10((n*1.0)/(nbWordInDoc*1.0));

		for(WordDocWritable wordDoc : tfs.keySet()) {
			context.write(wordDoc.toOutput(), new Text("with Tf-Idf = "+tfs.get(wordDoc)*idf+""));
		}	
	}
}
