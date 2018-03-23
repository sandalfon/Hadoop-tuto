package ooc.ex1.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex1.myWritable.WordDocWritable;

public class WordCountReducer extends Reducer<WordDocWritable, IntWritable, Text, IntWritable> {

    private IntWritable totalWordCount = new IntWritable();

    public void reduce(final WordDocWritable key, final Iterable<IntWritable> values,
            final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }

        totalWordCount.set(sum);
        // context.write(key, new IntWritable(sum));
        context.write(key.toText(), totalWordCount);
    }
}
