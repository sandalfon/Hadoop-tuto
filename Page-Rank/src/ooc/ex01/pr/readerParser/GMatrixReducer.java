package ooc.ex01.pr.readerParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex01.pr.myWritable.PageValueWritable;

public class GMatrixReducer extends Reducer<IntWritable, PageValueWritable, IntWritable, PageValueWritable> {

    private DoubleWritable pageOutFreq = new DoubleWritable();

    @Override
    public void reduce(final IntWritable key, final Iterable<PageValueWritable> values,
            final Context context) throws IOException, InterruptedException {

       int sum = 0;
        ArrayList<Integer> indexes = new ArrayList<Integer>();
        
        for(PageValueWritable page : values) {
        	sum++;
        	indexes.add(page.getPageIndex().get());
        }
        
        context.write(key, new PageValueWritable(new IntWritable( context.getConfiguration().getInt("nbPage", 0)), pageOutFreq));
        pageOutFreq.set(1.0/sum);
        for(int index : indexes) {
        	context.write(key, new PageValueWritable(new IntWritable(index), pageOutFreq));
        }
    }
}



