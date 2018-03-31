package ooc.ex01.pr.readerParser;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import ooc.ex01.pr.myWritable.PageValueWritable;



public class GMatrixMapper extends Mapper<LongWritable, Text, IntWritable, PageValueWritable> {
	private int index1 = 0;
	private int index2 = 0;
	private final static DoubleWritable one = new DoubleWritable(1.0);
	private PageValueWritable pageValueWritable = new PageValueWritable();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer tokenizer = new StringTokenizer( value.toString().replace(" -1",""),": ");
		index1 = Integer.parseInt(tokenizer.nextToken());
		if(tokenizer.hasMoreTokens()) {
			while(tokenizer.hasMoreTokens()) {
				index2 = Integer.parseInt(tokenizer.nextToken());
				pageValueWritable = new PageValueWritable(new IntWritable(index2), one);
				context.write(new IntWritable(index1), pageValueWritable);
			}
		}else {
			index2 = -1;
			pageValueWritable = new PageValueWritable(new IntWritable(index2), one);
			context.write(new IntWritable(index1), pageValueWritable);
		}

	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}


}
