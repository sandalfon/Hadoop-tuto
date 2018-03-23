package ooc.ex1.wordperdoc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ooc.ex1.myWritable.WordDocWritable;

public class WordPerDocMapper extends Mapper<LongWritable, Text, WordDocWritable, IntWritable> {

}
