package ooc.ex1.myWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class WordCountWritable implements WritableComparable<WordCountWritable> {
	private WordDocWritable wordDoc = new WordDocWritable();
	private IntWritable count = new IntWritable(0);

	public WordCountWritable() {

	}

	public WordCountWritable(WordDocWritable wordDoc, IntWritable count) {
		super();
		this.count = count;
		this.wordDoc = wordDoc;
	}

	public WordDocWritable getWordDoc() {
		return wordDoc;
	}
	public void setWordDoc(WordDocWritable wordDoc) {
		this.wordDoc = wordDoc;
	}
	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		count.readFields(in);
		wordDoc.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		count.write(out);
		wordDoc.write(out);

	}
	
	public void set(WordCountWritable other) {
		count = other.getCount();
		wordDoc = other.getWordDoc();
	}
	
	@Override
	public int compareTo(WordCountWritable o) {
		if(this.getCount() == o.getCount()) {
			return(this.getWordDoc().compareTo(o.getWordDoc()));
		}else {
			return 1;
		}
	}
}
