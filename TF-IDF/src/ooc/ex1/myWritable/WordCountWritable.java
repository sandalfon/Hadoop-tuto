package ooc.ex1.myWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordCountWritable  implements WritableComparable<WordCountWritable> {
	private Text word = new Text();
	private IntWritable count =  new IntWritable(0);
	
	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}
	
	public WordCountWritable() {
		
	}
	
	public WordCountWritable(Text word, IntWritable count) {
		super();
		this.word = word;
		this.count = count;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		count.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		count.write(out);
		
	}

	@Override
	public int compareTo(WordCountWritable o) {
		if (this.getCount() == o.getCount()) {
			return this.getWord().compareTo(o.getWord());
		}
		else{
			return 1;
		}
	}
	
	public Text toText() {
		return(new Text(this.getWord()+"::"+this.getCount().toString()));
	}
}
