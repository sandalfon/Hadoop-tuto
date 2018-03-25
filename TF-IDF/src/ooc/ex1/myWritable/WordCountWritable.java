package ooc.ex1.myWritable;
 
 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 
 
 import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
 
public class WordCountWritable implements WritableComparable<WordCountWritable> {
	private Text word = new Text();
 	private IntWritable count = new IntWritable(0);
 
public WordCountWritable() {
 
 	}
 
public WordCountWritable(Text word, IntWritable count) {
 		super();
 		this.count = count;
 		this.word = word;
 	}
 
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
 	@Override
 	public void readFields(DataInput in) throws IOException {
 		count.readFields(in);
 		word.readFields(in);
 	}
 	@Override
 	public void write(DataOutput out) throws IOException {
 		count.write(out);
 		word.write(out);
 
 	}
	public void set(WordCountWritable other) {

 		count = other.getCount();
 		word = other.getWord();
 	}
 	
 	@Override
	public int compareTo(WordCountWritable o) {

 		if(this.getCount() == o.getCount()) {
 			return(this.getWord().compareTo(o.getWord()));
 		}else {
 			return 1;
 		}
 	}
 	
 	
 	
 }