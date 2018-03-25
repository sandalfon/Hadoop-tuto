package ooc.ex1.myWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordCountWordPerDocWritable  implements WritableComparable<WordCountWordPerDocWritable> {
	private IntWritable wordPerDoc = new IntWritable(0);
	private IntWritable wordCount = new IntWritable(0);
	public WordCountWordPerDocWritable() {

	}

	public WordCountWordPerDocWritable(IntWritable wordCount, IntWritable wordPerDoc){
		super();
		this.wordCount = wordCount;
		this.wordPerDoc = wordPerDoc;

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		wordCount.readFields(in);
		wordPerDoc.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		wordCount.write(out);
		wordPerDoc.write(out);
	}

	@Override
	public int compareTo(WordCountWordPerDocWritable o) {
		if(this.getWordPerDoc()==o.getWordPerDoc()) {
			if(this.getWordCount()==o.getWordCount()) {
				return 0;
			}else {
				return 1;
			}
		}
		else {
			return 1;
		}
	}

	public void set(WordCountWordPerDocWritable other) {
		wordCount = other.getWordCount();
		wordPerDoc = other.getWordPerDoc();
	}

	public IntWritable getWordPerDoc() {
		return wordPerDoc;
	}

	public void setWordPerDoc(IntWritable wordPerDoc) {
		this.wordPerDoc = wordPerDoc;
	}

	public IntWritable getWordCount() {
		return wordCount;
	}

	public void setWordCount(IntWritable wordCount) {
		this.wordCount = wordCount;
	}

	public Text toText() {
		
	return new Text(this.getWordCount().toString()+"::"+this.getWordPerDoc().toString());
	}
}
