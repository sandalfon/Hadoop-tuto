package ooc.ex1.myWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordDocWritable implements WritableComparable<WordDocWritable> {
	private Text word = new Text(); 
	private Text docId= new Text();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		docId.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		docId.write(out);
	}

	@Override
	public int compareTo(WordDocWritable o) {
		
		if (this.getDocId().compareTo(o.getDocId())==0) {
			return this.getWord().compareTo(o.getWord());
		}
		else{
			return this.getDocId().compareTo(o.getDocId());
		}
	}
	
	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getDocId() {
		return docId;
	}

	public void setDocId(Text docId) {
		this.docId = docId;
	}

	public void set(WordDocWritable other) {
		word = other.getWord();
		docId = other.getDocId();
	}
	
	public WordDocWritable() {
		
	}
	
	public WordDocWritable(Text word, Text docId) {
		super();
		this.word = word;
		this.docId = docId;
	}

	public Text toText() {
		return(new Text(this.getDocId()+"::"+this.getWord()));
	}
}
