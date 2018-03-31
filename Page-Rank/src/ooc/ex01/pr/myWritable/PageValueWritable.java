package ooc.ex01.pr.myWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class PageValueWritable implements WritableComparable<PageValueWritable> {
	private IntWritable pageIndex = new IntWritable(0);
	private DoubleWritable value = new DoubleWritable(0.0);
	
	@Override
	public void readFields(DataInput in) throws IOException {
		pageIndex.readFields(in);
		value.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pageIndex.write(out);
		value.write(out);
		
	}

	@Override
	public int compareTo(PageValueWritable o) {
		if(this.getValue()==o.getValue()) {
			if(this.getPageIndex() == o.getPageIndex()) {
				return 0;
			}
		}
		return 1;
	}

	public IntWritable getPageIndex() {
		return pageIndex;
	}

	public void setPageIndex(IntWritable pageIndex) {
		this.pageIndex = pageIndex;
	}

	public DoubleWritable getValue() {
		return value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}

	public PageValueWritable() {
		
	}
	
	public PageValueWritable(IntWritable pageIndex, DoubleWritable value) {
		super();
		this.pageIndex = pageIndex;
		this.value = value;
	}
	
	public PageValueWritable(String s) {
		super();
		StringTokenizer tokens = new StringTokenizer(s.toString(), "::");
		this.pageIndex =  new IntWritable(Integer.parseInt(tokens.nextToken()));
		this.value =  new DoubleWritable(Double.parseDouble(tokens.nextToken()));
	}
	
	@Override
	public String toString() {
		return this.getPageIndex()+"::"+this.getValue();
	}
	
}
