package ooc.ex01.pr.myWritable;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

public class MatrixBlockWritable extends TwoDArrayWritable {

	public MatrixBlockWritable() {
		super(ShortWritable.class);
	}

	public MatrixBlockWritable(Writable[][] values) {
		super(ShortWritable.class, values);
	}
	
	@Override
	public String toString() {
		String res ="[";
		for(int i = 0; i<this.get().length; i++) {
			res = res +"[";
			for(int j = 0; j< this.get()[i].length; j++) {
				res = res+"\t"+this.get()[i][j];
			}
			
			res = res+"]\n";
		}
		res = res+"]";
		return res;
	}
}

