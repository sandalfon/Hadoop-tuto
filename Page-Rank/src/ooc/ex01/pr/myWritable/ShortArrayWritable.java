package ooc.ex01.pr.myWritable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ShortArrayWritable extends ArrayWritable implements
WritableComparable<ShortArrayWritable> {

	public ShortArrayWritable() {
		super(ShortWritable.class);
	}

	public ShortArrayWritable(Writable[] values) {
		super(ShortWritable.class, values);
	}

	public int compareTo(ShortArrayWritable that) {
		Writable[] self = this.get();
		Writable[] other = that.get();

		if (self.length != other.length) {
			// compare la taille
			return Integer.valueOf(self.length).compareTo(Integer.valueOf(other.length));
		} else {
			// puis les elements.
			for (int i = 0; i < self.length; i++) {
				short s = ((ShortWritable) self[i]).get();
				short o = ((ShortWritable) other[i]).get();
				if (s != o) return Integer.valueOf(s).compareTo(Integer.valueOf(o));
			}
			return 0;
		}
	}
	
	@Override
	public String toString() {
		String res ="{";
		for(int i = 0; i< this.get().length; i++) {
			res = res+this.get()[i] +"\t";
		}
		res = res+ "}";
		return res;
	}
}