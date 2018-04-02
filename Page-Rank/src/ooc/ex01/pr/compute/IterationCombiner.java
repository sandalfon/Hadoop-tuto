package ooc.ex01.pr.compute;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import ooc.ex01.pr.myWritable.FloatArrayWritable;


public class IterationCombiner extends Reducer<ShortWritable, FloatArrayWritable,
ShortWritable, FloatArrayWritable> {

	@Override
	protected void reduce(ShortWritable key,
			Iterable<FloatArrayWritable> values, Context context)
					throws IOException, InterruptedException {

		// somme sur les parties du vecteur v_n
		FloatWritable[] vi = null;

		for (FloatArrayWritable value : values) {
			Writable[] partialVi = value.get();

			if (vi == null) {
				// gestion du cas ou le dernier v_k est de taille moindre que le reste des v_k
				vi = new FloatWritable[partialVi.length];
				for (int k = 0; k < vi.length; k++) {
					vi[k] = new FloatWritable(0);
				}
			}

			// somme
			for (int k = 0; k < vi.length; k++) {
				vi[k].set(vi[k].get() + ((FloatWritable) partialVi[k]).get());
			}
		}

		context.write(key, new FloatArrayWritable(vi));
	}
}