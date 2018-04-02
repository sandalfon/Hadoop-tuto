package ooc.ex01.pr.compute;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex01.pr.myWritable.FloatArrayWritable;

public class IterationReducer extends Reducer<ShortWritable, FloatArrayWritable,
ShortWritable, FloatArrayWritable> {

	@Override
	protected void reduce(ShortWritable key,
			Iterable<FloatArrayWritable> values, Context context)
					throws IOException, InterruptedException {

		// Somme des différentes parties du vecteur v_k et ajout de la teleportation
		
		Configuration conf = context.getConfiguration();
		int numPages = Integer.parseInt(conf.get("nbPages"));
		float beta = Float.parseFloat(conf.get("teleportProb"));

		FloatWritable[] vi = null;

		for (FloatArrayWritable value : values) {
			Writable[] partialVi = value.get();

			if (vi == null) {
				// gestion du denier v_k qui peut être incomplet en taille .
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

		// teleporte.
		for (int k = 0; k < vi.length; k++) {
			vi[k].set(beta * vi[k].get() + (1 - beta) / numPages);
		}

		context.write(key, new FloatArrayWritable(vi));
	}

}
