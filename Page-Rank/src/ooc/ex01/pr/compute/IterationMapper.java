package ooc.ex01.pr.compute;
import ooc.ex01.pr.myWritable.ShortArrayWritable;
import ooc.ex01.pr.myWritable.MatrixBlockWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import ooc.ex01.pr.myWritable.FloatArrayWritable;

public class IterationMapper  extends Mapper<ShortArrayWritable, MatrixBlockWritable,
ShortWritable, FloatArrayWritable> {

	@Override
	public void map(ShortArrayWritable key, MatrixBlockWritable value,
			Context context) throws IOException, InterruptedException {

		// This task gets each block M_{i,j}, loads the corresponding stripe j
		// of the vector v_{k-1} and produces the partial result of the stripe i
		// of the vector v_k.

		Configuration conf = context.getConfiguration();
		int iter = Integer.parseInt(conf.get("iterations"));
		int numPages = Integer.parseInt(conf.get("nbPages"));
		short blockSize = Short.parseShort(conf.get("blockSize"));

		Writable[] blockIndexes = key.get();
		short i = ((ShortWritable) blockIndexes[0]).get();
		short j = ((ShortWritable) blockIndexes[1]).get();

		int vjSize = (j > numPages / blockSize) ? (numPages % blockSize) : blockSize;
		FloatWritable[] vj = new FloatWritable[vjSize];

		if (iter == 1) {
			// Initial PageRank vector with 1/n for all pages.
			for (int k = 0; k < vj.length; k++) {
				vj[k] = new FloatWritable(1.0f / numPages);
			}
		} else {
			// Load the stripe j of the vector v_{k-1} from the MapFiles.
			Path outputDir = MapFileOutputFormat.getOutputPath(context).getParent();
			Path vjDir = new Path(outputDir, "v" + (iter - 1));
			MapFile.Reader[] readers = MapFileOutputFormat.getReaders(vjDir, conf);
			Partitioner<ShortWritable, FloatArrayWritable> partitioner =
					new HashPartitioner<ShortWritable, FloatArrayWritable>();
			ShortWritable keyPart = new ShortWritable(j);
			FloatArrayWritable valuePart = new FloatArrayWritable();
			MapFileOutputFormat.getEntry(readers, partitioner, keyPart, valuePart);
			Writable[] writables = valuePart.get();
			for (int k = 0; k < vj.length; k++) {
				vj[k] = (FloatWritable) writables[k];
			}
			for (MapFile.Reader reader : readers) {
				reader.close();
			}
		}

		// Initialize the partial result i of the vector v_k.
		int viSize = (i > numPages / blockSize) ? (numPages % blockSize) : blockSize;
		FloatWritable[] vi = new FloatWritable[viSize];
		for (int k = 0; k < vi.length; k++) {
			vi[k] = new FloatWritable(0);
		}

		// Multiply M_{i,j} by the stripe j of the vector v_{k-1} to obtain the
		// partial result i of the vector v_k.
		Writable[][] blockColumns = value.get();
		for (int k = 0; k < blockColumns.length; k++) {
			Writable[] blockColumn = blockColumns[k];
			if (blockColumn.length > 0) {
				int vDegree = ((ShortWritable) blockColumn[0]).get();
				for (int columnIndex = 1; columnIndex < blockColumn.length; columnIndex++) {
					int l = ((ShortWritable) blockColumn[columnIndex]).get();
					float aa = vi[l].get();
					float bb =vj[k].get();
					vi[l].set(aa +  (1.0f / vDegree) * bb);
				}
			}
		}

		context.write(new ShortWritable(i), new FloatArrayWritable(vi));
	}
}