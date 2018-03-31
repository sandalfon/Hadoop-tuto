package ooc.ex01.pr.readerParser;

import ooc.ex01.pr.myWritable.ShortArrayWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex01.pr.myWritable.MatrixBlockWritable;;


public class MatrixReducer  extends Reducer<ShortArrayWritable, ShortArrayWritable, ShortArrayWritable, MatrixBlockWritable> {

	@Override
	public void reduce(ShortArrayWritable key,
			Iterable<ShortArrayWritable> values, Context context)
					throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		short blockSize = Short.parseShort(conf.get("blockSize"));

		short vIndexInBlock, wIndexInBlock, vDegree;
		List<List<Short>> blockColumns = new ArrayList<List<Short>>(blockSize);
		for (int k = 0; k < blockSize; k++) {
			blockColumns.add(new ArrayList<Short>());
		}

		for (ShortArrayWritable value : values) {
			Writable[] blockEntry = value.get();
			vIndexInBlock = ((ShortWritable) blockEntry[0]).get();
			wIndexInBlock = ((ShortWritable) blockEntry[1]).get();
			vDegree = ((ShortWritable) blockEntry[2]).get();

			if (blockColumns.get(vIndexInBlock).isEmpty()) {
				blockColumns.get(vIndexInBlock).add(vDegree);
			}
			blockColumns.get(vIndexInBlock).add(wIndexInBlock);
		}

		ShortWritable[][] blockColumnWritables = new ShortWritable[blockColumns.size()][];
		for (int k = 0; k < blockColumns.size(); k++) {
			List<Short> column = blockColumns.get(k);
			blockColumnWritables[k] = new ShortWritable[column.size()];
			for (int l = 0; l < column.size(); l++) {
				blockColumnWritables[k][l] = new ShortWritable();
				blockColumnWritables[k][l].set(column.get(l).shortValue());
			}
		}

		context.write(key, new MatrixBlockWritable(blockColumnWritables));
	}
}
