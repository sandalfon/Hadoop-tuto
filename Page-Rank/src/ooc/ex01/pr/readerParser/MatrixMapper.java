package ooc.ex01.pr.readerParser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ooc.ex01.pr.myWritable.ShortArrayWritable;



public class MatrixMapper extends Mapper<LongWritable, Text, ShortArrayWritable, ShortArrayWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		short blockSize = Short.parseShort(conf.get("blockSize"));

		ShortWritable[] blockIndexes = new ShortWritable[2];
		blockIndexes[0] = new ShortWritable();
		blockIndexes[1] = new ShortWritable();

		ShortWritable[] blockEntry = new ShortWritable[3];
		blockEntry[0] = new ShortWritable();
		blockEntry[1] = new ShortWritable();
		blockEntry[2] = new ShortWritable();
		String[] lineParts = value.toString().replace(" -1","").split(":\\s+");
		if(lineParts.length >1 ) {
			String[] vOutlinks = lineParts[1].split("\\s"); 

			int v, w;
			short i, j;

			v = Integer.parseInt(lineParts[0])+1;
			j = (short) ((v - 1) / blockSize + 1);

			for (int k = 0; k < vOutlinks.length; k++) {
				w = Integer.parseInt(vOutlinks[k])+1;
				i = (short) ((w - 1) / blockSize + 1);

				// Index du bloque M_i,j.
				blockIndexes[0].set(i);
				blockIndexes[1].set(j);
				// l'element du  blocque M_i,j correspond au lien de v -> w
				// et on ajoute le degree du noeud
				blockEntry[0].set((short) ((v - 1) % blockSize));
				blockEntry[1].set((short) ((w - 1) % blockSize));
				blockEntry[2].set((short) vOutlinks.length);

				context.write(new ShortArrayWritable(blockIndexes),
						new ShortArrayWritable(blockEntry));
			}
		}
	}
}
