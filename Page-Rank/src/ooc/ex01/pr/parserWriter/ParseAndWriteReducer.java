package ooc.ex01.pr.parserWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ooc.ex01.pr.myWritable.MapEntryKeyComparator;
public class ParseAndWriteReducer extends Reducer<FloatWritable, IntWritable, FloatWritable, Text> {



	private PriorityQueue<Map.Entry<Float, Integer>> topRes;
	private static HashMap<Integer, String> myDic;


	private static  HashMap<Integer,String>readDic(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		HashMap<Integer,String> dic =new HashMap<Integer,String>();
		Path titlesFile = new Path(conf.get("outFileUrl"));
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(titlesFile)));
		String line;
		line=br.readLine();
		String[] splitStr;
		while (line != null){
			splitStr = line.split("\\s");
			dic.put(Integer.parseInt(splitStr[0]),splitStr[1]);
			line=br.readLine();
		}
		br.close();
		return dic;
	}

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {

		Configuration conf = context.getConfiguration();
		int nbTopRes = Integer.parseInt(conf.get("topRes"));


		topRes = new PriorityQueue<Map.Entry<Float, Integer>>(nbTopRes,
				new MapEntryKeyComparator<Float, Integer>());
		myDic = readDic(context);
	}

	@Override
	protected void reduce(FloatWritable inKey,
			Iterable<IntWritable> inValues, Context context)
					throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("topRes"));

		for (IntWritable inValue : inValues) {
			int page = inValue.get();
			float pageRank = inKey.get();
			if (topRes.size() < topResults || pageRank >= topRes.peek().getKey()) {
				topRes.add(new AbstractMap.SimpleEntry<Float, Integer>(pageRank, page));
				if (topRes.size() > topResults) {
					topRes.poll();
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {

		float[] pageRanks = new float[topRes.size()];
		String[] urls  = new String[topRes.size()];		
		for (int i = 0; i <pageRanks.length ; i++) {
			Map.Entry<Float, Integer> entry = topRes.poll();
			pageRanks[i] = entry.getKey();
			urls[i] =myDic.get(entry.getValue());

		}
		//remet la liste en ordre decroissant
		for (int i = pageRanks.length - 1; i >= 0; i--) {
			context.write(new FloatWritable(pageRanks[i]), new Text(urls[i]));
		}
	}
}