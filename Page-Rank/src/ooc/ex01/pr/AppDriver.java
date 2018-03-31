package ooc.ex01.pr;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import ooc.ex01.pr.compute.IterationCombiner;
import ooc.ex01.pr.compute.IterationMapper;
import ooc.ex01.pr.compute.IterationReducer;
import ooc.ex01.pr.myWritable.FloatArrayWritable;
import ooc.ex01.pr.myWritable.MatrixBlockWritable;
import ooc.ex01.pr.myWritable.PageValueWritable;
import ooc.ex01.pr.myWritable.ShortArrayWritable;
import ooc.ex01.pr.parserWriter.ParseAndWriteMapper;
import ooc.ex01.pr.parserWriter.ParseAndWriteReducer;
import ooc.ex01.pr.readerParser.GMatrixMapper;
import ooc.ex01.pr.readerParser.GMatrixReducer;
import ooc.ex01.pr.readerParser.MatrixMapper;
import ooc.ex01.pr.readerParser.MatrixReducer;

public class AppDriver  {
	private static String inFileUrl = "nodes";
	private static String outFileUrl = "id_urls";
	private static String fileGraph = "adj_list";
	private static int nbPages = 0;
	private static int blockSize = 5000;
	private static int iteration = 9;
	private static double teleportProb = 0.15;
	private static int topRes = 50;

	/*private static int getNumPages(Configuration conf, Path titlesDir)
			throws Exception {

		int numPages = 0;

		IntWritable pageNumber = new IntWritable();
		MapFile.Reader[] readers = MapFileOutputFormat.getReaders(titlesDir, conf);
		for (int i = 0; i < readers.length; i++) {
			readers[i].finalKey(pageNumber);
			if (pageNumber.get() > numPages) {
				numPages = pageNumber.get();
			}
		}
		for (MapFile.Reader reader : readers) {
			reader.close();
		}

		return numPages;
	}*/

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);		

		Configuration conf = new Configuration();
		conf.setInt("blockSize", blockSize);
		conf.setDouble("teleportProb", teleportProb);
		conf.setInt("topRes", topRes);
		conf.setStrings("outFileUrl", args[0]+"/"+outFileUrl+"/data");
		nbPages = format(conf,inputFilePath);
		conf.setInt("nbPages", nbPages);
		System.out.println("Start parse input");
		runParseInput(inputFilePath, outputFilePath, conf);
		System.out.println("End parse input");
		for (int iter = 1; iter <= iteration; iter++) {
			conf.setInt("iterations", iter);
			System.out.println("Start iter "+iter);
			runIteration(iter, outputFilePath, conf);
			System.out.println("End iter "+iter);
			System.out.println("Start Clean "+iter);
			cleanPreviousIteration(iter, outputFilePath, conf);
			System.out.println("End Clean "+iter);
		}
		System.out.println("Start parse output ");
		parseAndWrite(iteration, outputFilePath, conf);
		System.out.println("End parse output");
	}


	private static int format(Configuration conf, Path inputFilePath) throws IOException {
		int nbPages = 0;
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(inputFilePath+"/"+inFileUrl))));
		String line;
		line=br.readLine();

		Path outFile = new Path(inputFilePath+"/"+outFileUrl+"/data");
		FSDataOutputStream out = fs.create(outFile);
		BufferedWriter bw  = new BufferedWriter(new OutputStreamWriter(out));

		while (line != null){
			if(line.startsWith("http")) {

				nbPages++;
				bw.write(nbPages+"\t"+line+"\n");
			}
			line=br.readLine();
		}
		br.close();
		bw.close();
		out.close();
		return nbPages;
	}

	public static void runParseInput(Path inputFilePath, Path outputFilePath, Configuration conf) throws Exception {

		// Creation d'un job en lui fournissant la configuration et une description textuelle de la tache
		Job job = Job.getInstance(conf);
		job.setJobName("pr:matrix");

		// On precise les classes MyProgram, Map et Reduce
		job.setJarByClass(AppDriver.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MatrixMapper.class);
		job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
		job.getConfiguration().setClass("mapreduce.map.output.compress.codec",
				DefaultCodec.class, CompressionCodec.class);
		job.setMapOutputKeyClass(ShortArrayWritable.class);
		job.setMapOutputValueClass(ShortArrayWritable.class);
		job.setReducerClass(MatrixReducer.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(ShortArrayWritable.class);
		job.setOutputValueClass(MatrixBlockWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputFilePath+"/"+fileGraph));
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath, "M"));

		job.waitForCompletion(true);
		
	}

	private static void runIteration(int iter, Path outputDir, Configuration conf)
			throws Exception {

		// This job performs an iteration of the power iteration method to
		// compute PageRank. The map task processes each block M_{i,j}, loads
		// the corresponding stripe j of the vector v_{k-1} and produces the
		// partial result of the stripe i of the vector v_k. The reduce task
		// sums all the partial results of v_k and adds the teleportation factor
		// (the combiner only sums all the partial results). See Section 5.2
		// (and 5.2.3 in particular) of Mining of Massive Datasets
		// (http://infolab.stanford.edu/~ullman/mmds.html) for details. The
		// output is written in a "vk" subdir of the output dir, where k is the
		// iteration number. MapFileOutputFormat is used to keep an array of the
		// stripes of v.
		
		Job job = Job.getInstance(conf, "pr:Iteration");

		job.setJarByClass(AppDriver.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(IterationMapper.class);
		job.setMapOutputKeyClass(ShortWritable.class);
		job.setMapOutputValueClass(FloatArrayWritable.class);
		job.setCombinerClass(IterationCombiner.class);
		job.setReducerClass(IterationReducer.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		job.setOutputKeyClass(ShortWritable.class);
		job.setOutputValueClass(FloatArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(outputDir, "M"));
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "v" + iter));

		job.waitForCompletion(true);
	}

	private static void parseAndWrite(int iter, Path outputDir, Configuration conf)
			throws Exception {

		// This job creates a plain text file with the top N PageRanks and the
		// titles of the pages. Each map task emits the top N PageRanks it
		// receives, and the reduce task merges the partial results into the
		// global top N PageRanks. A single reducer is used in the job in order
		// to have access to all the individual top N PageRanks from the
		// mappers. The reducer looks up the titles in the index built by
		// TitleIndex. This job was designed considering that N is small.

		int topResults = Integer.parseInt(conf.get("topRes"));

		Job job = Job.getInstance(conf, "pr:TopN");

		job.setJarByClass(AppDriver.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(ParseAndWriteMapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ParseAndWriteReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(outputDir, "v" + iter));
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "v" + iter + "-top" + topResults));

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}



	private static void cleanPreviousIteration(int iter, Path outputDir, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path prevIterDir = new Path(outputDir, "v" + (iter - 1));
		fs.delete(prevIterDir, true);
		fs.close();
	}
}