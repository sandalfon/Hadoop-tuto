

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ooc.ex1.myWritable.WordCountWordPerDocWritable;
import ooc.ex1.myWritable.WordCountWritable;
import ooc.ex1.myWritable.WordDocWritable;
import ooc.ex1.wordcount.WordCountMapper;
import ooc.ex1.wordcount.WordCountReducer;
import ooc.ex1.wordperdoc.WordPerDocMapper;
import ooc.ex1.wordperdoc.WordPerDocReducer;

public class AppDriver  {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		Path wordCountPath = new Path("wordCount");
		runWordCount(inputFilePath, wordCountPath);
		runWordPerDoc(wordCountPath, outputFilePath);
	}

	public static void runWordCount(Path inputFilePath, Path outputFilePath) throws Exception {
		Configuration conf = new Configuration();
		// Creation d'un job en lui fournissant la configuration et une description textuelle de la tache
		Job job = Job.getInstance(conf);
		job.setJobName("wordcount");
		
		// On precise les classes MyProgram, Map et Reduce
		job.setJarByClass(AppDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// Definition des types clé/valeur de notre problème
		job.setOutputKeyClass(WordDocWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);



		// On accepte une entree recursive
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		job.waitForCompletion(true);
		System.out.println("wordcount Completed");

	}
	public static void runWordPerDoc(Path inputFilePath, Path outputFilePath) throws Exception {
		Configuration conf = new Configuration();
		// Creation d'un job en lui fournissant la configuration et une description textuelle de la tache
		Job job = Job.getInstance(conf);
		job.setJobName("wordPerDoc");

		// On precise les classes MyProgram, Map et Reduce
		job.setJarByClass(AppDriver.class);
		job.setMapperClass(WordPerDocMapper.class);
		job.setReducerClass(WordPerDocReducer.class);

		// Definition des types clé/valeur de notre problème
		//job.setMapOutputKeyClass(TextWritable.class);
		job.setMapOutputValueClass(WordCountWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordCountWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);



		// On accepte une entree recursive
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		job.waitForCompletion(true);
		System.out.println("WordPerDoc Completed");
	}
	public static void runTfIDF(Path inputFilePath, Path outputFilePath, Path oriPath) throws Exception {
		Configuration conf = new Configuration();
		// Creation d'un job en lui fournissant la configuration et une description textuelle de la tache
		Job job = Job.getInstance(conf);
		job.setJobName("tfIdf");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] userFilesStatusList = fs.listStatus(oriPath);
		final int nbInputDoc = userFilesStatusList.length;
        conf.setInt("nbInputDoc", nbInputDoc);
		
	}
}