package ooc.ex01.pr;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
import ooc.ex01.pr.myWritable.ShortArrayWritable;
import ooc.ex01.pr.parserWriter.ParseAndWriteMapper;
import ooc.ex01.pr.parserWriter.ParseAndWriteReducer;
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


	/*
	 Todo : prendre les args depuis un fichier conf ou en ligne de cmd 

	 */ 


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
		//Formatage du fichier des liens et recuperation du nombre de lien
		format(conf,inputFilePath);
		conf.setInt("nbPages", nbPages);
		//chargement parsage des données
		System.out.println("Start parse input");
		runParseInput(inputFilePath, outputFilePath, conf);
		System.out.println("End parse input");

		// iteration pour la puissance lors du calcul du page rank
		for (int iter = 1; iter <= iteration; iter++) {
			conf.setInt("iterations", iter);
			System.out.println("Start iter "+iter);
			runIteration(iter, outputFilePath, conf);
			System.out.println("End iter "+iter);
			System.out.println("Start Clean "+iter);
			cleanPreviousIteration(iter, outputFilePath, conf);
			System.out.println("End Clean "+iter);
		}

		// preparation et ecriture de la sortie
		System.out.println("Start parse output ");
		parseAndWrite(iteration, outputFilePath, conf);
		System.out.println("End parse output");
	}


	//formatage fichier d'entree de OOC
	private static void format(Configuration conf, Path inputFilePath) throws IOException {
		nbPages = 0;
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

	}


	public static void runParseInput(Path inputFilePath, Path outputFilePath, Configuration conf) throws Exception {

		// Creation d'un job 
		Job job = Job.getInstance(conf);
		job.setJobName("pr:matrix");


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
		job.setOutputKeyClass(ShortArrayWritable.class);
		job.setOutputValueClass(MatrixBlockWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputFilePath+"/"+fileGraph));
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath, "M"));

		job.waitForCompletion(true);

	}

	
	//calcul du score
	private static void runIteration(int iter, Path outputFilePath, Configuration conf)
			throws Exception {
		/*
		 * version avec bloque de matrice dans l'hypothèse ou M est tres grand
		 *  pour chaque bloque M_i,j on recupère le partie du vecteur v_n-1 correspondant et 
		 *  on calcule le vecteur v_n
		 *  dans le reduce on somme les v_k et on ajoute la "teleportation"
		 */

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
		FileInputFormat.addInputPath(job, new Path(outputFilePath, "M"));
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath, "v" + iter));

		job.waitForCompletion(true);
	}

	private static void parseAndWrite(int iter, Path outputFilePath, Configuration conf)
			throws Exception {

		/*
		 * Formatage et selection des N meilleurs score
		 * */

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
		FileInputFormat.addInputPath(job, new Path(outputFilePath, "v" + iter));
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath, "v" + iter + "-top" + topResults));

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}


	//nettoyage entre chaque iteration
	private static void cleanPreviousIteration(int iter, Path outputFilePath, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path prevIterDir = new Path(outputFilePath, "v" + (iter - 1));
		fs.delete(prevIterDir, true);
		fs.close();
	}
}