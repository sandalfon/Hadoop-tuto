package ooc.ex1.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import ooc.ex1.myWritable.WordDocWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, WordDocWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private WordDocWritable wordDocId;
	private Set<String> patternsToSkip = new HashSet<String>();
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String line = value.toString().toLowerCase().replaceAll("'","").replaceAll("[^\\p{L} ]", "");
		String word2chk;

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word2chk = tokenizer.nextToken();
			if(wordCheck(word2chk)) {
				word.set(word2chk);
				wordDocId = new WordDocWritable(word, new Text(fileName));
				context.write(wordDocId, one);
			}
		}
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		parseSkipFile();
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}


	public boolean wordCheck(String word2chk) {
		if(word2chk.length()<=3) {
			return false;
		}
		if ( patternsToSkip.contains(word2chk)) {
			return false;
		}

		return true;
	}

	private void parseSkipFile() {
		URL url;
		String urlStr = "https://sites.google.com/site/kevinbouge/stopwords-lists/stopwords_en.txt";
		try {
			url = new URL(urlStr);


			BufferedReader fis = new BufferedReader(new InputStreamReader(url.openStream()));
			String pattern;
			while ((pattern = fis.readLine()) != null) {
				patternsToSkip.add(pattern.replaceAll("'","").replaceAll("[^\\p{L} ]", ""));
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '"
					+ urlStr + "' : " + StringUtils.stringifyException(ioe));
		}
	}

}