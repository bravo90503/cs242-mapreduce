package edu.ucr.cs242;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static String CLEAR_PUNCTUATION_REGEX = "[\\p{P}&&[^\u0027|\u2019|\u002E|\u002F|\u002D|\\u005C]]";
	public static String CLEAR_TRAILING_PERIODS = "(?!^)\\.+$";
	public static Map<String, Boolean> docsMap = new HashMap<>();

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);
		conf.set("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString());
		Job job = Job.getInstance(conf, "wordcount");

		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CustomWritable.class);

		job.setMapperClass(Map2.class);
		job.setCombinerClass(Combine2.class);
		job.setReducerClass(Reduce2.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setCompressOutput(job, false);
		// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		job.waitForCompletion(true);

		System.out.println("program completed successfully");
	}

// CustomWritable
	public static class Map2 extends Mapper<LongWritable, Text, Text, CustomWritable> {
		private Text word = new Text();

		Pattern ascii = Pattern.compile("^\\p{ASCII}*$"); // ascii only
		Pattern words = Pattern.compile("[a-zA-Z0-9]"); // must have some words or numbers
		Pattern punc = Pattern.compile("\\p{Punct}"); // punctuation

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			long position = key.get() + 1;
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				// except quotes, periods, fwd-backslashes, hyphens
				// token = token.replaceAll(CLEAR_PUNCTUATION_REGEX, "");
				// now remove trailing periods
				// token = token.replaceAll(CLEAR_TRAILING_PERIODS, "");
				// lower case all tokens
				Matcher matcher = ascii.matcher(token);
				if (matcher.find()) {
					Matcher matcher2 = words.matcher(token);
					if (matcher2.find()) {
						Matcher matcher3 = punc.matcher(token);
						if (!matcher3.find()) {
							word.set(token.toLowerCase());
							// emit
							context.write(word, new CustomWritable(filename, 1, (int) position));
							position += token.length() + 1;
							// out - "the" (doc1, 1, 3)

							docsMap.put(filename, true);
						}
					}
					//
				}
			}
		}
	}

	public static class Combine2 extends Reducer<Text, CustomWritable, Text, CustomWritable> {

		public void reduce(Text key, Iterable<CustomWritable> values, Context context)
				throws IOException, InterruptedException {
			Map<String, StringBuilder> m = new HashMap<>();
			// in - "the" (doc1, 1, 3) (doc1, 1, 38) (doc2, 1, 10) (doc2, 1, 67) (doc2, 1,
			// 98) (doc3, 1, 1)
			// out - "the" (2, [doc1:3,38]) (3, [doc2:10,67,98]) (1, [doc3:1])
			long frequency = 0;
			StringBuilder positions = null;
			for (CustomWritable val : values) {
				frequency += val.getFrequency();
				String docId = val.getDocId();
				if (m.get(docId) == null) {
					positions = new StringBuilder();
					positions.append(docId).append(":").append(val.getPosition()).append(",");
					m.put(docId, positions);
				} else {
					positions = m.get(docId);
					positions.append(val.getPosition()).append(",");
					m.put(docId, positions);
				}
			}
			positions.append(";");
			// emit
			context.write(key, new CustomWritable(frequency, positions.toString()));
		}
	}

	public static class Reduce2 extends Reducer<Text, CustomWritable, Text, CustomWritable> {

		public void reduce(Text key, Iterable<CustomWritable> values, Context context)
				throws IOException, InterruptedException {
			// in - "the" (2, [doc1:3,38]) (3, [doc2:10,67,98]) (1, [doc3:1])
			// out - "the" (6, [doc1:3,38][doc2:10,67,98][doc3:1])
			long frequency = 0;
			StringBuilder positions = new StringBuilder();
			for (CustomWritable val : values) {
				// add frequency of terms from each incoming document
				frequency += val.getFrequency();
				positions.append(val.getPositions());
			}

			// emit
			context.write(key, new CustomWritable(docsMap.size(), frequency, positions.toString()));
		}
	}

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}