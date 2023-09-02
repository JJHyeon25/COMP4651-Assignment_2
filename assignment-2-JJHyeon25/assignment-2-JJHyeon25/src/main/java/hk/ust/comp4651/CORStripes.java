package hk.ust.comp4651;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Compute the bigram count using "pairs" approach
 */
public class CORStripes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CORStripes.class);

	/*
	 * TODO: write your first-pass Mapper here
	 */
	private static class CORMapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		//added
		private final static IntWritable ONE = new IntWritable(1);
		private final static Text WORD = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> word_set = new HashMap<String, Integer>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String clean_doc = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizer = new StringTokenizer(clean_doc);
			/*
			 * TODO: Your implementation goes here.
			 */
			while (doc_tokenizer.hasMoreElements()){
				WORD.set(doc_tokenizer.nextToken());
				context.write(WORD, ONE);
			}
		}
	}

	/*
	 * TODO: Write your first-pass reducer here
	 */
	private static class CORReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		//added
		private final static IntWritable SUM = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	/*
	 * TODO: write your second-pass Mapper here
	 */
	public static class CORStripesMapper2 extends Mapper<LongWritable, Text, Text, HashMapStringIntWritable /*MapWritable*/> {

		//added
		private final static Text WORD = new Text();
		private final static HashMapStringIntWritable STRIPE = new HashMapStringIntWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> sorted_word_set = new TreeSet<String>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String doc_clean = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizers = new StringTokenizer(doc_clean);
			while (doc_tokenizers.hasMoreTokens()) {
				sorted_word_set.add(doc_tokenizers.nextToken());
			}
			/*
			 * TODO: Your implementation goes here.
			 */
			while (!(sorted_word_set.isEmpty())){
				Iterator<String> iter = sorted_word_set.iterator();
				WORD.set(iter.next());
				while (iter.hasNext()){
					String w = iter.next();
					if (w.length() == 0) {
						continue;
					}
					STRIPE.increment(w);
					context.write(WORD, STRIPE);
					STRIPE.clear();
				}
				sorted_word_set.remove (WORD.toString());
			}
		}
	}

	/*
	 * TODO: write your second-pass Combiner here
	 */
	public static class CORStripesCombiner2 extends Reducer<Text, HashMapStringIntWritable /*MapWritable*/, Text, HashMapStringIntWritable /*MapWritable*/> {
		static IntWritable ZERO = new IntWritable(0);
		//added
		private final static HashMapStringIntWritable SUM_STRIPES = new HashMapStringIntWritable();

		@Override
		protected void reduce(Text key, Iterable<HashMapStringIntWritable /*MapWritable*/> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			Iterator<HashMapStringIntWritable> iter = values.iterator();

			while (iter.hasNext()) {
				for ( String second_w : iter.next().keySet() ) {
					SUM_STRIPES.increment(second_w);
				}
			}
			context.write(key, SUM_STRIPES);
			SUM_STRIPES.clear();
		}
	}

	/*
	 * TODO: write your second-pass Reducer here
	 */
	public static class CORStripesReducer2 extends Reducer<Text, HashMapStringIntWritable /*MapWritable*/, PairOfStrings, DoubleWritable> {

		//added
		private static Map<String, Integer> word_total_map = new HashMap<String, Integer>();
		private static IntWritable ZERO = new IntWritable(0);
		//added
		private final static HashMapStringIntWritable SUM_STRIPES = new HashMapStringIntWritable();
		private final static PairOfStrings BIGRAM = new PairOfStrings();
		private final static DoubleWritable FREQ = new DoubleWritable();

		/*
		 * Preload the middle result file.
		 * In the middle result file, each line contains a word and its frequency Freq(A), seperated by "\t"
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path middle_result_path = new Path("hdfs://quickstart.cloudera:9000/user/root/mid/part-r-00000");
			Configuration middle_conf = new Configuration();
			try {
				FileSystem fs = FileSystem.get(URI.create(middle_result_path.toString()), middle_conf);

				if (!fs.exists(middle_result_path)) {
					throw new IOException(middle_result_path.toString() + "not exist!");
				}

				FSDataInputStream in = fs.open(middle_result_path);
				InputStreamReader inStream = new InputStreamReader(in);
				BufferedReader reader = new BufferedReader(inStream);

				LOG.info("reading...");
				String line = reader.readLine();
				String[] line_terms;
				while (line != null) {
					line_terms = line.split("\t");
					word_total_map.put(line_terms[0], Integer.valueOf(line_terms[1]));
					LOG.info("read one line!");
					line = reader.readLine();
				}
				reader.close();
				LOG.info("finished！");
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

		/*
		 * TODO: write your second-pass Reducer here
		 */
		@Override
		protected void reduce(Text key, Iterable<HashMapStringIntWritable /*MapWritable*/> values, Context context) throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			Iterator<HashMapStringIntWritable> iter = values.iterator();
			String first_w = key.toString();
			while (iter.hasNext()) {
				SUM_STRIPES.plus(iter.next());
			}

			for (Entry<String, Integer> mapElement : SUM_STRIPES.entrySet()) {
				String second_w = (String) mapElement.getKey();
				int value1 = word_total_map.get(first_w);
				int value2 = word_total_map.get(second_w);
				double sum = (double) SUM_STRIPES.get(second_w);
				BIGRAM.set(first_w, second_w);
				FREQ.set(sum/(value1 * value2));
				context.write(BIGRAM, FREQ);
			}

			SUM_STRIPES.clear();
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public CORStripes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(NUM_REDUCERS));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		// Lack of arguments
		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String middlePath = "mid";
		String outputPath = cmdline.getOptionValue(OUTPUT);

		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + CORStripes.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - middle path: " + middlePath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Setup for the first-pass MapReduce
		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "Firstpass");

		job1.setJarByClass(CORStripes.class);
		job1.setMapperClass(CORMapper1.class);
		job1.setReducerClass(CORReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(middlePath));

		// Delete the output directory if it exists already.
		Path middleDir = new Path(middlePath);
		FileSystem.get(conf1).delete(middleDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		// Setup for the second-pass MapReduce

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf1).delete(outputDir, true);


		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Secondpass");

		job2.setJarByClass(CORStripes.class);
		job2.setMapperClass(CORStripesMapper2.class);
		job2.setCombinerClass(CORStripesCombiner2.class);
		job2.setReducerClass(CORStripesReducer2.class);

		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(HashMapStringIntWritable /*MapWritable*/.class);
		job2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));

		// Time the program
		startTime = System.currentTimeMillis();
		job2.waitForCompletion(true);
		LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CORStripes(), args);
	}
}