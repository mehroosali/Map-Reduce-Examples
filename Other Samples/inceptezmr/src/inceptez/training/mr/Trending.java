package inceptez.training.mr;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Trending {
	// hadoop bidata sql hadoop\n
	//.net teradata hadoop bigdata java\n
	//hadoop,1
	//bigdata,1
	//sql,1
	//hadoop,1
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		//context of Hadoop FS or local FS (in no reducer)
		private final static IntWritable one = new IntWritable(1);
		private Text trend = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			//line = hadoop bigdata sql hadoop
			//tokenizer = hadoop,bigdata,sql,hadoop
			while (tokenizer.hasMoreTokens()) {
				trend.set(tokenizer.nextToken());
				context.write(trend, one);
				//bigdata,1 hadoop,1 hadoop,1 sql,1
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//hadoop,<1,1,1>
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Trending");
		job.setJarByClass(Trending.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setCombinerClass(Reduce.class);
		//job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
	
}