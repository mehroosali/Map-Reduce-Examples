package inceptez.training.mr;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class TrendingPartitioner {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text txtMapOutputKey = new Text("");
		private Text txtMapOutputValue = new Text("");

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().length() > 0) {
				String arrEmpAttributes[] = value.toString().split(",");
				txtMapOutputKey.set(arrEmpAttributes[2]);
				txtMapOutputValue.set(arrEmpAttributes[0] + "		" + arrEmpAttributes[1]);
			}
			context.write(txtMapOutputKey, txtMapOutputValue);
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		private Text trend = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text val : values) {
				trend.set(val + "		" + key);
				context.write(NullWritable.get(), trend);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(TrendingPartitioner.class);
		job.setNumReduceTasks(2);
		job.setJobName("TrendingPartitioner");
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapperClass(Map.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

	public static class CustomPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String str=key.toString();
			// return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
			if (str.equals("chennai"))
				return 0;
			else
				return 1;

		}
	}
}