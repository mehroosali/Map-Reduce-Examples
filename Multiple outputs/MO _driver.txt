package com.hadoop.multioutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutputDriver
{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

	Path inputPath = new Path("hdfs://localhost:9000/user/jivesh/mo");         // input file path
    Path outputDir = new Path("hdfs://localhost:9000/user/jivesh/output/");      // output directory path
	
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Multi Output");

	//name of driver class
	job.setJarByClass(MultiOutputDriver.class);
	//name of mapper class
	job.setMapperClass(MultiOutMapper.class);
	//name of reducer class
	job.setReducerClass(MultiOutReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, inputPath);  //add input path 

 // Set 2 outputs 
	MultipleOutputs.addNamedOutput(job,"HR", TextOutputFormat.class, Text.class, LongWritable.class );

	MultipleOutputs.addNamedOutput(job,"Accounts", TextOutputFormat.class, Text.class, LongWritable.class );

	FileOutputFormat.setOutputPath(job, outputDir);
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
