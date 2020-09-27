package com.hadoop.salaryincrement;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.net.URISyntaxException;

public class SalaryInc
{
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,URISyntaxException
    {
	
	Path inputPath = new Path("hdfs://localhost:9000/user/jivesh/emp.txt");
	Path outputDir = new Path("hdfs://localhost:9000/user/jivesh/output/");
	
	Configuration conf = new Configuration();
	// add files to cache 
	DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/jivesh/designation.txt"), conf);

	Job job = new Job(conf, "Salary Increment");

	//name of driver class
	job.setJarByClass(SalaryInc.class);
	//name of Mapper class
	job.setMapperClass(SalaryIncMapper.class);
	//name of Reducer class
	job.setReducerClass(SalaryIncReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	
    FileInputFormat.addInputPath(job, inputPath);        
	FileOutputFormat.setOutputPath(job, outputDir);
	
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	job.waitForCompletion(true);
    }
}
