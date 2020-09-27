package p1;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MI_driver 
{
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException 
	{
		Path inputPath1 = new Path("hdfs://localhost:9000/user/jivesh//word1");
		
		Path inputPath2 = new Path("hdfs://localhost:9000/user/jivesh//word2");

		Path output_dir = new Path("hdfs://localhost:9000/user/jivesh/output/");

		Configuration conf = new Configuration();
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job job = new Job(conf, "Multiple input example");
     	job.setJarByClass(MI_driver.class);
		job.setMapperClass(MI_mapper2.class);
		job.setMapperClass(MI_mapper1.class);
		job.setReducerClass(MI_reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// adding the parameters for first input file
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class,MI_mapper1.class);
		
		// adding the parameters for second input file
		MultipleInputs.addInputPath(job, inputPath2, KeyValueTextInputFormat.class,MI_mapper2.class);

		FileOutputFormat.setOutputPath(job, output_dir);
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
