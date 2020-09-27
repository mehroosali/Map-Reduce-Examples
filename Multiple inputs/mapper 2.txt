package p1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MI_mapper2 extends Mapper<Text, Text, Text, IntWritable> {
// key1,Brady
	public void map(Text key, Text value, Context context)throws IOException, InterruptedException
	{
	
		context.write(value, new IntWritable(1));

	}
}
