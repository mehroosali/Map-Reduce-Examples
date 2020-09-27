package p1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MI_mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context c) throws IOException,InterruptedException
	{
		//Kane Lui
		String input = value.toString().trim();   //converting input value to string
		String input_array[]=input.split(" ");        // splitting the above input into array 
		
		for(String temp:input_array)
		{
			c.write(new Text(temp), new IntWritable(1));
		}
				
	}
}
