package com.hadoop.multioutput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

// DJPX255251,Arthur,HR,6397,2016 
public class MultiOutMapper extends Mapper<LongWritable, Text, Text, Text>
{

    private Text empId = new Text();
    private Text empData = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	String line = value.toString();        // DJPX255251,Arthur,HR,6397,2016 
	/* Split csv string */
	String[] words = line.split(",");     // [ {DJPX255251} {Arthur} {HR} {6397} {2016} ]

	empId.set(words[0]);               // empId = DJPX255251
	
	empData.set(words[1] + "," + words[2] + "," + words[3]);
	
	c.write(empId, empData);
	
    }  // DJPX255251  Arthur,HR,6397
}    //     key           value
