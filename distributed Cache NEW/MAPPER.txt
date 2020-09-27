package com.hadoop.salaryincrement;

import java.util.HashMap;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;

public class SalaryIncMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>  
{
                            
    private HashMap<String, Double> desg_map = new HashMap<String, Double>();      // [ [ {MGR: 2} {DLP:5} {HR:6} ] ]
                                                                                                                                 
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
	/* read data from distributed cache */
	BufferedReader br = null;
	Path[] LocalfilesPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	String record = "";
	
	for (Path path : LocalfilesPath)
	{
	    if (path.getName().toString().trim().equals("designation.txt")) 
	    {
		    br = new BufferedReader(new FileReader(path.toString()));
		    record = br.readLine();          // MGR,2  
		    while (record != null) 
		    {
			String data[] = record.split(",");                 //   [ {MGR} {2}]
			/* designation_code, increment_multiplier */
			desg_map.put(data[0].trim(), Double.parseDouble(data[1].trim()));
			record = br.readLine();
		    }		}
	    } 	}
    
    @Override
    // 607949MR,Allison,Developer,1414,4.4
    protected void map(LongWritable key, Text value,  Context context)throws IOException, java.lang.InterruptedException
    {
	
	String line = value.toString();
	/* Split csv string */
	String[] words = line.split(",");                         //  [{607949MR} {Allison} {Developer} {1414} {4.4} ]

		 String designation= words[2] ;         //  designation = Developer
	    
	    double n = 1;
	    if (designation.toString().equalsIgnoreCase("manager"))
	    {
		  n = desg_map.get("MGR");       // n= 2
	    } 
	    else if(designation.toString().equalsIgnoreCase("developer"))
	    {
		  n = desg_map.get("DLP");        // n = 5
	    } 
	    else if(designation.toString().equalsIgnoreCase("hr"))
	    {
		  n = desg_map.get("HR");         // n = 6
	    } 
	    else
	    {
		System.out.println("Invalid designation");
	    }
	    
	    int currentSalary = Integer.parseInt(words[3].trim());
	    double increment = (n/100) * currentSalary;
	  
	    context.write(new Text(designation), new DoubleWritable(increment));
	}
    }

