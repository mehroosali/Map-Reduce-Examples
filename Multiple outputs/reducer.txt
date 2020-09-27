package com.hadoop.multioutput;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//   DJPX255251        [{ Arthur,HR,6397} { Arthur,HR,6597} {Arthur,HR,6797} ]
public class MultiOutReducer extends Reducer<Text, Text, Text, Text>
{

    private MultipleOutputs<Text, Text> out;

    @Override
    protected void setup(Context c)	throws IOException, java.lang.InterruptedException
    {
	out = new MultipleOutputs(c);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
    {

	int totalSalary = 0;
	String dept = "";
	String name = "";
	
	Iterator<Text> valuesIter = values.iterator();

	while (valuesIter.hasNext())
	{
	    /* name,department,salaray */
	    String[] data = valuesIter.next().toString().split(",");     //[{ Arthur} {HR} {6397}]
	    name = data[0];                                   //  name = Arthur 
	    dept = data[1];                                   //  dept = HR
	    totalSalary += Integer.parseInt(data[2]);         //  totalSalary  = 19791 
	}

	/* output employee salaray to department file */
	if (dept.equalsIgnoreCase("hr"))
	{
	    out.write("HR", key, new Text(name + "," + totalSalary));
	}
	else if (dept.equalsIgnoreCase("accounts"))
	{
	    out.write("Accounts", key, new Text(name + "," + totalSalary));
	}
    }

    @Override
    protected void cleanup(Context c)throws IOException,java.lang.InterruptedException
    {
	out.close();
    }
}
