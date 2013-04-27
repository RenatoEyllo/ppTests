package com.eyllo.paprika.test.stress.hadoop;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PaprikaReducer extends Reducer<Text, Text, Text , Text>{
		
	public void reduce(Text key,  Iterator<Text> values, Context context) 
	throws IOException, InterruptedException{  
	  while (values.hasNext()) {
		context.write(key, values.next());
	  }
	}
}


