package com.hadoop.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class wordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	
	//经过mapper之后，数据变为：（key,values{}) <hello,{1,1,1,1,1,}>
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context) 
			throws IOException, InterruptedException {
	long count = 0;
	for (LongWritable value : values) {
		count += value.get();
	}
	 
	context.write(key, new LongWritable(count));
	}
	

	

}
