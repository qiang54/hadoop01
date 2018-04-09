package com.hadoop.wordCount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCountRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		//设置在本地运行到hadoop集群
		//conf.set("mapreduce.job.jar", "wc.jar");
		
		//1.获取job
		Job job = Job.getInstance(conf);
		
		//注意：设置jar
		job.setJarByClass(WordCountRunner.class);
		
		//2.设置mapper/reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(wordCountReducer.class);
		
		//3.设置reducer输入/输出 K-V 类型值
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//4.设置mapper的输入/输出 K-V 类型值
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//5.设置数据路径
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		
		//6.设置输出路径
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output4"));
		
		//7. 将job提交
		job.waitForCompletion(true); 
		
	}
}
