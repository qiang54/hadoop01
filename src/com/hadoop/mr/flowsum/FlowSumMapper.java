package com.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 读取一行数据，截取手机号，上行，下行流量，（k-v）
 * 输出给mapper
 * @author qiang
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

	/**
	 * key是偏移量
	 * value 是一行数据
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//取一行数据
		String line = value.toString();
		//截取信息
		String[] fields = StringUtils.split(line, "\t");
		String phoneNumber = fields[1];
		Long up_flow = Long.parseLong(fields[7]);
		Long down_flow = Long.parseLong(fields[8]);
		
		context.write(new Text(phoneNumber), new FlowBean(phoneNumber, up_flow, down_flow));
	
	}
}
