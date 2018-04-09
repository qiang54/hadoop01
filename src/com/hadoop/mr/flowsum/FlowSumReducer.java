package com.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	//接收数据格式:(phoneNumber,{FlowBean,FlowBean,FlowBean,...})
	@Override
	protected void reduce(Text phoneNumber, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {

		Long up_flow = 0L;
		Long down_flow = 0L;
		
		for (FlowBean flowBean : values) {
			up_flow += flowBean.getUp_flow();
			down_flow += flowBean.getDown_flow();
		}
		
		context.write(phoneNumber, new FlowBean(phoneNumber.toString(), up_flow, down_flow));
	 
	}
}
