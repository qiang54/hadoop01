package com.hadoop.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 必须继承Writable
 * @author qiang
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{

	private String phoneNumber;
	private Long up_flow;
	private Long down_flow;
	private Long sum_flow;
	
	//反射必须无参构造函数
	public FlowBean() {}
	
	public FlowBean(String phoneNumber, Long up_flow, Long down_flow) {
		super();
		this.phoneNumber = phoneNumber;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = up_flow + down_flow;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public Long getUp_flow() {
		return up_flow;
	}

	public void setUp_flow(Long up_flow) {
		this.up_flow = up_flow;
	}

	public Long getDown_flow() {
		return down_flow;
	}

	public void setDown_flow(Long down_flow) {
		this.down_flow = down_flow;
	}

	public Long getSum_flow() {
		return sum_flow;
	}

	public void setSum_flow(Long sum_flow) {
		this.sum_flow = sum_flow;
	}


	@Override
	public String toString() {
		return ""+ up_flow + "\t" + down_flow + "\t" + sum_flow + "";
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(phoneNumber);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		phoneNumber = in.readUTF();
		up_flow = in.readLong();
		down_flow = in.readLong();
		sum_flow = in.readLong();
		
	}

	@Override
	public int compareTo(FlowBean o) {
		return this.sum_flow > o.getSum_flow() ? -1 : 0;
	}

}
