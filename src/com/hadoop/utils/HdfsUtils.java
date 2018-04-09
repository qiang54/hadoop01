package com.hadoop.utils;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;


public class HdfsUtils {

	FileSystem fs = null;
	
	@Before
	public void init() throws IOException {
		
		//自动读取classpath下的xxx.xml配置文件，并解析，自动封装到conf对象中
		Configuration conf = new Configuration();
		//也可自己手动设置
		//conf.set("fs.defaultFS", "hdfs://localhost:9000/");
		
		//获取实力对象
		fs = FileSystem.get(conf);
	}
	
	/**
	 * 下载文件
	 * @throws IOException
	 */
	@Test
	public void download() throws IOException {
		
	
		Path src = new Path("hdfs://localhost:9000/mybatis-3.2.8.jar");
		FSDataInputStream in = fs.open(src);
		FileOutputStream out = new FileOutputStream("/Users/qiang/appInstall/download/haha.gz");
		IOUtils.copy(in, out);
	}
	
	/**
	 * 下载文件
	 * @throws IOException
	 */
	@Test
	public void download2() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path("hdfs://localhost:9000/aa/nini.txt"), new Path("/Users/qiang/appInstall/download/nini.gz"));
	}
	
	/**
	 * 上传文件，偏底层
	 * @throws IOException 
	 */
	@Test
	public void upload() throws IOException {
		
		Path src = new Path("hdfs://localhost:9000/aa/nini.txt");
		FSDataOutputStream os = fs.create(src);
		FileInputStream is = new FileInputStream("/Users/qiang/appInstall/download/haha.gz");
		IOUtils.copy(is, os);
	}
	
	/**
	 * 上传文件，
	 * @throws IOException
	 */
	@Test
	public void upload2() throws IOException {
		
		fs.copyFromLocalFile(new Path("/Users/qiang/appInstall/download/haha.gz"), new Path("hdfs://localhost:9000/bb/bb1/nini2.txt"));
	}
	
	/**
	 * 创建文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/bb/bb1"));
	}
	
	/**
	 * 删除文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void rm() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/aa"), true);
	}
	
	/**
	 * 查看文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		
		//listFiles 列出文件信息，附带递归遍历
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext() ) {
			LocatedFileStatus file = listFiles.next();
			String name = file.getPath().getName();
			System.out.println(name);
		}
		
		System.out.println("-------------");
		
		//listStatus 列出文件和文件夹信息，没递归遍历
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			String name = fileStatus.getPath().getName();
			System.out.println(name + (fileStatus.isDirectory() ? " : is dir " : " : is file"));
		}
	}
	
	
	
	
}
