package com.guanggong.www.crawl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestMapper implements Mapper<WritableComparable<?>,CrawlDatum,Text,CrawlDatum>{
//什么也没做，原样输出，这是Injector第二步合并文件时要调用的，没有这个无法正常运行。
	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(WritableComparable<?> arg0, CrawlDatum arg1,
			OutputCollector<Text, CrawlDatum> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		output.collect((Text) arg0, arg1);
	}
	
	
}