package com.guanggong.www.crawl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CrawlDbFilter implements Mapper<WritableComparable<?>,Text,Text,CrawlDatum>{

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(WritableComparable<?> arg0, Text arg1,
			OutputCollector<Text, CrawlDatum> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
