package com.guanggong.www.crawl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CrawlDbReducer implements Reducer<Text,CrawlDatum,Text,CrawlDatum>{

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(Text key, Iterator<CrawlDatum> value,
			OutputCollector<Text, CrawlDatum> out, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
