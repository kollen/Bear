package com.guanggong.www.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class NutchJob extends JobConf {
	
	public NutchJob(Configuration conf){
		super(conf,NutchJob.class);
	}
}
