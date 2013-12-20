package com.guanggong.www.crawl;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class Configured implements Configurable{

	private Configuration conf;
	
	public Configured(){
		this(null);
	}
	
	public Configured(Configuration conf){
		setConf(conf);
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

}
