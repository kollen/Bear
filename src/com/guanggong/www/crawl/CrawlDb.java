package com.guanggong.www.crawl;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.guanggong.www.util.NutchJob;


public class CrawlDb extends Configured implements Tool{

	public static final Logger LOG = Logger.getLogger(CrawlDb.class);
	
	public static final String CRAWLDB_PURGE_404 = "db.update.purge.404";

	public static final String CURRENT_NAME = "current";
	  
	public CrawlDb() {}
	  
	public CrawlDb(Configuration conf) {
	    setConf(conf);
	}
	  
	public static JobConf createJob(Configuration config, Path crawlDb)throws IOException {
		Path newCrawlDb =new Path(crawlDb, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		JobConf job = new NutchJob(config);
		job.setJobName("crawldb " + crawlDb);

		Path current = new Path(crawlDb, CURRENT_NAME);//在crawlDb目录下创建一个名为current的文件
		if (FileSystem.get(job).exists(current)) {//如果存在旧的crawlDb目录，就将其加入InputPath路径中，和上面的tempDir一起进行合并
			FileInputFormat.addInputPath(job, current);
		}
			    
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(CrawlDbFilter.class);
		job.setReducerClass(CrawlDbReducer.class);
			    
		FileOutputFormat.setOutputPath(job, newCrawlDb);
		job.setOutputFormat(MapFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CrawlDatum.class);
			    
		return job;			    
	  }	   
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void install(JobConf job, Path crawlDb) throws IOException {
		//重命名合并了所有具有相同入口的url后的文件为current，如果之前存在old和current文件，就分别删除之前存在的old文件，再把之前存在的current文件命名为old
		Path newCrawlDb = FileOutputFormat.getOutputPath(job);
		FileSystem fs = new JobClient(job).getFs();
		Path old = new Path(crawlDb,"old");
		Path current = new Path(crawlDb,CURRENT_NAME);
		if(fs.exists(current)){
			if(fs.exists(old))
				fs.delete(old,true);
			fs.rename(current,old);
		}
		fs.mkdirs(crawlDb);
		fs.rename(newCrawlDb,current);		
	}

}
