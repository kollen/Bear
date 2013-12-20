package com.guanggong.www.crawl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import com.guanggong.www.util.NutchJob;


public class Injector extends Configured implements Tool{
	//继承Configured是为了得到配置信息对应的对象，实现Tool接口是为了分离run方法
	
	public static final Logger LOG = Logger.getLogger(Injector.class);
	
	public static class InjectMapper implements Mapper<WritableComparable<?>,Text,Text,CrawlDatum>{
		//规范化并过滤url
		private float scoreInjected;
		private JobConf jobConf;
		private long curTime;
		private int fetchInterval = 2592000;
		
		@Override
		public void configure(JobConf job) {
			this.jobConf = job;
			scoreInjected = 1.0f;
			curTime = System.currentTimeMillis();			
		}

		@Override
		public void close() {}

		public void map(WritableComparable<?> key, Text value,
				OutputCollector<Text, CrawlDatum> output, Reporter reporter)
				throws IOException {
			//
			String url = value.toString();//Hadoop会自动地将url文件按每一行解析为key为偏移量，value为每一行的字符
			if(url !=null && url.trim().startsWith("#"))
				//忽略以#开头的url（注释）
				return;
			try{
				//规范化ulr并过滤url
			}catch(Exception e){
				if(LOG.isInfoEnabled())
					LOG.warn("Skipping " + url + ":" + e);
				url = null;
			}
			if(url==null){
				reporter.getCounter("injector","urls_filtered").increment(1);
			}else
			{
				//url是合法的，开始注入到CrawlDatum数据结构中
				value.set(url);
				CrawlDatum datum = new CrawlDatum();
				datum.setStatus(CrawlDatum.STATUS_INJECTED);//把新注入的url标记为STATUS_INJECTED
				datum.setFetchTime(curTime);
				datum.setFetchInterval(fetchInterval);
				datum.setScore(scoreInjected);
				reporter.getCounter("injector", "urls_injected").increment(1);
				output.collect(value, datum);
			}			
		}		
	}
	
	public static class InjectReducer implements Reducer<Text,CrawlDatum,Text,CrawlDatum>{
		//当一个url存在多个对应的CrawlDatum时，就取最后的一个CrawlDatum状态，此函数用作合并url入口
		private int interval;
		private float scoreInjected;
		
		@Override
		public void configure(JobConf job) {
			interval = 2592000;
			scoreInjected = 1.0f;			
		}

		@Override
		public void close() throws IOException {}
		
		private CrawlDatum old = new CrawlDatum();
		private CrawlDatum injected = new CrawlDatum();

		@Override
		public void reduce(Text key, Iterator<CrawlDatum> values,
				OutputCollector<Text, CrawlDatum> output, Reporter reporter)
				throws IOException {
			//整个reduce函数都是针对同一个key在进行循环，reduce函数本身就运行在一个循环函数中
			boolean oldSet = false;
			boolean injectedSet = false;
			while(values.hasNext()){
				//对同一个key的value进行循环，当key所对应的values有两个以上时，injectedSet和oldSet会同时设置为true,此时用res来复制一个values,并只写入一个value
				CrawlDatum val = values.next();
				if(val.getStatus() == CrawlDatum.STATUS_INJECTED){
					injected.set(val);
					injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
					injectedSet = true;			
				}else{
					old.set(val);
					oldSet = true;
				}
			}
			CrawlDatum res = null;
			
			if(injectedSet && oldSet){
				//存在两个以上的相同的url,取最后的一个url的状态作为统一的入口,写入到合并文件中
				res = old;				
			}
			if(injectedSet && !oldSet){
				//只存在一个唯一的url，该url并不重复
				res = injected;
			}			
			output.collect(key,res);			
		}		
	}
	
	public Injector(){}
	
	public Injector(Configuration conf){
		setConf(conf);
	}
	
	public void Inject(Path crawlDb, Path urlDir) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		if(LOG.isInfoEnabled()){
			LOG.info("Injector: starting at" + sdf.format(start));
			LOG.info("Injector: crawlDb: " + crawlDb);
			LOG.info("Injector: urlDir: " + urlDir);
		}
		Path tempDir = new Path("inject-temp"+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		if(LOG.isInfoEnabled()){
			LOG.info("Injector: Coverting injected urls to crawldb entries");
		}
		
		JobConf sortJob = new NutchJob(getConf());
		sortJob.setJobName("inject " + urlDir);
		FileInputFormat.addInputPath(sortJob,urlDir);
		sortJob.setMapperClass(InjectMapper.class);
		 
		FileOutputFormat.setOutputPath(sortJob, tempDir);//把map结果输出到临时目录中
		sortJob.setOutputFormat(SequenceFileOutputFormat.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(CrawlDatum.class);
		sortJob.setLong("injector.current.time", System.currentTimeMillis());
		RunningJob mapJob = JobClient.runJob(sortJob);

		long urlsInjected = mapJob.getCounters().findCounter("injector", "urls_injected").getValue();
		long urlsFiltered = mapJob.getCounters().findCounter("injector", "urls_filtered").getValue();
		LOG.info("Injector: total number of urls rejected by filters: " + urlsFiltered);
		LOG.info("Injector: total number of urls injected after normalization and filtering: "
		        + urlsInjected);

	   
		LOG.info("Injector: Merging injected urls into crawldb");
		JobConf mergeJob = CrawlDb.createJob(getConf(), crawlDb);//已经合并了current目录
		FileInputFormat.addInputPath(mergeJob, tempDir);//合并临时目录
		mergeJob.setReducerClass(InjectReducer.class);//合并所有相同的url入口
		JobClient.runJob(mergeJob);
		CrawlDb.install(mergeJob,crawlDb);//合并了所有相同的url入口后，重命名该文件为current
		
		//清除临时文件
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(tempDir,true);
		
		long end = System.currentTimeMillis();
		LOG.info("Injector: finished at " + sdf.format(end));
		
		
		
	}
		
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Injector(),args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		if(args.length < 2){
			System.err.println("Usage: Injector<crawldb> <url_dir>");
			return -1;
		}
		try{
			Inject(new Path(args[0]),new Path(args[1]));
			return 0;
		}catch(Exception e){
			LOG.error("Injector: "+StringUtils.stringifyException(e));
			return -1;
		}
	}

}
