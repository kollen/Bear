package com.guanggong.www.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.WritableComparable;

public class CrawlDatum implements WritableComparable<CrawlDatum>,Cloneable{
	
	  /** Page was not fetched yet. */
	  public static final byte STATUS_DB_UNFETCHED      = 0x01;
	  /** Page was successfully fetched. */
	  public static final byte STATUS_DB_FETCHED        = 0x02;
	  /** Page no longer exists. */
	  public static final byte STATUS_DB_GONE           = 0x03;
	  /** Page temporarily redirects to other page. */
	  public static final byte STATUS_DB_REDIR_TEMP     = 0x04;
	  /** Page permanently redirects to other page. */
	  public static final byte STATUS_DB_REDIR_PERM     = 0x05;
	  /** Page was successfully fetched and found not modified. */
	  public static final byte STATUS_DB_NOTMODIFIED    = 0x06;
	  
	  /** Maximum value of DB-related status. */
	  public static final byte STATUS_DB_MAX            = 0x1f;
	  
	  /** Fetching was successful. */
	  public static final byte STATUS_FETCH_SUCCESS     = 0x21;
	  /** Fetching unsuccessful, needs to be retried (transient errors). */
	  public static final byte STATUS_FETCH_RETRY       = 0x22;
	  /** Fetching temporarily redirected to other page. */
	  public static final byte STATUS_FETCH_REDIR_TEMP  = 0x23;
	  /** Fetching permanently redirected to other page. */
	  public static final byte STATUS_FETCH_REDIR_PERM  = 0x24;
	  /** Fetching unsuccessful - page is gone. */
	  public static final byte STATUS_FETCH_GONE        = 0x25;
	  /** Fetching successful - page is not modified. */
	  public static final byte STATUS_FETCH_NOTMODIFIED = 0x26;
	  
	  /** Maximum value of fetch-related status. */
	  public static final byte STATUS_FETCH_MAX         = 0x3f;
	  
	  /** Page signature. */
	  public static final byte STATUS_SIGNATURE         = 0x41;
	  /** Page was newly injected. */
	  public static final byte STATUS_INJECTED          = 0x42;
	  /** Page discovered through a link. */
	  public static final byte STATUS_LINKED            = 0x43;
	  /** Page got metadata from a parser */
	  public static final byte STATUS_PARSE_META        = 0x44;
	
	private final static byte CUR_VERSION=1;//版本号
	private byte status;
	private long fetchTime = System.currentTimeMillis();
	private int fetchInterval;
	private float score = 0.0f;
	public static final HashMap<Byte,String> statNames = new HashMap<Byte,String>();
	
	static{
		statNames.put(STATUS_DB_UNFETCHED, "db_unfetched");
	    statNames.put(STATUS_DB_FETCHED, "db_fetched");
	    statNames.put(STATUS_DB_GONE, "db_gone");
	    statNames.put(STATUS_DB_REDIR_TEMP, "db_redir_temp");
	    statNames.put(STATUS_DB_REDIR_PERM, "db_redir_perm");
	    statNames.put(STATUS_DB_NOTMODIFIED, "db_notmodified");
	    statNames.put(STATUS_SIGNATURE, "signature");
	    statNames.put(STATUS_INJECTED, "injected");
	    statNames.put(STATUS_LINKED, "linked");
	    statNames.put(STATUS_FETCH_SUCCESS, "fetch_success");
	    statNames.put(STATUS_FETCH_RETRY, "fetch_retry");
	    statNames.put(STATUS_FETCH_REDIR_TEMP, "fetch_redir_temp");
	    statNames.put(STATUS_FETCH_REDIR_PERM, "fetch_redir_perm");
	    statNames.put(STATUS_FETCH_GONE, "fetch_gone");
	    statNames.put(STATUS_FETCH_NOTMODIFIED, "fetch_notmodified");
	    statNames.put(STATUS_PARSE_META, "parse_metadata");
	}
	
	public CrawlDatum(){}
	
	public CrawlDatum(int status,int fetchInterval){
		this();
		this.status=(byte) status;
		this.fetchInterval = fetchInterval;
	}
	
	public CrawlDatum(int status, int fetchInterval, float score){
		this(status,fetchInterval);
		this.score = score;
	}
	
	public int getFetchInterval() {
		return fetchInterval;
	}

	public void setFetchInterval(int fetchInterval) {
		this.fetchInterval = fetchInterval;
	}

	public byte getStatus() {
		return status;
	}

	public void setStatus(byte status) {
		this.status = status;
	}

	public long getFetchTime() {
		return fetchTime;
	}

	public void setFetchTime(long fetchTime) {
		this.fetchTime = fetchTime;
	}

	public float getScore() {
		return score;
	}

	public void setScore(float score) {
		this.score = score;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//将对象转换为字节流，并写入到输出流中
		out.writeByte(CUR_VERSION);
		out.writeByte(status);
		out.writeLong(fetchTime);
		out.writeInt(fetchInterval);
		out.writeFloat(score);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//从输入流in中读取字节流并反序列化为对象
		byte version = in.readByte(); //读取版本号	只是要干什么？？
		status = in.readByte();
		fetchTime = in.readLong();
		fetchInterval = in.readInt();
		score = in.readFloat();
	}

	@Override
	public int compareTo(CrawlDatum that) {
		//根据分数由高到低排序
		if(that.score != this.score)
			return (that.score - this.score)>0 ? 1 : -1;
		if(that.status != this.status)
			return this.status - that.status;
		if(that.fetchTime != this.fetchTime)
			return (that.fetchTime - this.fetchTime) >0 ? 1 : -1;
		if(that.fetchInterval != this.fetchInterval)
			return (that.fetchInterval - this.fetchInterval) >0 ? 1 : -1;					
		else
			return 0;
	}
	
	public String toString(){
		StringBuilder buf = new StringBuilder();
		buf.append("Version: "+CUR_VERSION+"\n");
		buf.append("Status: "+getStatus()+"("+getStatusName(getStatus())+")\n");
		buf.append("Fetch time: "+new Date(getFetchTime())+"\n");
		buf.append("Retry interval: "+getFetchInterval() + "seconds("+(getFetchInterval()/(3600*24))+" days)\n");
		buf.append("Score: "+getScore()+"\n");
		buf.append("\n");
		return buf.toString();
	}

	private String getStatusName(byte value) {
		String res = statNames.get(value);
		if(res == null){
			res = "unknown";
		}
		return res;
	}
	
	public void set(CrawlDatum that){
		//复制另一个url的抓取状态
		this.status = that.status;
		this.fetchTime = that.fetchTime;
		this.fetchInterval = that.fetchInterval;
		this.score = that.score;
	}
	
	
	

}
