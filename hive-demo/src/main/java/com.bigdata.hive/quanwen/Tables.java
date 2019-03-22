package com.bigdata.hive.quanwen;
public class Tables {
	private String tablename;
	private long count;
	private long quota;
	
	public Tables(){
		
	}
	
	public String getTablename() {
		return tablename;
	}
	
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
	
	public long getQuota() {
		return quota;
	}
	
	public void setQuota(long quota) {
		this.quota = quota;
	}
}
