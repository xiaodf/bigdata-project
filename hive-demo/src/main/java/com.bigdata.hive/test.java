package com.bigdata.hive;

import org.apache.hadoop.hive.metastore.api.Table;

public class test {

	public static void main(String[] args) {
		
		String principal= args[0];
		String keytab = args[1];
		String metastoreUris=args[2];
		String db = args[3];
		String tb = args[4];
		MetaUtil metaUtil = new MetaUtil();
		
		Table tale = metaUtil.getTable(principal, keytab, metastoreUris, db, tb);
	}
}
