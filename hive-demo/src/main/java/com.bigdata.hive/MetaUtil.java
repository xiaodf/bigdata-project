package com.bigdata.hive;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetaUtil {
	private static final Logger logger = LoggerFactory.getLogger(MetaUtil.class);
	private static final Map<String, Table> tableCache = Maps.newHashMapWithExpectedSize(100);
	private static final ConcurrentMap<String, Boolean> partitionCache = new ConcurrentHashMap<String, Boolean>();
	private static HiveMetaStoreClient client;
	private static Table NOT_EXISTS = new Table();

	public static Table getTable(String metastoreUris, String db, String tb) {
		return getTable(null, null, metastoreUris, db, tb);
	}

	public static Table getTable(String principal, String keytab, String metastoreUris, String db, String tb) {
		Table table = tableCache.get(db + "." + tb);
		synchronized (tableCache) {
			if (table == null) {
				try {
					if (!ensureMetaStoreClient(principal, keytab, metastoreUris)) {
						System.out.println("------------------HiveMetaStoreClient 为 null");
						return null;
					}
					System.out.println("get table from hive metastore " + db + "." + tb);
					logger.info("get table from hive metastore " + db + "." + tb);
					table = client.getTable(db, tb);
					logger.info("cache table " + db + "." + tb
						+ " null:" + (table == null));
					tableCache.put(db + "." + tb, table);
				} catch (NoSuchObjectException e) {
					System.out.println(e.getMessage());
					logger.error(e.getMessage());
					table = NOT_EXISTS;
				} catch (Exception e) {
					System.out.println(e.getMessage());
					logger.error(e.getMessage());
					table = null;
				}
			}
			if (table == NOT_EXISTS) {
				table = null;
			}
			return table;
		}
	}

	/**
	 * @param principal
	 * @param keytab
	 * @param db
	 * @param tb
	 * @param partName
	 */
	public static void cachePartition(String principal, String keytab, String metastoreUris, String db, String tb, String partName) throws Exception {
		String partKey = db + "." + tb + "/" + partName;
		// fast checck without lock
		if (partitionCache.get(partKey) == null) {
			if (!ensureMetaStoreClient(principal, keytab, metastoreUris)) {
				System.out.println("------------------HiveMetaStoreClient 为 null");
				return;
			}
			synchronized (partitionCache) {
				try {
					logger.info("appending partition: " + partName + " to " + db + "." + tb);
					client.appendPartition(db, tb, partName);
				} catch (AlreadyExistsException e) {
					logger.warn("append partition cause errors: ", e);
					// no need to throw exception, cause maybe partition already exists.
				} catch (MetaException e) {
					logger.error("append partition cause errors: ", e);
					throw new Exception(e.getMessage());
				} catch (InvalidObjectException e) {
					logger.error("append partition cause errors: ", e);
					throw new Exception(e.getMessage());
				} catch (TException e) {
					logger.error("append partition cause errors: ", e);
					throw new Exception(e.getMessage());
				}
				// to make sure this partition is created in hive successfully,
				// we cache after append partition into hive metastore
				partitionCache.put(partKey, true);
			}
		}
	}

	/**
	 * 检查 HiveMetaStoreClient 是否可用，如果为NULL或者查询失败，创建新的连接
	 *
	 * @return
	 */
	private static synchronized boolean ensureMetaStoreClient(String principal, String keytab, String metastoreUris) throws IOException {

		System.out.println("--------------------------1111111111111");
		if (client == null) {
			// init jaas client
			Configuration conf = new Configuration();
			conf.set("hadoop.security.authentication", "Kerberos");
			conf.set("hive.metastore.uris", metastoreUris);
			System.out.println("metastoreUris========"+metastoreUris);
			
			HiveConf hiveConf = HCatUtil.getHiveConf(conf);
			if (principal != null && principal.length() > 0 && keytab != null && keytab.length() > 0) {
				hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUris);
//				hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
//				hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytab);
//				hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");

				UserGroupInformation.setConfiguration(conf);
				logger.info("try to login.");
				UserGroupInformation.loginUserFromKeytab(principal, keytab);
				System.out.println("current user:" + UserGroupInformation.getCurrentUser());
				System.out.println("login user:" + UserGroupInformation.getLoginUser());
				logger.info("current user:" + UserGroupInformation.getCurrentUser());
				logger.info("login user:" + UserGroupInformation.getLoginUser());
				logger.info("hive.metastore.uris:" + metastoreUris);
			}
			try {
				client = HCatUtil.getHiveClient(hiveConf);
			} catch (Exception e) {
				System.out.println("connect hive metastore client cause errors: "+ e);
				logger.error("connect hive metastore client cause errors: ", e);
				return false;
			}
		}
		boolean isOK = false;
		try {
			client.getAllDatabases();
			isOK = true;
		} catch (MetaException e) {
			try {
				client.reconnect();
				client.getAllDatabases();
				isOK = true;
			} catch (MetaException e1) {
				logger.error("reconnect hive metastore client cause errors: ",
					e);
			}
		}
		return isOK;
	}

	
}
