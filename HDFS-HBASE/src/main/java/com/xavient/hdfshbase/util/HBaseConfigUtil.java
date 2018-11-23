package com.xavient.hdfshbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.xavient.hdfshbase.constants.Constants;

public class HBaseConfigUtil {
	public static Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		//
		conf.set("fs.defaultFS", "hdfs://10.2.0.61:8020");
		conf.set("hbase.zookeeper.quorum", "10.2.0.61");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		conf.set("hbase.cluster.distributed", "true");
		conf.addResource(new Path(Constants.Core_Site));
		conf.addResource(new Path(Constants.HDFS_Site));
		return conf;
	}
}