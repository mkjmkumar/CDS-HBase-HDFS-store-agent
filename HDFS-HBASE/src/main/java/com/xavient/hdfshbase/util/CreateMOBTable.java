package com.xavient.hdfshbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;

/**
 * This method is used create HBase Table with MOB type enabled
 * 
 * @param Take
 *            input as Configuration object
 */
public class CreateMOBTable {
	public void createmobTable(Configuration config) throws HdfsHbaseException {
		// Configuration config = HBaseConfigUtil.getHBaseConfiguration();

		Connection conn = null;
		HBaseAdmin admin = null;

		try {
			//System.out.println("creating connection" + config.get("hbase.cluster.distributed"));
			conn = ConnectionFactory.createConnection(config);
			//System.out.println("created connection");
			admin = (HBaseAdmin) conn.getAdmin();
			//System.out.println("got admin");

			/*
			 * if
			 * (!admin.isTableEnabled(TableName.valueOf(Constants.tableName))) {
			 * System.out.println("inserting data"); HTableDescriptor hbaseTable
			 * = new HTableDescriptor(TableName.valueOf(Constants.tableName));
			 * HColumnDescriptor Row_Checksum_id = new
			 * HColumnDescriptor(Constants.Row_Checksum_id); HColumnDescriptor
			 * Mob_Image = new HColumnDescriptor(Constants.Mob_Image);
			 * HColumnDescriptor Content_Type = new
			 * HColumnDescriptor(Constants.Content_Type); HColumnDescriptor
			 * File_Properties = new
			 * HColumnDescriptor(Constants.File_Properties);
			 * 
			 * Mob_Image.setMobEnabled(true);
			 * Mob_Image.setMobThreshold(1000000000L);
			 * hbaseTable.addFamily(Row_Checksum_id);
			 * hbaseTable.addFamily(Mob_Image);
			 * hbaseTable.addFamily(Content_Type);
			 * hbaseTable.addFamily(File_Properties);
			 * admin.createTable(hbaseTable);
			 * System.out.println("Table created"); }
			 */
		} catch (IOException e) {
			System.out.println("Got exception: " + e.getMessage());
			throw new HdfsHbaseException(e.getMessage());
		} catch (Exception e) {

			throw new HdfsHbaseException(e.getMessage());
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
